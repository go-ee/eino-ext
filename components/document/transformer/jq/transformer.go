package jq

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/cloudwego/eino/components/document"
	"github.com/cloudwego/eino/schema"
	"github.com/itchyny/gojq"
)

// --- Public Transformer Entrypoint ---

type Transformer struct {
	Rules *TransformerRules
}

// implOptions is used to extract the rules from the generic TransformerOption.
type implOptions struct {
	Rules *TransformerRules
}

func WithRules(Rules *TransformerRules) document.TransformerOption {
	return document.WrapTransformerImplSpecificOptFn(func(o *implOptions) {
		o.Rules = Rules
	})
}

// Transform is the main entry point called by the Eino framework.
func (t *Transformer) Transform(ctx context.Context, docs []*schema.Document, opts ...document.TransformerOption) (documents []*schema.Document, err error) {
	option := implOptions{}
	document.GetTransformerImplSpecificOptions(&option, opts...)

	rules := option.Rules
	if rules == nil {
		rules = t.Rules
	}

	if rules != nil {
		documents, err = rules.Transform(ctx, docs, opts...)
	} else {
		log.Println("No jq transformer rules provided, skipping transformation.")
		documents = docs
	}
	return
}

// --- Configuration Structs with JSON annotations ---
type AggregationAction struct {
	SourceField   string  `yaml:"source_field" json:"source_field"`
	TargetField   string  `yaml:"target_field" json:"target_field"`
	Mode          string  `yaml:"mode" json:"mode"`
	JoinSeparator *string `yaml:"join_separator,omitempty" json:"join_separator,omitempty"`
	LevelField    string  `yaml:"level_key" json:"level_key"` // Renamed from LevelKey for consistency
}

type AggregationRule struct {
	Name           string            `yaml:"name" json:"name"`
	SourceSelector string            `yaml:"source_selector" json:"source_selector"`
	TargetSelector string            `yaml:"target_selector" json:"target_selector"`
	Action         AggregationAction `yaml:"action" json:"action"`

	sourceQuery *gojq.Query
	targetQuery *gojq.Query
}

type CustomTransform struct {
	Name        string   `yaml:"name" json:"name"`
	Selector    string   `yaml:"selector" json:"selector"`
	Function    string   `yaml:"function" json:"function"`
	TargetField string   `yaml:"target_key" json:"target_key"` // Renamed from TargetKey for consistency
	Args        []string `yaml:"args" json:"args"`

	selectorQuery *gojq.Query
	argQueries    []*gojq.Query
}

type Config struct {
	Transform   string `yaml:"transform" json:"transform"`
	Filter      string `yaml:"filter" json:"filter"`
	Aggregation struct {
		Rules []AggregationRule `yaml:"rules" json:"rules"`
	} `yaml:"aggregation" json:"aggregation"`
	CustomTransforms []CustomTransform `yaml:"custom_transforms" json:"custom_transforms"`
}

// --- Core Logic Implementation ---

// ConfigRules stores the compiled queries and rules for a single config
type ConfigRules struct {
	transformQuery   *gojq.Query
	filterQuery      *gojq.Query
	aggregationRules []AggregationRule
	customTransforms []CustomTransform
}

type TransformerRules struct {
	configRules      []*ConfigRules
	functionRegistry map[string]any
}

// LeveledDocument represents a document with an associated level
type LeveledDocument struct {
	Level int
	Doc   *schema.Document
}

func NewTransformerRules(cfgs []*Config, funcRegistry map[string]any) (transformer *TransformerRules, err error) {
	if len(cfgs) == 0 {
		err = fmt.Errorf("configurations cannot be nil or empty")
		return
	}

	transformer = &TransformerRules{
		functionRegistry: funcRegistry,
		configRules:      make([]*ConfigRules, 0, len(cfgs)),
	}

	for _, cfg := range cfgs {
		configRule := &ConfigRules{
			aggregationRules: cfg.Aggregation.Rules,
			customTransforms: cfg.CustomTransforms,
		}

		// Parse transform query if provided
		if cfg.Transform != "" {
			if configRule.transformQuery, err = gojq.Parse(cfg.Transform); err != nil {
				err = fmt.Errorf("failed to parse transform query: %w", err)
				return
			}
		}

		// Parse filter query if provided
		if cfg.Filter != "" {
			if configRule.filterQuery, err = gojq.Parse(cfg.Filter); err != nil {
				err = fmt.Errorf("failed to parse filter query: %w", err)
				return
			}
		}

		// Parse aggregation rules
		for i := range configRule.aggregationRules {
			rule := &configRule.aggregationRules[i]
			if rule.sourceQuery, err = gojq.Parse(rule.SourceSelector); err != nil {
				err = fmt.Errorf("failed to parse source selector for rule '%s': %w", rule.Name, err)
				return
			}
			if rule.targetQuery, err = gojq.Parse(rule.TargetSelector); err != nil {
				err = fmt.Errorf("failed to parse target selector for rule '%s': %w", rule.Name, err)
				return
			}
		}

		// Parse custom transforms
		for i := range configRule.customTransforms {
			rule := &configRule.customTransforms[i]
			if rule.selectorQuery, err = gojq.Parse(rule.Selector); err != nil {
				err = fmt.Errorf("failed to parse selector for transform '%s': %w", rule.Name, err)
				return
			}
			rule.argQueries = make([]*gojq.Query, len(rule.Args))
			for j, argExpr := range rule.Args {
				if rule.argQueries[j], err = gojq.Parse(argExpr); err != nil {
					err = fmt.Errorf("failed to parse arg %d for transform '%s': %w", j, rule.Name, err)
					return
				}
			}
		}

		transformer.configRules = append(transformer.configRules, configRule)
	}
	return
}

func (t *TransformerRules) Transform(ctx context.Context, docs []*schema.Document, opts ...document.TransformerOption) (documents []*schema.Document, err error) {
	documents = docs

	// Process documents through each config in sequence
	for _, configRule := range t.configRules {
		if documents, err = t.applyConfigRules(ctx, configRule, documents); err != nil {
			return
		}
	}

	return
}

func (t *TransformerRules) applyConfigRules(_ context.Context, rules *ConfigRules, docs []*schema.Document) (documents []*schema.Document, err error) {
	// Filter documents if filter query is defined
	filteredDocs := docs
	if rules.filterQuery != nil {
		filteredDocs = make([]*schema.Document, 0, len(docs))
		for _, doc := range docs {
			docAsMap := docToMap(doc)
			var keep bool
			if keep, err = runBoolQuery(rules.filterQuery, docAsMap); err != nil {
				err = fmt.Errorf("document filtering failed: %w", err)
				return
			} else if keep {
				filteredDocs = append(filteredDocs, doc)
			}
		}
	}

	// Prepare buffers for aggregation
	joinBuffers := make(map[string][]*schema.Document)
	hierarchicalBuffers := make(map[string][]*LeveledDocument)

	// Process each document
	for _, doc := range filteredDocs {
		docAsMap := docToMap(doc)

		// Apply individual transformation
		if rules.transformQuery != nil {
			var transformedMap map[string]any
			if transformedMap, err = runMapQuery(rules.transformQuery, docAsMap); err != nil {
				return
			} else {
				updateDocFromMap(doc, transformedMap)
				// Refresh map in case the transform changed it
				docAsMap = docToMap(doc)
			}
		}

		// Apply aggregation rules
		for i := range rules.aggregationRules {
			rule := &rules.aggregationRules[i]

			if rule.Action.Mode == "hierarchical_by_level" {
				if err = t.handleHierarchicalAggregation(doc, docAsMap, rule, hierarchicalBuffers); err != nil {
					return
				}
			} else {
				if err = t.handleJoinAggregation(doc, docAsMap, rule, joinBuffers); err != nil {
					return
				}
			}
		}

		// Apply custom transforms
		for i := range rules.customTransforms {
			rule := &rules.customTransforms[i]
			var isTarget bool
			if isTarget, err = runBoolQuery(rule.selectorQuery, docAsMap); err != nil {
				err = fmt.Errorf("rule '%s' selector check failed: %w", rule.Name, err)
				return
			} else if isTarget {
				if err = t.applyCustomFunction(doc, rule, docAsMap); err != nil {
					return
				}
			}
		}
	}

	documents = filteredDocs
	return
}

// Common helper functions for query execution
func runBoolQuery(query *gojq.Query, input map[string]any) (result bool, err error) {
	iter := query.Run(input)
	var v interface{}
	var ok bool
	if v, ok = iter.Next(); !ok {
		result = false
		return
	}
	if e, isErr := v.(error); isErr {
		err = e
		return
	}
	var boolResult bool
	if boolResult, ok = v.(bool); !ok {
		err = fmt.Errorf("query did not return a boolean, got %T", v)
		return
	}
	result = boolResult
	return
}

func runMapQuery(query *gojq.Query, input map[string]any) (result map[string]any, err error) {
	iter := query.Run(input)
	var v interface{}
	var ok bool
	if v, ok = iter.Next(); !ok {
		result = input
		return
	}
	if e, isErr := v.(error); isErr {
		err = fmt.Errorf("query error: %w", e)
		return
	}
	if result, ok = v.(map[string]any); !ok {
		err = fmt.Errorf("query did not return a map")
		return
	}
	return
}

// Update document from transformed map
func updateDocFromMap(doc *schema.Document, transformedMap map[string]any) {
	if newContent, exists := transformedMap["content"].(string); exists {
		doc.Content = newContent
	}
	if newMeta, exists := transformedMap["meta_data"].(map[string]any); exists {
		doc.MetaData = newMeta
	}
}

// Aggregation handling functions
func (t *TransformerRules) handleJoinAggregation(doc *schema.Document, docAsMap map[string]any, rule *AggregationRule, buffers map[string][]*schema.Document) (err error) {
	// Check if document is a target for aggregation
	var isTarget bool
	if isTarget, err = runBoolQuery(rule.targetQuery, docAsMap); err != nil {
		err = fmt.Errorf("rule '%s' target check failed: %w", rule.Name, err)
		return
	}

	// Handle target document
	if isTarget {
		if sourceContents := buffers[rule.Name]; len(sourceContents) > 0 {
			// Collect content from source documents
			contentsToAggregate := []interface{}{}
			for _, sourceDoc := range sourceContents {
				contentsToAggregate = appendFieldOrContent(contentsToAggregate, rule.Action.SourceField, sourceDoc)
			}
			contentsToAggregate = appendFieldOrContent(contentsToAggregate, rule.Action.SourceField, doc)

			// Apply aggregated content to target
			applyAggregatedContent(doc, rule.Action, contentsToAggregate)

			// Clear the buffer after using it
			buffers[rule.Name] = nil
		}
	}

	// Check if document is a source for aggregation
	var isSource bool
	if isSource, err = runBoolQuery(rule.sourceQuery, docAsMap); err != nil {
		err = fmt.Errorf("rule '%s' source check failed: %w", rule.Name, err)
		return
	}

	if isSource {
		buffers[rule.Name] = append(buffers[rule.Name], doc)
	}
	return
}

func (t *TransformerRules) handleHierarchicalAggregation(doc *schema.Document, docAsMap map[string]any, rule *AggregationRule, buffers map[string][]*LeveledDocument) (err error) {
	// Check if document is a target for aggregation
	var isTarget bool
	if isTarget, err = runBoolQuery(rule.targetQuery, docAsMap); err != nil {
		err = fmt.Errorf("rule '%s' target check failed: %w", rule.Name, err)
		return
	}

	// Handle target document
	if isTarget {
		buffer := buffers[rule.Name]
		if len(buffer) > 0 {
			var targetLevel int
			if targetLevel, err = getLevelValue(doc, rule.Action.LevelField); err != nil {
				return
			}

			// Collect content from documents with levels < targetLevel
			contentsToAggregate := []interface{}{}
			for _, leveledDoc := range buffer {
				if leveledDoc.Level < targetLevel {
					contentsToAggregate = appendFieldOrContent(contentsToAggregate, rule.Action.SourceField, leveledDoc.Doc)
				}
			}
			contentsToAggregate = appendFieldOrContent(contentsToAggregate, rule.Action.SourceField, doc)

			// Apply aggregated content if there's anything to aggregate
			if len(contentsToAggregate) > 0 {
				applyAggregatedContent(doc, rule.Action, contentsToAggregate)
			}
		}
	}

	// Check if document is a source for aggregation
	var isSource bool
	if isSource, err = runBoolQuery(rule.sourceQuery, docAsMap); err != nil {
		err = fmt.Errorf("rule '%s' source check failed: %w", rule.Name, err)
		return
	}

	if isSource {
		var sourceLevel int
		if sourceLevel, err = getLevelValue(doc, rule.Action.LevelField); err != nil {
			return
		}

		// Create a new LeveledDocument
		leveledDoc := &LeveledDocument{
			Level: sourceLevel,
			Doc:   doc,
		}

		// Find where to insert the new document to maintain order
		currentBuffer := buffers[rule.Name]
		for i, existing := range currentBuffer {
			if existing.Level >= sourceLevel {
				// Cut all existing documents after this point
				currentBuffer = currentBuffer[:i]
				break
			}
		}
		currentBuffer = append(currentBuffer, leveledDoc)

		// Update the buffer
		buffers[rule.Name] = currentBuffer
	}

	return
}

// Helper function to get level value from document
func getLevelValue(doc *schema.Document, levelField string) (level int, err error) {
	levelVal, ok := doc.MetaData[levelField]
	if !ok {
		err = fmt.Errorf("doc %s missing level_field '%s'", doc.ID, levelField)
		return
	}

	var isInt bool
	if level, isInt = toInt(levelVal); !isInt {
		err = fmt.Errorf("level_field for doc %s is not a valid integer", doc.ID)
		return
	}

	return
}

// Apply aggregated content to the target document
func applyAggregatedContent(doc *schema.Document, action AggregationAction, contents []interface{}) {
	if action.TargetField != "" {
		if action.JoinSeparator == nil {
			doc.MetaData[action.TargetField] = contents
		} else {
			doc.MetaData[action.TargetField] = join(contents, *action.JoinSeparator)
		}
	} else {
		separator := "\t"
		if action.JoinSeparator != nil {
			separator = *action.JoinSeparator
		}
		doc.Content = join(contents, separator)
	}
}

func appendFieldOrContent(contentsToAggregate []interface{}, sourceField string, doc *schema.Document) []interface{} {
	if sourceField != "" {
		// Extract from specific field
		if fieldValue, ok := doc.MetaData[sourceField]; ok {
			contentsToAggregate = append(contentsToAggregate, fieldValue)
		}
	} else {
		contentsToAggregate = append(contentsToAggregate, doc.Content)
	}
	return contentsToAggregate
}

func (t *TransformerRules) applyCustomFunction(doc *schema.Document, rule *CustomTransform, docAsMap map[string]any) (err error) {
	goFunc, ok := t.functionRegistry[rule.Function]
	if !ok {
		err = fmt.Errorf("custom function '%s' not found", rule.Function)
		return
	}
	var args []reflect.Value
	if args, err = t.extractArgs(rule.argQueries, docAsMap); err != nil {
		err = fmt.Errorf("failed to extract args for '%s': %w", rule.Function, err)
		return
	}
	results := reflect.ValueOf(goFunc).Call(args)
	if len(results) > 1 && !results[1].IsNil() {
		if errResult, ok := results[1].Interface().(error); ok {
			err = fmt.Errorf("custom function '%s' returned an error: %w", rule.Function, errResult)
			return
		}
	}
	doc.MetaData[rule.TargetField] = results[0].Interface()
	return
}

func (t *TransformerRules) extractArgs(argQueries []*gojq.Query, input map[string]any) (args []reflect.Value, err error) {
	args = make([]reflect.Value, len(argQueries))
	for i, q := range argQueries {
		iter := q.Run(input)
		var v interface{}
		var ok bool
		if v, ok = iter.Next(); !ok {
			err = fmt.Errorf("arg query %d produced no value", i)
			args = nil
			return
		}
		args[i] = reflect.ValueOf(v)
	}
	return
}

// docToMap is now used consistently to create a reliable input for gojq.
func docToMap(doc *schema.Document) (result map[string]any) {
	metaCopy := make(map[string]any, len(doc.MetaData))
	for k, v := range doc.MetaData {
		metaCopy[k] = v
	}
	result = map[string]any{"id": doc.ID, "content": doc.Content, "meta_data": metaCopy}
	return
}

func toInt(v any) (i int, ok bool) {
	switch val := v.(type) {
	case int:
		i, ok = val, true
	case int64:
		i, ok = int(val), true
	case float64:
		i, ok = int(val), true
	default:
		valueStr := fmt.Sprintf("%v", v)
		var err error
		if i, err = strconv.Atoi(valueStr); err == nil {
			ok = true
		}
	}
	return
}

func join(input []interface{}, separator string) (result string) {
	strs := make([]string, len(input))
	for i, v := range input {
		strs[i] = fmt.Sprintf("%v", v)
	}
	result = strings.Join(strs, separator)
	return
}
