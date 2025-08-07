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
	LevelKey      string  `yaml:"level_key" json:"level_key"`
}

type AggregationRule struct {
	Name           string            `yaml:"name" json:"name"`
	SourceSelector string            `yaml:"source_selector" json:"source_selector"`
	TargetSelector string            `yaml:"target_selector" json:"target_selector"`
	Action         AggregationAction `yaml:"action" json:"action"`
	sourceQuery    *gojq.Query
	targetQuery    *gojq.Query
}

type CustomTransform struct {
	Name      string   `yaml:"name" json:"name"`
	Selector  string   `yaml:"selector" json:"selector"`
	Function  string   `yaml:"function" json:"function"`
	TargetKey string   `yaml:"target_key" json:"target_key"`
	Args      []string `yaml:"args" json:"args"`

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

		// Simple parse, no special options needed.
		if cfg.Transform != "" {
			configRule.transformQuery, err = gojq.Parse(cfg.Transform)
			if err != nil {
				err = fmt.Errorf("failed to parse transform query: %w", err)
				return
			}
		}

		// Parse filter query if provided
		if cfg.Filter != "" {
			configRule.filterQuery, err = gojq.Parse(cfg.Filter)
			if err != nil {
				err = fmt.Errorf("failed to parse filter query: %w", err)
				return
			}
		}

		for i := range configRule.aggregationRules {
			rule := &configRule.aggregationRules[i]
			rule.sourceQuery, err = gojq.Parse(rule.SourceSelector)
			if err != nil { /* ... handle error ... */
			}
			rule.targetQuery, err = gojq.Parse(rule.TargetSelector)
			if err != nil { /* ... handle error ... */
			}
		}
		for i := range configRule.customTransforms {
			rule := &configRule.customTransforms[i]
			rule.selectorQuery, err = gojq.Parse(rule.Selector)
			if err != nil { /* ... handle error ... */
			}
			rule.argQueries = make([]*gojq.Query, len(rule.Args))
			for j, argExpr := range rule.Args {
				rule.argQueries[j], err = gojq.Parse(argExpr)
				if err != nil { /* ... handle error ... */
				}
			}
		}

		transformer.configRules = append(transformer.configRules, configRule)
	}
	return
}

func (t *TransformerRules) Transform(ctx context.Context, docs []*schema.Document, opts ...document.TransformerOption) (documents []*schema.Document, err error) {
	// Start with the original documents
	documents = docs

	// Process documents through each config in sequence
	for _, configRule := range t.configRules {
		// Apply the current config to the documents from the previous iteration
		documents, err = t.applyConfigRules(ctx, configRule, documents)
		if err != nil {
			return nil, err
		}
	}

	return documents, nil
}

func (t *TransformerRules) applyConfigRules(_ context.Context, rules *ConfigRules, docs []*schema.Document) (documents []*schema.Document, err error) {
	filteredDocs := make([]*schema.Document, 0, len(docs))

	// Filter documents if filter query is defined
	if rules.filterQuery != nil {
		for _, doc := range docs {
			docAsMap := docToMap(doc)
			shouldKeep, err := t.shouldKeepDocument(rules.filterQuery, docAsMap)
			if err != nil {
				return nil, fmt.Errorf("document filtering failed: %w", err)
			}
			if shouldKeep {
				filteredDocs = append(filteredDocs, doc)
			}
		}
	} else {
		filteredDocs = docs
	}

	// Continue with the filtered documents
	joinBuffers := make(map[string][]*schema.Document) // Changed to store Documents directly
	hierarchicalBuffers := make(map[string][]*LeveledDocument)

	for _, doc := range filteredDocs {
		// Consistently convert the doc to a map at the beginning of the loop.
		docAsMap := docToMap(doc)

		// Part 1: Individual Transformation
		if rules.transformQuery != nil {
			var transformedMap map[string]any
			transformedMap, err = t.applyIndividualTransform(rules.transformQuery, docAsMap)
			if err != nil {
				documents = nil
				return
			}
			// Update the document from the transformed map before proceeding.
			if newContent, exists := transformedMap["content"].(string); exists {
				doc.Content = newContent
			}
			if newMeta, exists := transformedMap["meta_data"].(map[string]any); exists {
				doc.MetaData = newMeta
			}
			// Refresh docAsMap in case the transform changed it.
			docAsMap = docToMap(doc)
		}

		// Part 2: Stateful Aggregation
		for i := range rules.aggregationRules {
			rule := &rules.aggregationRules[i]
			switch rule.Action.Mode {
			case "hierarchical_by_level":
				err = t.handleHierarchicalAggregation(doc, docAsMap, rule, hierarchicalBuffers)
			default:
				err = t.handleJoinAggregation(doc, docAsMap, rule, joinBuffers)
			}
			if err != nil {
				documents = nil
				return
			}
		}

		// Part 3: Custom Go Functions
		for i := range rules.customTransforms {
			rule := &rules.customTransforms[i]
			var isTarget bool
			isTarget, err = checkCondition(rule.selectorQuery, docAsMap)
			if err != nil {
				err = fmt.Errorf("rule '%s' selector check failed: %w", rule.Name, err)
				documents = nil
				return
			}
			if isTarget {
				if err = t.applyCustomFunction(doc, rule, docAsMap); err != nil {
					documents = nil
					return
				}
			}
		}
	}

	// Set the return value to the filtered documents
	documents = filteredDocs
	return
}

// shouldKeepDocument evaluates if a document should be kept after filtering
func (t *TransformerRules) shouldKeepDocument(filterQuery *gojq.Query, docAsMap map[string]any) (bool, error) {
	iter := filterQuery.Run(docAsMap)
	v, ok := iter.Next()
	if !ok {
		return false, nil // No result means don't keep
	}
	if e, isErr := v.(error); isErr {
		return false, fmt.Errorf("filter query error: %w", e)
	}
	result, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("filter query did not return a boolean, got %T", v)
	}
	return result, nil
}

// --- Handlers & Helpers ---

func (t *TransformerRules) applyIndividualTransform(transformQuery *gojq.Query, input map[string]any) (result map[string]any, err error) {
	iter := transformQuery.Run(input)
	v, ok := iter.Next()
	if !ok {
		result = input // No change, return the original map
		return
	}
	if e, isErr := v.(error); isErr {
		err = fmt.Errorf("query error: %w", e)
		return
	}
	result, ok = v.(map[string]any)
	if !ok {
		err = fmt.Errorf("query did not return a map")
	}
	return
}

func (t *TransformerRules) handleJoinAggregation(doc *schema.Document, docAsMap map[string]any, rule *AggregationRule, buffers map[string][]*schema.Document) (err error) {
	isTarget, err := checkCondition(rule.targetQuery, docAsMap)
	if err != nil {
		err = fmt.Errorf("rule '%s' target check failed: %w", rule.Name, err)
		return
	}
	if isTarget {
		if sourceContents := buffers[rule.Name]; len(sourceContents) > 0 {
			var contentsToAggregate []interface{}
			for _, sourceDoc := range sourceContents {
				contentsToAggregate = appendFieldOrContent(contentsToAggregate, rule.Action.SourceField, sourceDoc)
			}
			contentsToAggregate = appendFieldOrContent(contentsToAggregate, rule.Action.SourceField, doc)

			if rule.Action.TargetField != "" {
				if rule.Action.JoinSeparator == nil {
					doc.MetaData[rule.Action.TargetField] = contentsToAggregate
				} else {
					doc.MetaData[rule.Action.TargetField] = join(contentsToAggregate, *rule.Action.JoinSeparator)
				}
			} else {
				separator := "\t"
				if rule.Action.JoinSeparator != nil {
					separator = *rule.Action.JoinSeparator
				}

				// Append to content (original behavior)
				doc.Content = join(contentsToAggregate, separator)
			}
			// Clear the buffer after using it
			buffers[rule.Name] = nil
		}
	}

	isSource, err := checkCondition(rule.sourceQuery, docAsMap)
	if err != nil {
		err = fmt.Errorf("rule '%s' source check failed: %w", rule.Name, err)
		return
	}

	if isSource {
		buffers[rule.Name] = append(buffers[rule.Name], doc)
	}
	return
}

func (t *TransformerRules) handleHierarchicalAggregation(doc *schema.Document, docAsMap map[string]any, rule *AggregationRule, buffers map[string][]*LeveledDocument) (err error) {
	isTarget, err := checkCondition(rule.targetQuery, docAsMap)
	if err != nil {
		err = fmt.Errorf("rule '%s' target check failed: %w", rule.Name, err)
		return
	}

	if isTarget {
		buffer := buffers[rule.Name]
		if len(buffer) == 0 {
			return
		}

		targetLevelVal, ok := doc.MetaData[rule.Action.LevelKey]
		if !ok {
			err = fmt.Errorf("target doc %s missing level_key '%s'", doc.ID, rule.Action.LevelKey)
			return
		}

		var targetLevel int
		targetLevel, ok = toInt(targetLevelVal)
		if !ok {
			err = fmt.Errorf("level_key for doc %s is not a valid integer", doc.ID)
			return
		}

		// Collect content from documents with levels < targetLevel
		var contentsToAggregate []interface{}

		for _, leveledDoc := range buffer {
			if leveledDoc.Level < targetLevel {
				contentsToAggregate = appendFieldOrContent(contentsToAggregate, rule.Action.SourceField, leveledDoc.Doc)
			}
		}
		contentsToAggregate = appendFieldOrContent(contentsToAggregate, rule.Action.SourceField, doc)

		if len(contentsToAggregate) > 0 {
			if rule.Action.TargetField != "" {
				if rule.Action.JoinSeparator == nil {
					doc.MetaData[rule.Action.TargetField] = contentsToAggregate
				} else {
					doc.MetaData[rule.Action.TargetField] = join(contentsToAggregate, *rule.Action.JoinSeparator)
				}
			} else {
				separator := "\t"
				if rule.Action.JoinSeparator != nil {
					separator = *rule.Action.JoinSeparator
				}

				// Append to content (original behavior)
				doc.Content = join(contentsToAggregate, separator)
			}
		}
	}

	isSource, err := checkCondition(rule.sourceQuery, docAsMap)
	if err != nil {
		err = fmt.Errorf("rule '%s' source check failed: %w", rule.Name, err)
		return
	}

	if isSource {
		sourceLevelVal, ok := doc.MetaData[rule.Action.LevelKey]
		if !ok {
			err = fmt.Errorf("source doc %s missing level_key '%s'", doc.ID, rule.Action.LevelKey)
			return
		}

		var sourceLevel int
		sourceLevel, ok = toInt(sourceLevelVal)
		if !ok {
			err = fmt.Errorf("level_key for doc %s is not a valid integer", doc.ID)
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
				// Cut all existing documents after this point, because they belong to previous target level only
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
	args, err = t.extractArgs(rule.argQueries, docAsMap)
	if err != nil {
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
	doc.MetaData[rule.TargetKey] = results[0].Interface()
	return
}

func (t *TransformerRules) extractArgs(argQueries []*gojq.Query, input map[string]any) (args []reflect.Value, err error) {
	args = make([]reflect.Value, len(argQueries))
	for i, q := range argQueries {
		iter := q.Run(input)
		v, ok := iter.Next()
		if !ok {
			err = fmt.Errorf("arg query %d produced no value", i)
			args = nil
			return
		}
		args[i] = reflect.ValueOf(v)
	}
	return
}

func checkCondition(query *gojq.Query, input map[string]any) (isMatch bool, err error) {
	iter := query.Run(input)
	v, ok := iter.Next()
	if !ok {
		isMatch = false
		return
	}
	if e, isErr := v.(error); isErr {
		err = e
		return
	}
	result, ok := v.(bool)
	if !ok {
		err = fmt.Errorf("selector query did not return a boolean, got %T", v)
		return
	}
	isMatch = result
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

func join(input []interface{}, separator string) string {
	result := make([]string, len(input))
	for i, v := range input {
		result[i] = fmt.Sprintf("%v", v)
	}
	return strings.Join(result, separator)
}
