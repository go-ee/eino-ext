package jq

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/cloudwego/eino/components/document"
	"github.com/cloudwego/eino/schema"
	"github.com/itchyny/gojq"
)

// Common constants for document fields
const (
	DOC_ID        = "id"
	DOC_CONTENT   = "content"
	DOC_META_DATA = "meta_data"
)

// --- Public Transformer Entrypoint ---

type implOptions struct {
	Transformers *Transformers
}

func WithRules(transformers *Transformers) document.TransformerOption {
	return document.WrapTransformerImplSpecificOptFn(func(o *implOptions) {
		o.Transformers = transformers
	})
}

func NewDocExtended(doc *schema.Document) (ret *DocExtended) {
	ret = &DocExtended{
		Doc: doc,
	}
	ret.UpdateMap()
	return
}

type DocExtended struct {
	Doc *schema.Document
	Map map[string]any
}

func (d *DocExtended) ID() (id string) {
	var ok bool
	if id, ok = d.Map[DOC_ID].(string); !ok {
		id = d.Doc.ID
	}
	return
}

func (d *DocExtended) CheckChangedID() (err error) {
	possibleTransformedID := d.ID()
	if d.Doc.ID != possibleTransformedID {
		d.Doc.ID = possibleTransformedID
	}
	return
}

// Update document from transformed map
func (d *DocExtended) UpdateDoc() *schema.Document {
	if newID, exists := d.Map[DOC_ID].(string); exists {
		d.Doc.ID = newID
	}
	if newContent, exists := d.Map[DOC_CONTENT].(string); exists {
		d.Doc.Content = newContent
	}
	if newMeta, exists := d.Map[DOC_META_DATA].(map[string]any); exists {
		d.Doc.MetaData = newMeta
	}
	return d.Doc
}

func (d *DocExtended) UpdateMap() map[string]any {
	metaCopy := make(map[string]any, len(d.Doc.MetaData))
	for k, v := range d.Doc.MetaData {
		metaCopy[k] = v
	}
	d.Map = map[string]any{DOC_ID: d.Doc.ID, DOC_CONTENT: d.Doc.Content, DOC_META_DATA: metaCopy}
	return d.Map
}

// convertMapsToDocuments updates original documents with modified maps and returns them
func convertMapsToDocuments(docs []*DocExtended) (documents []*schema.Document) {
	documents = make([]*schema.Document, 0, len(docs))
	for i, doc := range docs {
		documents[i] = doc.UpdateDoc()
	}
	return documents
}

type Transformer struct {
	Transformers *Transformers
}

// Transform is the main entry point called by the Eino framework.
func (t *Transformer) Transform(ctx context.Context, docs []*schema.Document, opts ...document.TransformerOption) (documents []*schema.Document, err error) {
	option := implOptions{}
	document.GetTransformerImplSpecificOptions(&option, opts...)

	transformers := option.Transformers
	if transformers == nil {
		transformers = t.Transformers
	}

	if transformers != nil {
		docsExtended := make([]*DocExtended, len(docs))
		for i, doc := range docs {
			docsExtended[i] = NewDocExtended(doc)
		}
		docsExtended, err = transformers.Transform(ctx, docsExtended, opts...)
		documents = convertMapsToDocuments(docsExtended)
	} else {
		log.Println("No jq transformer rules provided, skipping transformation.")
		documents = docs
	}
	return
}

// --- Configuration Structs with JSON annotations ---
type Config struct {
	Transform   *Transform       `yaml:"transform" json:"transform" jsonscheme:"description=Transform operation,optional"`
	Filter      *Filter          `yaml:"filter" json:"filter" jsonscheme:"description=Filter operation,optional"`
	Aggregation *Aggregation     `yaml:"aggregation" json:"aggregation" jsonscheme:"description=Aggregation rule,optional"`
	Custom      *CustomTransform `yaml:"custom" json:"custom" jsonscheme:"description=Custom transform rule,optional"`
}

func (c *Config) Apply(docs []*DocExtended, functionRegistry map[string]any) (currentDocs []*DocExtended, err error) {
	currentDocs = docs

	if c.Filter != nil {
		if currentDocs, err = c.Filter.Apply(currentDocs); err != nil {
			return
		}
	}

	// Prepare buffers for aggregation - track sources for each rule
	joinBuffers := make(map[string][]*DocExtended) // Store doc IDs instead of docs
	hierarchicalBuffers := []*LeveledDocument{}

	// Second pass: Process each document
	for _, doc := range currentDocs {
		if c.Transform != nil {
			if err = c.Transform.Apply(doc); err != nil {
				return
			}
		}

		if c.Aggregation != nil {
			rule := c.Aggregation
			// Determine aggregation direction
			isForward := rule.Action.Forward

			// Check if document is a target for aggregation
			var isTarget bool
			if isTarget, err = rule.CheckTarget(doc); err != nil {
				return
			}

			if isTarget {
				// Apply appropriate aggregation based on direction and type
				if rule.Action.Hierarchy != nil {
					// Apply hierarchical aggregation directly on the map
					if err = rule.applyHierarchical(doc, hierarchicalBuffers); err != nil {
						return
					}
				} else if isForward {
					// Skip forward aggregation here as we'll handle it in the final pass
				} else {
					// Apply backward join aggregation (the original behavior)
					if sourceDocs := joinBuffers[rule.Name]; len(sourceDocs) > 0 {
						// Collect content from source documents (only previous docs)
						contentsToAggregate := []any{}

						for _, sourceDoc := range sourceDocs {
							// Use map-based function for content extraction
							if contentsToAggregate, err = rule.Action.AppendFieldOrContent(contentsToAggregate, sourceDoc); err != nil {
								return
							}
						}

						// Add current document's content if needed
						if contentsToAggregate, err = rule.Action.AppendFieldOrContent(contentsToAggregate, doc); err != nil {
							return
						}

						// Apply content to the map directly
						if rule.Action.Target != nil {
							if rule.Action.Join == nil {
								doc.Map[DOC_META_DATA].(map[string]any)[*rule.Action.Target] = contentsToAggregate
							} else {
								doc.Map[DOC_META_DATA].(map[string]any)[*rule.Action.Target] = JoinToStr(contentsToAggregate, *rule.Action.Join)
							}
						} else {
							separator := "\t"
							if rule.Action.Join != nil {
								separator = *rule.Action.Join
							}
							doc.Map[DOC_CONTENT] = JoinToStr(contentsToAggregate, separator)
						}
					}
				}
			}

			// After processing as a target, check if it's also a source for backward aggregation
			if !isForward {
				var isSource bool
				if isSource, err = rule.CheckSource(doc); err != nil {
					return
				}

				if isSource {
					// Add to appropriate buffer for future targets
					if rule.Action.Hierarchy != nil {
						// Get hierarchy value directly from the map
						var sourceLevel int
						metaData := doc.Map[DOC_META_DATA].(map[string]any)
						hierarchy, ok := metaData[*rule.Action.Hierarchy]
						if !ok {
							err = fmt.Errorf("doc %s missing hierarchy field '%s'", doc.ID(), *rule.Action.Hierarchy)
							return
						}
						if sourceLevel, ok = ToInt(hierarchy); !ok {
							err = fmt.Errorf("hierarchy field for doc %s is not a valid integer", doc.ID())
							return
						}

						leveledDoc := &LeveledDocument{
							Level: sourceLevel,
							Doc:   doc,
						}

						hierarchicalBuffers = append(hierarchicalBuffers, leveledDoc)
					} else {
						joinBuffers[rule.Name] = append(joinBuffers[rule.Name], doc)
					}
				}
			}
		}

		// Apply custom transform if present
		if c.Custom != nil {
			if err = c.Custom.Apply(doc, functionRegistry); err != nil {
				return
			}
		}
	}

	// Final pass: Process forward aggregation if present
	if c.Aggregation != nil && c.Aggregation.Action.Forward {
		if err = c.Aggregation.processForward(currentDocs); err != nil {
			return
		}
	}
	return
}

// --- Core Logic Implementation ---

type Transformers struct {
	configs          []*Config
	functionRegistry map[string]any
}

// LeveledDocument represents a document with an associated level
type LeveledDocument struct {
	Level int
	Doc   *DocExtended
}

func NewTransformers(cfgs []*Config, funcRegistry map[string]any) (transformers *Transformers, err error) {
	if len(cfgs) == 0 {
		err = fmt.Errorf("configurations cannot be nil or empty")
		return
	}

	transformers = &Transformers{
		functionRegistry: funcRegistry,
		configs:          make([]*Config, 0, len(cfgs)),
	}

	for _, cfg := range cfgs {
		// Parse transform query if provided
		if cfg.Transform != nil {
			if err = cfg.Transform.Init(); err != nil {
				return
			}
		}

		// Parse filter query if provided
		if cfg.Filter != nil {
			if err = cfg.Filter.Init(); err != nil {
				return
			}
		}

		// Parse single aggregation rule if present
		if cfg.Aggregation != nil {
			if err = cfg.Aggregation.Init(); err != nil {
				return
			}
		}

		// Parse single custom transform if present
		if cfg.Custom != nil {
			if err = cfg.Custom.Init(); err != nil {
				return
			}
		}
		transformers.configs = append(transformers.configs, cfg)
	}
	return
}

func (t *Transformers) Transform(ctx context.Context, docs []*DocExtended, opts ...document.TransformerOption) (currentDocs []*DocExtended, err error) {
	// Create document maps and indices once	// Process documents through each config in sequence
	currentDocs = docs
	for i, config := range t.configs {
		// Log configuration details for debugging
		configDetails := config.Description()
		log.Printf("Applying transformer rule #%d: %s", i+1, configDetails)

		if currentDocs, err = config.Apply(currentDocs, t.functionRegistry); err != nil {
			log.Printf("Error applying rule #%d: %v", i+1, err)
			return
		}
		log.Printf("Successfully applied rule #%d, documents count: %d", i+1, len(currentDocs))
	}
	return
}

// Common helper functions for query execution
func runBoolQuery(query *gojq.Query, docAsMap map[string]any) (result bool, err error) {
	iter := query.Run(docAsMap)
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

func MapQuery(query *gojq.Query, docAsMap map[string]any) (resultMap map[string]any, err error) {
	iter := query.Run(docAsMap)
	var v any
	var ok bool
	if v, ok = iter.Next(); !ok {
		resultMap = docAsMap
		return
	}
	if e, isErr := v.(error); isErr {
		err = fmt.Errorf("query error: %w", e)
		return
	}
	if resultMap, ok = v.(map[string]any); !ok {
		err = fmt.Errorf("query did not return a map")
		return
	}
	return
}

func ToInt(v any) (i int, ok bool) {
	switch val := v.(type) {
	case int:
		i, ok = val, true
	case int32:
		i, ok = int(val), true
	case int64:
		i, ok = int(val), true
	case float32:
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

func JoinToStr(input []interface{}, separator string) (result string) {
	strs := make([]string, len(input))
	for i, v := range input {
		strs[i] = ToStr(v)
	}
	result = strings.Join(strs, separator)
	return
}

func ToStr(input interface{}) (ret string) {
	switch val := input.(type) {
	case string:
		ret = val
	default:
		ret = fmt.Sprintf("%v", input)
	}
	return
}

// getConfigDescription returns a human-readable description of the configuration
func (c *Config) Description() string {
	var parts []string

	if c.Transform != nil {
		parts = append(parts, fmt.Sprintf("transform '%s'", c.Transform.Name))
	}

	if c.Filter != nil {
		parts = append(parts, fmt.Sprintf("filter '%s'", c.Filter.Name))
	}

	if c.Aggregation != nil {
		parts = append(parts, fmt.Sprintf("aggregation '%s'", c.Aggregation.Name))
	}

	if c.Custom != nil {
		parts = append(parts, fmt.Sprintf("custom transform '%s'", c.Custom.Name))
	}

	if len(parts) == 0 {
		return "empty config"
	}

	return strings.Join(parts, ", ")
}
