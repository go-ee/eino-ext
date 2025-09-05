package jq

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/cloudwego/eino/components/document"
	"github.com/cloudwego/eino/schema"
)

// Common constants for document fields
const (
	DOC_ID        = "id"
	DOC_CONTENT   = "content"
	DOC_META_DATA = "meta_data"
)

// --- Public Transformer Entrypoint ---
type Options struct {
	Transformers *Transformers
}

func WithRules(transformers *Transformers) document.TransformerOption {
	return document.WrapTransformerImplSpecificOptFn(func(o *Options) {
		o.Transformers = transformers
	})
}

type Transformer struct {
	Transformers *Transformers
}

// Transform is the main entry point called by the Eino framework.
func (t *Transformer) Transform(ctx context.Context, docs []*schema.Document, opts ...document.TransformerOption) (documents []*schema.Document, err error) {
	option := Options{}
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

		// Parse load file transform if present
		if cfg.LoadFile != nil {
			if err = cfg.LoadFile.Init(); err != nil {
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

// --- Configuration Structs with JSON annotations ---
type Config struct {
	Transform   *Transform       `yaml:"transform" json:"transform" jsonscheme:"description=Transform operation,optional"`
	Filter      *Filter          `yaml:"filter" json:"filter" jsonscheme:"description=Filter operation,optional"`
	Aggregation *Aggregation     `yaml:"aggregation" json:"aggregation" jsonscheme:"description=Aggregation rule,optional"`
	Custom      *CustomTransform `yaml:"custom" json:"custom" jsonscheme:"description=Custom transform rule,optional"`
	LoadFile    *LoadFile        `yaml:"loadFile" json:"loadFile" jsonscheme:"description=Load external file into document,optional"`
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

		// Apply load file transform if present
		if c.LoadFile != nil {
			if err = c.LoadFile.Apply(doc); err != nil {
				if c.LoadFile.IgnoreErrors {
					log.Println(err)
					err = nil
				} else {
					return
				}
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

	if c.LoadFile != nil {
		parts = append(parts, fmt.Sprintf("loadFile '%s'", c.LoadFile.Name))
	}

	if len(parts) == 0 {
		return "empty config"
	}

	return strings.Join(parts, ", ")
}
