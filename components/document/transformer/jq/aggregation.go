package jq

import (
	"fmt"

	"github.com/itchyny/gojq"
)

type Aggregation struct {
	Name   string            `yaml:"name" json:"name" jsonscheme:"description=Name of the aggregation"`
	Source string            `yaml:"source" json:"source" jsonscheme:"description=Selector of source documents to aggregate"`
	Target string            `yaml:"target" json:"target" jsonscheme:"description=Selector of target documents to aggregate to"`
	Action AggregationAction `yaml:"action" json:"action" jsonscheme:"description=Action to perform for the aggregation"`

	source *gojq.Query `yaml:"-" json:"-"`
	target *gojq.Query `yaml:"-" json:"-"`
}

func (a *Aggregation) Init() (err error) {
	a.source, err = gojq.Parse(a.Source)
	if err != nil {
		err = fmt.Errorf("failed to parse source query for aggregation '%s': %w", a.Name, err)
		return
	}

	a.target, err = gojq.Parse(a.Target)
	if err != nil {
		err = fmt.Errorf("failed to parse target query for aggregation '%s': %w", a.Name, err)
		return
	}

	if err = a.Action.Init(); err != nil {
		err = fmt.Errorf("failed to initialize action for aggregation '%s': %w", a.Name, err)
		return
	}
	return
}

// CheckTarget checks if a document is a target for this aggregation
func (a *Aggregation) CheckTarget(doc *DocExtended) (isTarget bool, err error) {
	if isTarget, err = runBoolQuery(a.target, doc.Map); err != nil {
		err = fmt.Errorf("rule '%s' target check failed: %w", a.Name, err)
	}
	return
}

// CheckSource checks if a document is a source for this aggregation
func (a *Aggregation) CheckSource(doc *DocExtended) (isSource bool, err error) {
	if isSource, err = runBoolQuery(a.source, doc.Map); err != nil {
		err = fmt.Errorf("rule '%s' source check failed: %w", a.Name, err)
	}
	return
}

type AggregationAction struct {
	Source *string `yaml:"source,omitempty" json:"source,omitempty" jsonscheme:"description=JQ query to extract content from source documents, if empty uses content"`
	Target *string `yaml:"target,omitempty" json:"target,omitempty" jsonscheme:"description=MetaData field to set aggregated data in target documents, if empty uses content"`

	Join *string `yaml:"join,omitempty" json:"join,omitempty" jsonscheme:"description=Field to join on, if empty no join is performed"`

	Forward bool `yaml:"forward,omitempty" json:"forward,omitempty" jsonscheme:"description=Direction of aggregation, if true aggregate following documents (forward), if false or not set aggregate preceding documents (backward)"`

	Hierarchy      *string `yaml:"hierarchy,omitempty" json:"hierarchy,omitempty" jsonscheme:"description=Field to use for hierarchy, if empty no hierarchy is applied"`
	HierarchyUntil *int    `yaml:"hierarchy_until,omitempty" json:"hierarchy_until,omitempty" jsonscheme:"description=Level until which hierarchy is applied, if empty applies to all levels"`
	HierarchySkip  []int   `yaml:"hierarchy_skip,omitempty" json:"hierarchy_skip,omitempty" jsonscheme:"description=Levels to skip in hierarchy, if empty no levels are skipped"`
	LevelPrefix    *string `yaml:"level_prefix,omitempty" json:"level_prefix,omitempty" jsonscheme:"description=Prefix to add to level names, if empty no prefix is added"`

	// Compiled queries - will be set by the transformer
	source *gojq.Query `yaml:"-" json:"-"`
}

func (a *AggregationAction) Init() (err error) {
	if a.Source != nil {
		if a.source, err = gojq.Parse(*a.Source); err != nil {
			err = fmt.Errorf("failed to parse source query for aggregation action: %w", err)
		}
	}
	return
}

func (a *AggregationAction) AppendFieldOrContent(contentsToAggregate []any, doc *DocExtended) (ret []interface{}, err error) {
	ret = contentsToAggregate
	if a.source != nil {
		// Use JQ query to extract content
		iter := a.source.Run(doc.Map)
		var v any
		var ok bool
		if v, ok = iter.Next(); ok {
			if e, isErr := v.(error); isErr {
				err = fmt.Errorf("error executing source query: %w", e)
			} else {
				ret = append(ret, v)
			}
		}
	} else {
		// Extract from content
		if content, ok := doc.Map[DOC_CONTENT]; ok {
			ret = append(ret, content)
		}
	}
	return
}

// processForwardAggregation applies forward aggregation rules to document maps
func (a *Aggregation) processForward(docs []*DocExtended) (err error) {
	// Process documents in order of appearance
	var currentTarget *DocExtended

	// First pass: collect sources for each target
	targetToSources := make(map[*DocExtended][]*DocExtended)

	// Iterate through documents in order to identify targets and their sources
	for i, doc := range docs {
		// Check if this document is a target
		var isTarget bool
		if isTarget, err = a.CheckTarget(doc); err != nil {
			return
		}

		// If it's a target, start collecting sources for it
		if isTarget {
			currentTarget = doc
			// Initialize an empty slice for this target's sources
			targetToSources[currentTarget] = []*DocExtended{}
		} else if currentTarget != nil {
			// If not a target, check if it's a source for the current target
			var isSource bool
			if isSource, err = a.CheckSource(doc); err != nil {
				return
			}

			if isSource {
				// Add this source to the current target's sources
				targetToSources[currentTarget] = append(targetToSources[currentTarget], doc)
			}
		}

		// If we've reached a new target or the end of the document list,
		// and we previously had a target, process that target with its sources
		if (isTarget || i == len(docs)-1) && len(targetToSources) > 0 {
			// Process all previous targets that have been identified
			for target, sources := range targetToSources {
				// Skip the current target as we're still collecting its sources
				if target == currentTarget && i < len(docs)-1 {
					continue
				}

				contentsToAggregate := []any{}

				// Add target document's content first
				if contentsToAggregate, err = a.Action.AppendFieldOrContent(contentsToAggregate, target); err != nil {
					return
				}

				// Add source documents' content
				for _, source := range sources {
					if contentsToAggregate, err = a.Action.AppendFieldOrContent(contentsToAggregate, source); err != nil {
						return
					}
				}

				// Apply aggregated content to the target document
				var contentToApply interface{}

				// Join content if needed
				if a.Action.Join != nil {
					contentToApply = JoinToStr(contentsToAggregate, *a.Action.Join)
				} else {
					contentToApply = contentsToAggregate
				}

				if a.Action.Target != nil {
					metaData := target.Map[DOC_META_DATA].(map[string]any)
					metaData[*a.Action.Target] = contentToApply
				} else {
					separator := "\t"
					if a.Action.Join != nil {
						separator = *a.Action.Join
					}
					target.Map[DOC_CONTENT] = JoinToStr(contentsToAggregate, separator)
				}

				// Remove this target from the map after processing
				if target != currentTarget || i == len(docs)-1 {
					delete(targetToSources, target)
				}
			}
		}
	}

	return
}

// Helper function to apply hierarchical aggregation directly on document maps
func (a *Aggregation) applyHierarchical(doc *DocExtended, buffer []*LeveledDocument) (err error) {
	if len(buffer) == 0 {
		return
	}

	// Get hierarchy field from map
	metaData, ok := doc.Map[DOC_META_DATA].(map[string]any)
	if !ok {
		err = fmt.Errorf("invalid metadata structure")
		return
	}

	hierarchy, ok := metaData[*a.Action.Hierarchy]
	if !ok {
		docID := doc.Map[DOC_ID].(string)
		err = fmt.Errorf("doc %s missing hierarchy field '%s'", docID, *a.Action.Hierarchy)
		return
	}

	var targetLevel int
	if targetLevel, ok = ToInt(hierarchy); !ok {
		docID := doc.Map[DOC_ID].(string)
		err = fmt.Errorf("hierarchy field for doc %s is not a valid integer", docID)
		return
	}

	// We need to collect the most recent document for each level without gaps
	// First sort the buffer from newest to oldest (assuming buffer is in order of appearance)
	// This allows us to only take the most recent document at each level

	// Find the most recent valid document at each level
	mostRecentByLevel := make(map[int]*LeveledDocument)
	for i := len(buffer) - 1; i >= 0; i-- {
		leveledDoc := buffer[i]
		// Only consider levels below the target
		if leveledDoc.Level < targetLevel {
			// Skip levels specified in HierarchySkip
			skipLevel := false
			if a.Action.HierarchySkip != nil {
				for _, skip := range a.Action.HierarchySkip {
					if leveledDoc.Level == skip {
						skipLevel = true
						break
					}
				}
			}

			// Skip levels below HierarchyUntil if specified
			if !skipLevel && (a.Action.HierarchyUntil == nil || leveledDoc.Level >= *a.Action.HierarchyUntil) {
				// If we haven't seen this level yet, record it
				if _, exists := mostRecentByLevel[leveledDoc.Level]; !exists {
					mostRecentByLevel[leveledDoc.Level] = leveledDoc
				}
			}
		}
	}

	// Now collect documents from consecutive levels without gaps
	contentsToAggregate := []interface{}{}
	joinSeparator := "\t"
	if a.Action.Join != nil {
		joinSeparator = *a.Action.Join
	}

	// Start from the level immediately below the target and work backwards
	// until we find a gap
	for level := targetLevel - 1; level >= 1; level-- {
		leveledDoc, exists := mostRecentByLevel[level]
		if !exists {
			// We found a gap, stop collecting
			break
		}

		// Extract content from this level's document map
		levelContent := ""
		var content any

		// Get source content from the map
		if a.Action.Source == nil {
			// If source not specified, use content
			content = leveledDoc.Doc.Map[DOC_CONTENT]
		} else {
			// If source specified, look in metadata
			levelMetaData := leveledDoc.Doc.Map[DOC_META_DATA].(map[string]any)
			content = levelMetaData[*a.Action.Source]
		}

		if content != nil {
			levelContent = ToStr(content)

			// Add level prefix if configured
			if a.Action.LevelPrefix != nil {
				levelContent = fmt.Sprintf("%s %d: %s", *a.Action.LevelPrefix, level, levelContent)
			}

			// Add to the beginning since we're going backwards
			contentsToAggregate = append([]interface{}{levelContent}, contentsToAggregate...)
		}
	}

	// Add the target document's content
	var targetContent any

	// Get target content from the map
	if a.Action.Source == nil {
		// If source not specified, use content
		targetContent = doc.Map[DOC_CONTENT]
	} else {
		// If source specified, look in metadata
		targetContent = metaData[*a.Action.Source]
	}

	if targetContent != nil {
		targetContentStr := ToStr(targetContent)

		// Add level prefix if configured
		if a.Action.LevelPrefix != nil {
			targetContentStr = fmt.Sprintf("%s %d: %s", *a.Action.LevelPrefix, targetLevel, targetContentStr)
		}

		contentsToAggregate = append(contentsToAggregate, targetContentStr)
	}

	// Apply aggregated content if there's anything to aggregate
	if len(contentsToAggregate) > 0 {
		// Apply directly to the map
		if a.Action.Target != nil {
			// Apply to metadata field
			if a.Action.Join == nil {
				metaData[*a.Action.Target] = contentsToAggregate
			} else {
				metaData[*a.Action.Target] = JoinToStr(contentsToAggregate, *a.Action.Join)
			}
		} else {
			// Apply to content
			doc.Map[DOC_CONTENT] = JoinToStr(contentsToAggregate, joinSeparator)
		}
	}

	return
}
