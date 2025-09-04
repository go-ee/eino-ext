package jq

import (
	"fmt"

	"github.com/itchyny/gojq"
)

// Filter defines a document filtering operation
type Filter struct {
	Name  string `yaml:"name" json:"name" jsonscheme:"description=Name of the filter operation"`
	Query string `yaml:"query" json:"query" jsonscheme:"description=Filter query to apply"`

	query *gojq.Query `yaml:"-" json:"-"`
}

func (f *Filter) Init() (err error) {
	if f.query, err = gojq.Parse(f.Query); err != nil {
		err = fmt.Errorf("failed to parse filter query '%s': %w", f.Name, err)
		return
	}
	return
}

// Apply applies the filter to get filtered document IDs using only docMaps
func (f *Filter) Apply(docs []*DocExtended) (currentDocs []*DocExtended, err error) {
	// Apply filter query
	for _, doc := range docs {
		var keep bool
		if keep, err = runBoolQuery(f.query, doc.Map); err != nil {
			err = fmt.Errorf("document filtering '%s' failed: %w", f.Name, err)
			return
		} else if keep {
			currentDocs = append(currentDocs, doc)
		}
	}
	return
}
