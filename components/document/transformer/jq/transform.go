package jq

import (
	"fmt"

	"github.com/itchyny/gojq"
)

// Transform defines a document transformation operation
type Transform struct {
	Name       string `yaml:"name" json:"name" jsonscheme:"description=Name of the transform operation"`
	Expression string `yaml:"expression" json:"expression" jsonscheme:"description=Transform expression to apply"`

	expression *gojq.Query `yaml:"-" json:"-"`
}

func (t *Transform) Init() (err error) {
	if t.expression, err = gojq.Parse(t.Expression); err != nil {
		err = fmt.Errorf("failed to parse transform query '%s': %w", t.Name, err)
		return
	}
	return
}

// Apply applies the transformation to a document map
func (t *Transform) Apply(doc *DocExtended) (err error) {
	// Apply the transformation
	if doc.Map, err = MapQuery(t.expression, doc.Map); err != nil {
		err = fmt.Errorf("transform '%s' failed: %w", t.Name, err)
		return
	}
	if err = doc.CheckChangedID(); err != nil {
		err = fmt.Errorf("transform '%s' resulted in invalid document ID: %w", t.Name, err)
		return
	}
	return
}
