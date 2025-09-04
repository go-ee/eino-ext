package jq

import (
	"fmt"
	"reflect"

	"github.com/itchyny/gojq"
)

type CustomTransform struct {
	Name     string   `yaml:"name" json:"name" jsonscheme:"description=Name of the custom transform"`
	Selector string   `yaml:"selector" json:"selector" jsonscheme:"description=Selector to apply the transform"`
	Function string   `yaml:"function" json:"function" jsonscheme:"description=Function to call for the transform"`
	Target   string   `yaml:"target" json:"target" jsonscheme:"description=Field to store the transform result"`
	Args     []string `yaml:"args" json:"args" jsonscheme:"description=Arguments to pass to the transform function"`

	selector *gojq.Query   `yaml:"-" json:"-"`
	args     []*gojq.Query `yaml:"-" json:"-"`
}

func (c *CustomTransform) Init() (err error) {
	if c.selector, err = gojq.Parse(c.Selector); err != nil {
		err = fmt.Errorf("failed to parse selector for transform '%s': %w", c.Name, err)
		return
	}

	c.args = make([]*gojq.Query, len(c.Args))
	for j, argExpr := range c.Args {
		if c.args[j], err = gojq.Parse(argExpr); err != nil {
			err = fmt.Errorf("failed to parse arg %d for transform '%s': %w", j, c.Name, err)
			return
		}
	}
	return
}

// Apply applies the custom transformation to a document map
func (c *CustomTransform) Apply(doc *DocExtended, functionRegistry map[string]any) (err error) {
	// Skip if no transform
	if c == nil || c.selector == nil {
		return nil
	}

	// Check if document is a target for this transform
	var isTarget bool
	if isTarget, err = runBoolQuery(c.selector, doc.Map); err != nil {
		err = fmt.Errorf("rule '%s' selector check failed: %w", c.Name, err)
		return
	}

	if !isTarget {
		return nil // Not a target, skip
	}

	// Apply the function
	goFunc, ok := functionRegistry[c.Function]
	if !ok {
		err = fmt.Errorf("custom function '%s' not found", c.Function)
		return
	}

	// Extract arguments
	args := make([]reflect.Value, len(c.args))
	for i, q := range c.args {
		iter := q.Run(doc)
		var v interface{}
		var ok bool
		if v, ok = iter.Next(); !ok {
			err = fmt.Errorf("arg query %d produced no value for function '%s'", i, c.Function)
			return
		}
		args[i] = reflect.ValueOf(v)
	}

	// Call the function
	results := reflect.ValueOf(goFunc).Call(args)
	if len(results) > 1 && !results[1].IsNil() {
		if errResult, ok := results[1].Interface().(error); ok {
			err = fmt.Errorf("custom function '%s' returned an error: %w", c.Function, errResult)
			return
		}
	}

	// Apply the result to the metadata
	metaData, ok := doc.Map[DOC_META_DATA].(map[string]any)
	if !ok {
		err = fmt.Errorf("invalid metadata structure in document")
		return
	}
	metaData[c.Target] = results[0].Interface()

	return nil
}
