package jq

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cloudwego/eino/schema"
	"github.com/itchyny/gojq"
)

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
	documents = make([]*schema.Document, len(docs))
	for i, doc := range docs {
		documents[i] = doc.UpdateDoc()
	}
	return documents
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
