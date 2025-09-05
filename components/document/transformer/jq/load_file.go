package jq

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/itchyny/gojq"
)

type LoadFile struct {
	Name         string      `yaml:"name" json:"name" jsonscheme:"description=Name of the load file transform"`
	Selector     *string     `yaml:"selector" json:"selector" jsonscheme:"description=Selector to apply the transform"`
	BaseDir      string      `yaml:"baseDir" json:"baseDir" jsonscheme:"description=Base directory where files are located"`
	Filename     string      `yaml:"filename" json:"filename" jsonscheme:"description=JQ query to extract filename or static filename"`
	Target       string      `yaml:"target" json:"target" jsonscheme:"description=Field in metadata to store the file content"`
	IgnoreErrors bool        `yaml:"ignoreErrors" json:"ignoreErrors" jsonscheme:"description=Whether to ignore errors when loading files"`
	selector     *gojq.Query `yaml:"-" json:"-"`
	filename     *gojq.Query `yaml:"-" json:"-"`
}

func (lf *LoadFile) Init() (err error) {
	if lf.Selector != nil {
		// Parse the selector query
		if lf.selector, err = gojq.Parse(*lf.Selector); err != nil {
			err = fmt.Errorf("failed to parse selector for LoadFile '%s': %w", lf.Name, err)
			return
		}
	}

	// Parse the filename query
	if lf.filename, err = gojq.Parse(lf.Filename); err != nil {
		err = fmt.Errorf("failed to parse filename query for LoadFile '%s': %w", lf.Name, err)
		return
	}
	return
}

// Apply loads a file based on the configured rules and adds its content to the document metadata
func (lf *LoadFile) Apply(doc *DocExtended) (err error) {
	if lf.selector != nil {
		// Check if document is a target for this transform
		var isTarget bool
		if isTarget, err = runBoolQuery(lf.selector, doc.Map); err != nil {
			err = fmt.Errorf("LoadFile '%s' selector check failed: %w", lf.Name, err)
			return
		} else if !isTarget {
			return
		}
	}

	// Get the filename from the document using the jq query
	var filePath string
	if filePath, err = lf.findFilePath(doc); err != nil {
		return
	}

	// Read the file content
	var content []byte
	if content, err = os.ReadFile(filePath); err != nil {
		err = fmt.Errorf("failed to read file for LoadFile '%s': %w", lf.Name, err)
		return
	}

	// Add the content to the document metadata
	metaData, ok := doc.Map[DOC_META_DATA].(map[string]any)
	if !ok {
		err = fmt.Errorf("invalid metadata structure in document for LoadFile '%s'", lf.Name)
		return
	}
	metaData[lf.Target] = string(content)

	return
}

func (lf *LoadFile) findFilePath(doc *DocExtended) (ret string, err error) {
	var filenameValue interface{}
	iter := lf.filename.Run(doc.Map)
	var ok bool
	if filenameValue, ok = iter.Next(); !ok {
		err = fmt.Errorf("filename query produced no value for LoadFile '%s'", lf.Name)
		return
	}
	if e, isErr := filenameValue.(error); isErr {
		err = fmt.Errorf("filename query error for LoadFile '%s': %w", lf.Name, e)
		return
	}

	// Convert the filename to a string
	filename := ToStr(filenameValue)

	// Build the full path
	ret = filepath.Join(lf.BaseDir, filename)

	// Check if the file exists
	if _, statErr := os.Stat(ret); os.IsNotExist(statErr) {
		err = fmt.Errorf("file not found for LoadFile '%s': %s", lf.Name, ret)
		return
	}
	return
}
