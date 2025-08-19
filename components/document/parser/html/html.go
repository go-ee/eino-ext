/*
 * Copyright 2024 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package html

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/microcosm-cc/bluemonday"

	"github.com/cloudwego/eino/components/document/parser"
	"github.com/cloudwego/eino/schema"
)

const (
	MetaKeyTitle   = "_title"
	MetaKeyDesc    = "_description"
	MetaKeyLang    = "_language"
	MetaKeyCharset = "_charset"
	MetaKeySource  = "_source"
	MetaFileName   = "_file_name"

	// DefaultChunkSize is the default size for text chunks
	DefaultChunkSize = 7000 // Default text chunk size in characters
)

var _ parser.Parser = (*Parser)(nil)

type Config struct {
	// content selector of goquery. eg: body for <body>, #id for <div id="id">
	Selector    *string `yaml:"selector" json:"selector"`
	UseNameAsID bool    `yaml:"use_name_as_id" json:"use_name_as_id"`
	Handler     *string `yaml:"handler" json:"handler"`
	ChunkSize   int     `yaml:"chunk_size" json:"chunk_size"`
}

// implOptions is used to extract the config from the generic parser.Option
type implOptions struct {
	Config *Config
}

// WithConfig specifies the xlsx parser config
func WithConfig(config *Config) parser.Option {
	return parser.WrapImplSpecificOptFn(func(o *implOptions) {
		o.Config = config
	})
}

var (
	BodySelector = "body"
)

type Handler func(ctx context.Context, selection *goquery.Selection, config *Config, meta map[string]any) (docs []*schema.Document, err error)

// NewParser returns a new parser.
func NewParser(ctx context.Context, conf *Config) (parser *Parser, err error) {
	if conf == nil {
		conf = &Config{}
	}

	return &Parser{
		Config:   conf,
		Handlers: map[string]Handler{},
	}, nil
}

// Parser implements parser.Parser. It parses HTML content to text.
// use goquery to parse the HTML content, will read the <body> content as text (remove tags).
// will extract title/description/language/charset from the HTML content as meta data.
type Parser struct {
	Config   *Config
	Handlers map[string]Handler
}

func (p *Parser) Parse(ctx context.Context, reader io.Reader, opts ...parser.Option) (docs []*schema.Document, err error) {
	// Extract implementation-specific options
	config := parser.GetImplSpecificOptions(&implOptions{}, opts...).Config

	// Use config from options if provided, otherwise use default from parser instance
	if config == nil {
		config = p.Config
	}

	// Return error if no config is available
	if config == nil {
		return nil, fmt.Errorf("xlsx parser config not provided in options and no default config available")
	}

	if config.ChunkSize == 0 {
		config.ChunkSize = DefaultChunkSize
	}

	doc, err := goquery.NewDocumentFromReader(reader)
	if err != nil {
		return nil, err
	}

	option := parser.GetCommonOptions(&parser.Options{}, opts...)

	meta, err := p.getMetaData(ctx, doc)
	if err != nil {
		return nil, err
	}
	meta[MetaKeySource] = option.URI

	var handler Handler
	if config.Handler != nil {
		handler, _ = p.Handlers[*config.Handler]
	}
	if handler == nil {
		handler = Generic
	}

	var contentSel *goquery.Selection
	if config.Selector != nil {
		contentSel = doc.Find(*config.Selector).Contents()
	} else {
		contentSel = doc.Contents()
	}

	docs, err = handler(ctx, contentSel, config, meta)
	return
}

func (p *Parser) getMetaData(_ context.Context, doc *goquery.Document) (map[string]any, error) {
	meta := map[string]any{}

	title := doc.Find("title")
	if title != nil {
		if t := title.Text(); t != "" {
			meta[MetaKeyTitle] = t
		}
	}

	description := doc.Find("meta[name=description]")
	if description != nil {
		if desc := description.AttrOr("content", ""); desc != "" {
			meta[MetaKeyDesc] = desc
		}
	}

	html := doc.Find("html")
	if html != nil {
		if language := html.AttrOr("lang", ""); language != "" {
			meta[MetaKeyLang] = language
		}
	}

	charset := doc.Find("meta[charset]")
	if charset != nil {
		if c := charset.AttrOr("charset", ""); c != "" {
			meta[MetaKeyCharset] = c
		}
	}

	return meta, nil
}

func Generic(ctx context.Context, selection *goquery.Selection, config *Config, meta map[string]any) (docs []*schema.Document, err error) {
	sanitized := bluemonday.UGCPolicy().Sanitize(selection.Text())
	content := strings.TrimSpace(sanitized)

	id := ""
	if config.UseNameAsID {
		id = meta[MetaFileName].(string)
	}
	docs = []*schema.Document{
		{
			ID:       id,
			Content:  content,
			MetaData: meta,
		},
	}
	return
}
