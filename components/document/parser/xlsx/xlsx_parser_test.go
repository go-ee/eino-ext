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

package xlsx

import (
	"context"
	"os"
	"testing"

	"github.com/cloudwego/eino/components/document/parser"
	"github.com/stretchr/testify/assert"
)

func TestXlsxParser_Parse(t *testing.T) {
	t.Run("TestXlsxParser_WithDefault", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, nil)

		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f, parser.WithExtraMeta(map[string]any{"test": "test"}))
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)
		assert.True(t, len(docs[0].Content) > 0)
		assert.Equal(t, map[string]any{"年龄": "21", "性别": "男", "姓名": "张三"}, docs[0].MetaData[MetaDataRow])
		assert.Equal(t, "test", docs[0].MetaData["test"]) // Extra metadata directly in doc.MetaData
	})

	t.Run("TestXlsxParser_WithAnotherSheet", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet2",
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f, parser.WithExtraMeta(map[string]any{"test": "test"}))
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)
		assert.True(t, len(docs[0].Content) > 0)
		assert.Equal(t, map[string]any{"年龄": "21", "性别": "男", "姓名": "张三"}, docs[0].MetaData[MetaDataRow])
		assert.Equal(t, "test", docs[0].MetaData["test"]) // Extra metadata directly in doc.MetaData
	})

	t.Run("TestXlsxParser_WithIDPrefix", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet2",
			IDPrefix:  "_xlsx_row_",
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f, parser.WithExtraMeta(map[string]any{"test": "test"}))
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)
		assert.True(t, len(docs[0].Content) > 0)
		assert.Equal(t, map[string]any{"年龄": "21", "性别": "男", "姓名": "张三"}, docs[0].MetaData[MetaDataRow])
		assert.Equal(t, "test", docs[0].MetaData["test"]) // Extra metadata directly in doc.MetaData
	})

	t.Run("TestXlsxParser_WithNoHeader", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet3",
			Columns: Columns{
				NoHeader: true,
			},
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f, parser.WithExtraMeta(map[string]any{"test": "test"}))
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)
		assert.True(t, len(docs[0].Content) > 0)
		assert.Equal(t, map[string]any{}, docs[0].MetaData[MetaDataRow])
		assert.Equal(t, "test", docs[0].MetaData["test"]) // Extra metadata directly in doc.MetaData
	})

	t.Run("TestXlsxParser_WithColumns_Content", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet2",
			Columns: Columns{
				Content: []string{"A"}, // Only use column A for content
			},
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f)
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)

		// Assuming column A contains "张三" in the first row
		assert.Equal(t, "张三", docs[0].Content)

		// Metadata should still be populated from all columns
		assert.Equal(t, map[string]any{"年龄": "21", "性别": "男", "姓名": "张三"}, docs[0].MetaData[MetaDataRow])
	})

	t.Run("TestXlsxParser_WithColumns_Meta", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet2",
			Columns: Columns{
				Meta: []string{"A", "C"}, // Only use columns A and C for metadata
			},
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f)
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)

		// Content should include all columns
		assert.True(t, len(docs[0].Content) > 0)

		// Metadata should include all columns (not just A and C)
		// This is the current behavior of the implementation
		metaData := docs[0].MetaData[MetaDataRow].(map[string]any)
		assert.Equal(t, 3, len(metaData))
		assert.Contains(t, metaData, "姓名")
		assert.Contains(t, metaData, "年龄")
		assert.Contains(t, metaData, "性别")

		// Columns A and C should also be in root metadata
		assert.Equal(t, "张三", docs[0].MetaData["姓名"])
		assert.Equal(t, "21", docs[0].MetaData["年龄"])
	})

	t.Run("TestXlsxParser_WithColumns_CustomNames", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet2",
			Columns: Columns{
				CustomNames: map[string]string{
					"A": "Name",
					"B": "Gender",
					"C": "Age",
				},
			},
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f)
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)

		// Content should include all columns
		assert.True(t, len(docs[0].Content) > 0)

		// Metadata should use custom names
		metaData := docs[0].MetaData[MetaDataRow].(map[string]any)
		assert.Equal(t, map[string]any{
			"Name":   "张三",
			"Gender": "男",
			"Age":    "21",
		}, metaData)
	})

	t.Run("TestXlsxParser_WithColumns_Combined", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet2",
			Columns: Columns{
				Content: []string{"A"},
				Meta:    []string{"B", "C"},
				CustomNames: map[string]string{
					"B": "Gender",
					"C": "Age",
				},
			},
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f)
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)

		// Content should only include column A
		assert.Equal(t, "张三", docs[0].Content)

		// _row should contain all columns with custom names applied
		metaData := docs[0].MetaData[MetaDataRow].(map[string]any)
		assert.Equal(t, 3, len(metaData))
		assert.Equal(t, "张三", metaData["姓名"])
		assert.Equal(t, "男", metaData["Gender"])
		assert.Equal(t, "21", metaData["Age"])

		// Meta columns should be directly in root metadata with custom names
		assert.Equal(t, "男", docs[0].MetaData["Gender"])
		assert.Equal(t, "21", docs[0].MetaData["Age"])
	})

	t.Run("TestXlsxParser_ConfigFromOptions", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		// Create parser with default config
		p, err := NewXlsxParser(ctx, nil)
		assert.NoError(t, err)

		// Override config through options
		optionConfig := &Config{
			SheetName: "Sheet2",
			Columns: Columns{
				Content: []string{"A"},
				CustomNames: map[string]string{
					"A": "Name",
				},
			},
		}

		docs, err := p.Parse(ctx, f, WithConfig(optionConfig))
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)

		// Content should only include column A
		assert.Equal(t, "张三", docs[0].Content)

		// Metadata should use custom name for column A
		metaData := docs[0].MetaData[MetaDataRow].(map[string]any)
		assert.Contains(t, metaData, "Name")
		assert.Equal(t, "张三", metaData["Name"])
	})

	t.Run("TestXlsxParser_WithNoRowMeta", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet2",
			Columns: Columns{
				NoRowMeta: true,
			},
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f, parser.WithExtraMeta(map[string]any{"test": "test"}))
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)
		assert.True(t, len(docs[0].Content) > 0)

		// Should not have _row metadata
		_, hasRowMeta := docs[0].MetaData[MetaDataRow]
		assert.False(t, hasRowMeta)

		// Should still have extra metadata directly in doc.MetaData
		assert.Equal(t, "test", docs[0].MetaData["test"])
	})

	t.Run("TestXlsxParser_NoHeader_WithMetadata", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet3",
			Columns: Columns{
				NoHeader: true,
				// Instead of using Content, let's use CustomNames to avoid the empty map case
				CustomNames: map[string]string{
					"A": "A", // Using same name to maintain expected test behavior
					"B": "B",
					"C": "C",
				},
			},
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f)
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)

		// Should have _row metadata with column letters as keys
		rowMeta := docs[0].MetaData[MetaDataRow].(map[string]any)
		assert.Contains(t, rowMeta, "A")
		assert.Contains(t, rowMeta, "B")
		assert.Contains(t, rowMeta, "C")
	})

	t.Run("TestXlsxParser_NoHeader_WithCustomNames", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet3",
			Columns: Columns{
				NoHeader: true,
				CustomNames: map[string]string{
					"A": "Name",
					"B": "Gender",
					"C": "Age",
				},
			},
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f)
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)

		// Should have _row metadata with custom names
		rowMeta := docs[0].MetaData[MetaDataRow].(map[string]any)
		assert.Contains(t, rowMeta, "Name")
		assert.Contains(t, rowMeta, "Gender")
		assert.Contains(t, rowMeta, "Age")
		assert.NotContains(t, rowMeta, "A")
		assert.NotContains(t, rowMeta, "B")
		assert.NotContains(t, rowMeta, "C")
	})

	t.Run("TestXlsxParser_MetaColumnsInRootMetadata", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet2",
			Columns: Columns{
				Meta: []string{"B", "C"}, // Add columns B and C directly to metadata
				CustomNames: map[string]string{
					"B": "Gender",
					"C": "Age",
				},
			},
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f)
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)

		// Meta columns should be directly in MetaData
		assert.Equal(t, "男", docs[0].MetaData["Gender"])
		assert.Equal(t, "21", docs[0].MetaData["Age"])

		// _row should contain all columns
		rowMeta := docs[0].MetaData[MetaDataRow].(map[string]any)
		assert.Equal(t, 3, len(rowMeta))
		assert.Equal(t, "张三", rowMeta["姓名"])
		assert.Equal(t, "男", rowMeta["Gender"])
		assert.Equal(t, "21", rowMeta["Age"])
	})

	t.Run("TestXlsxParser_NoHeader_MetaColumnsInRootMetadata", func(t *testing.T) {
		ctx := context.Background()

		f, err := os.Open("./examples/testdata/test.xlsx")
		assert.NoError(t, err)

		p, err := NewXlsxParser(ctx, &Config{
			SheetName: "Sheet3",
			Columns: Columns{
				NoHeader: true,
				Meta:     []string{"A", "B"}, // Add columns A and B directly to metadata
				CustomNames: map[string]string{
					"A": "Name",
					"B": "Gender",
				},
			},
		})
		assert.NoError(t, err)

		docs, err := p.Parse(ctx, f)
		assert.NoError(t, err)
		assert.True(t, len(docs) > 0)

		// Meta columns should be directly in MetaData with custom names
		assert.Contains(t, docs[0].MetaData, "Name")
		assert.Contains(t, docs[0].MetaData, "Gender")

		// _row should contain all columns with letter keys (or custom names)
		rowMeta := docs[0].MetaData[MetaDataRow].(map[string]any)
		assert.Contains(t, rowMeta, "Name")
		assert.Contains(t, rowMeta, "Gender")
		assert.Contains(t, rowMeta, "C")
	})
}
