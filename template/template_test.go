package template_test

import (
	"testing"

	"github.com/gozssky/dbgen/template"
	"github.com/stretchr/testify/require"
)

func TestQName(t *testing.T) {
	var testCases = []struct {
		name          string
		parts         []string
		baseName      string
		qualifiedName string
		schemaName    string
		uniqueName    string
	}{
		{
			name:          "simple",
			parts:         []string{"db", "schema", "table"},
			baseName:      "table",
			qualifiedName: "db.schema.table",
			schemaName:    "db.schema",
			uniqueName:    "db.schema.table",
		},
		{
			name:          "empty_name",
			parts:         []string{"``"},
			baseName:      "``",
			qualifiedName: "``",
			schemaName:    "",
			uniqueName:    "",
		},
		{
			name:          "lower_case",
			parts:         []string{"db", "SCHEMA", "table"},
			baseName:      "table",
			qualifiedName: "db.SCHEMA.table",
			schemaName:    "db.SCHEMA",
			uniqueName:    "db.schema.table",
		},
		{
			name:          "simple_quoted",
			parts:         []string{`"db"`, `"schema"`, `"table"`},
			baseName:      `"table"`,
			qualifiedName: `"db"."schema"."table"`,
			schemaName:    `"db"."schema"`,
			uniqueName:    "db.schema.table",
		},
		{
			name:          "different_quotes",
			parts:         []string{`'db'`, `[schema]`, "`table`"},
			baseName:      "`table`",
			qualifiedName: "'db'.[schema].`table`",
			schemaName:    "'db'.[schema]",
			uniqueName:    "db.schema.table",
		},
		{
			name:          "escaped_double_quote",
			parts:         []string{`'d''b'`, `[schema]`, "`ta``ble`"},
			baseName:      "`ta``ble`",
			qualifiedName: "'d''b'.[schema].`ta``ble`",
			schemaName:    "'d''b'.[schema]",
			uniqueName:    "d'b.schema.ta`ble",
		},
		{
			name:          "special_characters",
			parts:         []string{"'d.b'", "`sch-ema`", "'tab/le'"},
			baseName:      "'tab/le'",
			qualifiedName: "'d.b'.`sch-ema`.'tab/le'",
			schemaName:    "'d.b'.`sch-ema`",
			uniqueName:    "d%2Eb.sch-ema.tab/le",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			qn := template.NewQName(tc.parts...)
			require.Equal(t, tc.baseName, qn.Name(false))
			require.Equal(t, tc.qualifiedName, qn.Name(true))
			require.Equal(t, tc.schemaName, qn.SchemaName())
			require.Equal(t, tc.uniqueName, qn.UniqueName())
		})
	}
}
