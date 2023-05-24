package template_test

import (
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/gozssky/dbgen/constant"
	"github.com/gozssky/dbgen/template"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	var testCases = []struct {
		name  string
		input string
		tmpl  *template.Template
	}{
		{
			"simple",
			`
CREATE TABLE "database"."schema"."table" (
    "id"        INTEGER,
        /*{{ rownum }}*/
    "name"      CHAR(40),
        /*{{ rand.regex('[a-zA-Z ]{40}') }}*/
    UNIQUE KEY "some_index"("id")
);`,
			&template.Template{
				Tables: []*template.Table{
					{
						Name: template.NewQName(`"database"`, `"schema"`, `"table"`),
						Columns: []*template.Column{
							{Name: template.NewName(`"id"`), Expr: makeExpr(t, "rownum")},
							{Name: template.NewName(`"name"`), Expr: makeExpr(t, `rand.regex('[a-zA-Z ]{40}')`)},
						},
					},
				},
			},
		},
		{
			"global-exprs",
			`
{{ @dirs := array['North', 'West', 'East', 'South'] }}
CREATE TABLE cardinals (
    t INTEGER       {{ rownum }},
    d1 VARCHAR(5)   {{ @dirs[rand.zipf(4, 0.8)] }},
    d2 VARCHAR(5)   {{ @dirs[rand.zipf(4, 0.8)] }}
);`,
			&template.Template{
				GlobalExprs: []template.Expr{makeExpr(t, `@dirs := array['North', 'West', 'East', 'South']`)},
				Tables: []*template.Table{
					{
						Name: template.NewQName("cardinals"),
						Columns: []*template.Column{
							{Name: template.NewName("t"), Expr: makeExpr(t, "rownum")},
							{Name: template.NewName("d1"), Expr: makeExpr(t, `@dirs[rand.zipf(4, 0.8)]`)},
							{Name: template.NewName("d2"), Expr: makeExpr(t, `@dirs[rand.zipf(4, 0.8)]`)},
						},
					},
				},
			},
		},
		{
			"derived-tables",
			`
CREATE TABLE "parent" (
    "parent_id" UUID PRIMARY KEY,
        /*{{ @parent_id := rand.uuid() }}*/
    "child_count" INT UNSIGNED NOT NULL
        /*{{ @child_count := rand.range_inclusive(0, 4) }}*/
);

/*{{ for each row of "parent" generate @child_count rows of "child" }}*/
CREATE TABLE "child" (
    "child_id" UUID PRIMARY KEY,
        /*{{ rand.uuid() }}*/
    "parent_id" UUID NOT NULL REFERENCES "parent"("parent_id")
        /*{{ @parent_id }}*/
);`,
			&template.Template{
				Tables: []*template.Table{
					{
						Name: template.NewQName(`"parent"`),
						Columns: []*template.Column{
							{Name: template.NewName(`"parent_id"`), Expr: makeExpr(t, `@parent_id := rand.uuid()`)},
							{Name: template.NewName(`"child_count"`), Expr: makeExpr(t, `@child_count := rand.range_inclusive(0, 4)`)},
						},
						Derived: []lo.Tuple2[int, template.Expr]{
							lo.T2(1, makeExpr(t, `@child_count`)),
						},
					},
					{
						Name: template.NewQName(`"child"`),
						Columns: []*template.Column{
							{Name: template.NewName(`"child_id"`), Expr: makeExpr(t, `rand.uuid()`)},
							{Name: template.NewName(`"parent_id"`), Expr: makeExpr(t, `@parent_id`)},
						},
					},
				},
			},
		},
		{
			"multi-derived-tables",
			`
CREATE TABLE A ( … );
/*{{ for each row of A generate 2 rows of B }}*/
CREATE TABLE B ( … );
/*{{ for each row of B generate 1 row of C }}*/
CREATE TABLE C ( … );
/*{{ for each row of A generate 4 rows of D }}*/
CREATE TABLE D ( … );`,
			&template.Template{
				Tables: []*template.Table{
					{
						Name: template.NewQName("A"),
						Derived: []lo.Tuple2[int, template.Expr]{
							lo.T2(1, makeExpr(t, "2")),
							lo.T2(3, makeExpr(t, "4")),
						},
					},
					{
						Name: template.NewQName("B"),
						Derived: []lo.Tuple2[int, template.Expr]{
							lo.T2(2, makeExpr(t, "1")),
						},
					},
					{
						Name: template.NewQName("C"),
					},
					{
						Name: template.NewQName("D"),
					},
				},
			},
		},
		{
			"balanced-text",
			`CREATE TABLE t (()[]{}([]{()})) ()[]{}([]{()});`,
			&template.Template{
				Tables: []*template.Table{
					{Name: template.NewQName("t")},
				},
			},
		},
		{
			"comment",
			`CREATE /* comment */ TABLE t (a INT);`,
			&template.Template{
				Tables: []*template.Table{
					{Name: template.NewQName("t")},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := template.Parse(tc.input)
			require.NoError(t, err)
			for _, table := range tmpl.Tables {
				table.Content = ""
			}
			require.Equal(t, tc.tmpl, tmpl)
		})
	}
}

func makeExpr(t *testing.T, input string) template.Expr {
	expr, err := template.ParseExpr(input)
	require.NoError(t, err)
	return expr
}

func TestTableContent(t *testing.T) {
	datadriven.RunTest(t, "testdata/table_content", func(t *testing.T, d *datadriven.TestData) string {
		tmpl, err := template.Parse(d.Input)
		require.NoError(t, err)
		var contents []string
		for _, table := range tmpl.Tables {
			contents = append(contents, table.Content)
		}
		return strings.Join(contents, "\n")
	})
}

func TestParseExpr(t *testing.T) {
	var testCases = []struct {
		input   string
		expr    template.Expr
		exprStr string
		// It's guaranteed exprStr is semantically equivalent to input.
		// But it's not guaranteed that exprStr is the same as input.
		isExprStrSameAsInput bool
	}{
		{
			"rownum",
			&template.RowNum{},
			"rownum",
			true,
		},
		{
			"subrownum",
			&template.SubRowNum{},
			"subrownum",
			true,
		},
		{
			"null",
			&template.Constant{Value: constant.Null},
			"NULL",
			true,
		},
		{
			"true",
			&template.Constant{Value: constant.MakeBool(true)},
			"TRUE",
			true,
		},
		{
			"false",
			&template.Constant{Value: constant.MakeBool(false)},
			"FALSE",
			true,
		},
		{
			"current_timestamp",
			&template.CurrentTimestamp{},
			"current_timestamp",
			true,
		},
		{
			"(rownum)",
			&template.ParenExpr{Expr: &template.RowNum{}},
			"(rownum)",
			true,
		},
		{
			"'abc'",
			&template.Constant{Value: constant.MakeBytes([]byte("abc"))},
			"'abc'",
			true,
		},
		{
			"'\x58\xb3\x8e\x4e'",
			&template.Constant{Value: constant.MakeBytes([]byte("\x58\xb3\x8e\x4e"))},
			"X'58b38e4e'",
			false,
		},
		{
			"123",
			&template.Constant{Value: constant.MakeInt64(123)},
			"123",
			true,
		},
		{
			"@x",
			&template.GetVariable{Name: "x"},
			"@`x`",
			true,
		},
		{
			"@x:=1",
			&template.SetVariable{Name: "x", Value: &template.Constant{Value: constant.MakeInt64(1)}},
			"@`x` := 1",
			true,
		},
		{
			"X'C2BF 3F'",
			&template.FuncExpr{
				Name: template.NewQName("hex", "decode"),
				Args: []template.Expr{&template.Constant{Value: constant.MakeBytes([]byte("C2BF 3F"))}},
			},
			"hex.decode('C2BF 3F')",
			false,
		},
		{
			"case @x when 1 then '1' when 2 then '2' else '3' end",
			&template.CaseValueWhen{
				Value: &template.GetVariable{Name: "x"},
				Whens: []*template.When{
					{
						Cond: &template.Constant{Value: constant.MakeInt64(1)},
						Then: &template.Constant{Value: constant.MakeBytes([]byte("1"))},
					},
					{
						Cond: &template.Constant{Value: constant.MakeInt64(2)},
						Then: &template.Constant{Value: constant.MakeBytes([]byte("2"))},
					},
				},
				Else: &template.Constant{Value: constant.MakeBytes([]byte("3"))},
			},
			"CASE @`x` WHEN 1 THEN '1' WHEN 2 THEN '2' ELSE '3' END",
			true,
		},
		{
			"case when @x = 1 then @y:=1;'1' when @x = 2 then @y:=2;'2' else @y:=3;'3' end",
			&template.CaseValueWhen{
				Whens: []*template.When{
					{
						Cond: &template.BinaryExpr{
							Op:    template.OpEQ,
							Left:  &template.GetVariable{Name: "x"},
							Right: &template.Constant{Value: constant.MakeInt64(1)},
						},
						Then: &template.BinaryExpr{
							Op:    template.OpSemicolon,
							Left:  &template.SetVariable{Name: "y", Value: &template.Constant{Value: constant.MakeInt64(1)}},
							Right: &template.Constant{Value: constant.MakeBytes([]byte("1"))},
						},
					},
					{
						Cond: &template.BinaryExpr{
							Op:    template.OpEQ,
							Left:  &template.GetVariable{Name: "x"},
							Right: &template.Constant{Value: constant.MakeInt64(2)},
						},
						Then: &template.BinaryExpr{
							Op:    template.OpSemicolon,
							Left:  &template.SetVariable{Name: "y", Value: &template.Constant{Value: constant.MakeInt64(2)}},
							Right: &template.Constant{Value: constant.MakeBytes([]byte("2"))},
						},
					},
				},
				Else: &template.BinaryExpr{
					Op:    template.OpSemicolon,
					Left:  &template.SetVariable{Name: "y", Value: &template.Constant{Value: constant.MakeInt64(3)}},
					Right: &template.Constant{Value: constant.MakeBytes([]byte("3"))},
				},
			},
			"CASE WHEN @`x` = 1 THEN @`y` := 1 ; '1' WHEN @`x` = 2 THEN @`y` := 2 ; '2' ELSE @`y` := 3 ; '3' END",
			true,
		},
		{
			"timestamp '2016-01-02 15:04:05.999'",
			&template.Timestamp{Value: &template.Constant{Value: constant.MakeBytes([]byte("2016-01-02 15:04:05.999"))}},
			"TIMESTAMP '2016-01-02 15:04:05.999'",
			true,
		},
		{
			"timestamp with time zone '2016-01-02 15:04:05.999 Asia/Hong_Kong'",
			&template.Timestamp{WithTimezone: true, Value: &template.Constant{Value: constant.MakeBytes([]byte("2016-01-02 15:04:05.999 Asia/Hong_Kong"))}},
			"TIMESTAMP WITH TIME ZONE '2016-01-02 15:04:05.999 Asia/Hong_Kong'",
			true,
		},
		{
			"interval 30 minute",
			&template.Interval{Unit: template.IntervalUnitMinute, Value: &template.Constant{Value: constant.MakeInt64(30)}},
			"INTERVAL 30 MINUTE",
			true,
		},
		{
			"array['X', 'Y', 'Z']",
			&template.Array{Elems: []template.Expr{
				&template.Constant{Value: constant.MakeBytes([]byte{'X'})},
				&template.Constant{Value: constant.MakeBytes([]byte{'Y'})},
				&template.Constant{Value: constant.MakeBytes([]byte{'Z'})},
			}},
			"ARRAY['X', 'Y', 'Z']",
			true,
		},
		{
			"@x[1]",
			&template.Subscript{Base: &template.GetVariable{Name: "x"}, Index: &template.Constant{Value: constant.MakeInt64(1)}},
			"@`x`[1]",
			true,
		},
		{
			"substring('ⓘⓝⓟⓤⓣ' FROM 2 FOR 3 USING CHARACTERS)",
			&template.Substring{
				Input: &template.Constant{Value: constant.MakeBytes([]byte("ⓘⓝⓟⓤⓣ"))},
				From:  &template.Constant{Value: constant.MakeInt64(2)},
				For:   &template.Constant{Value: constant.MakeInt64(3)},
				Unit:  template.StringUnitCharacters,
			},
			"substring('ⓘⓝⓟⓤⓣ' FROM 2 FOR 3 USING CHARACTERS)",
			true,
		},
		{
			"substring('input' FROM 2 FOR 3 USING OCTETS)",
			&template.Substring{
				Input: &template.Constant{Value: constant.MakeBytes([]byte("input"))},
				From:  &template.Constant{Value: constant.MakeInt64(2)},
				For:   &template.Constant{Value: constant.MakeInt64(3)},
				Unit:  template.StringUnitOctets,
			},
			"substring('input' FROM 2 FOR 3 USING OCTETS)",
			true,
		},
		{
			"overlay('input' PLACING 'replacement' FROM 2 FOR 3 USING CHARACTERS)",
			&template.Overlay{
				Input:   &template.Constant{Value: constant.MakeBytes([]byte("input"))},
				Placing: &template.Constant{Value: constant.MakeBytes([]byte("replacement"))},
				From:    &template.Constant{Value: constant.MakeInt64(2)},
				For:     &template.Constant{Value: constant.MakeInt64(3)},
				Unit:    template.StringUnitCharacters,
			},
			"overlay('input' PLACING 'replacement' FROM 2 FOR 3 USING CHARACTERS)",
			true,
		},
		{
			"rand.regex('[0-9a-z]+', 'i', 100)",
			&template.FuncExpr{
				Name: template.NewQName("rand", "regex"),
				Args: []template.Expr{
					&template.Constant{Value: constant.MakeBytes([]byte("[0-9a-z]+"))},
					&template.Constant{Value: constant.MakeBytes([]byte("i"))},
					&template.Constant{Value: constant.MakeInt64(100)},
				},
			},
			"rand.regex('[0-9a-z]+', 'i', 100)",
			true,
		},
		{
			"not true",
			&template.UnaryExpr{
				Op:   template.OpNot,
				Expr: &template.Constant{Value: constant.MakeBool(true)},
			},
			"NOT TRUE",
			true,
		},
		{
			"-123",
			&template.UnaryExpr{
				Op:   template.OpSub,
				Expr: &template.Constant{Value: constant.MakeInt64(123)},
			},
			"- 123",
			true,
		},
		{
			"-123+456",
			&template.BinaryExpr{
				Op:    template.OpAdd,
				Left:  &template.UnaryExpr{Op: template.OpSub, Expr: &template.Constant{Value: constant.MakeInt64(123)}},
				Right: &template.Constant{Value: constant.MakeInt64(456)},
			},
			"- 123 + 456",
			true,
		},
		{
			"-123-456",
			&template.BinaryExpr{
				Op:    template.OpSub,
				Left:  &template.UnaryExpr{Op: template.OpSub, Expr: &template.Constant{Value: constant.MakeInt64(123)}},
				Right: &template.Constant{Value: constant.MakeInt64(456)},
			},
			"- 123 - 456",
			true,
		},
		{
			"~123",
			&template.UnaryExpr{
				Op:   template.OpBitNot,
				Expr: &template.Constant{Value: constant.MakeInt64(123)},
			},
			"~ 123",
			true,
		},
		{
			"not @x and @y",
			&template.BinaryExpr{
				Op: template.OpAnd,
				Left: &template.UnaryExpr{
					Op:   template.OpNot,
					Expr: &template.GetVariable{Name: "x"},
				},
				Right: &template.GetVariable{Name: "y"},
			},
			"NOT @`x` AND @`y`",
			true,
		},
		{
			"not @x > @y",
			&template.UnaryExpr{
				Op: template.OpNot,
				Expr: &template.BinaryExpr{
					Op:    template.OpGT,
					Left:  &template.GetVariable{Name: "x"},
					Right: &template.GetVariable{Name: "y"},
				},
			},
			"NOT @`x` > @`y`",
			true,
		},
		{
			"1 is 1",
			&template.BinaryExpr{
				Op:    template.OpIs,
				Left:  &template.Constant{Value: constant.MakeInt64(1)},
				Right: &template.Constant{Value: constant.MakeInt64(1)},
			},
			"1 IS 1",
			true,
		},
		{
			"1 is not 2",
			&template.BinaryExpr{
				Op:    template.OpIsNot,
				Left:  &template.Constant{Value: constant.MakeInt64(1)},
				Right: &template.Constant{Value: constant.MakeInt64(2)},
			},
			"1 IS NOT 2",
			true,
		},
		{
			"1 is not 2 is false",
			&template.BinaryExpr{
				Op: template.OpIs,
				Left: &template.BinaryExpr{
					Op:    template.OpIsNot,
					Left:  &template.Constant{Value: constant.MakeInt64(1)},
					Right: &template.Constant{Value: constant.MakeInt64(2)},
				},
				Right: &template.Constant{Value: constant.MakeBool(false)},
			},
			"1 IS NOT 2 IS FALSE",
			true,
		},
		{
			"@x:=1+1",
			&template.SetVariable{
				Name: "x",
				Value: &template.BinaryExpr{
					Op:    template.OpAdd,
					Left:  &template.Constant{Value: constant.MakeInt64(1)},
					Right: &template.Constant{Value: constant.MakeInt64(1)},
				},
			},
			"@`x` := 1 + 1",
			true,
		},
		{
			"@x:=@y:=1+1",
			&template.SetVariable{
				Name: "x",
				Value: &template.SetVariable{
					Name: "y",
					Value: &template.BinaryExpr{
						Op:    template.OpAdd,
						Left:  &template.Constant{Value: constant.MakeInt64(1)},
						Right: &template.Constant{Value: constant.MakeInt64(1)},
					},
				},
			},
			"@`x` := @`y` := 1 + 1",
			true,
		},
		{
			"123+456*(789-123)",
			&template.BinaryExpr{
				Op:   template.OpAdd,
				Left: &template.Constant{Value: constant.MakeInt64(123)},
				Right: &template.BinaryExpr{
					Op:   template.OpMul,
					Left: &template.Constant{Value: constant.MakeInt64(456)},
					Right: &template.ParenExpr{
						Expr: &template.BinaryExpr{
							Op:    template.OpSub,
							Left:  &template.Constant{Value: constant.MakeInt64(789)},
							Right: &template.Constant{Value: constant.MakeInt64(123)},
						},
					},
				},
			},
			"123 + 456 * (789 - 123)",
			true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := template.ParseExpr(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expr, expr)
			exprStr := expr.String()
			require.Equal(t, tc.exprStr, exprStr)

			reparsed, err := template.ParseExpr(exprStr)
			require.NoError(t, err)
			if tc.isExprStrSameAsInput {
				require.Equal(t, tc.expr, reparsed)
			}
		})
	}
}
