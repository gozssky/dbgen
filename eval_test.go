package dbgen_test

import (
	"testing"

	"github.com/gozssky/dbgen"
	"github.com/gozssky/dbgen/constant"
	"github.com/gozssky/dbgen/template"
	"github.com/stretchr/testify/require"
)

func TestEval(t *testing.T) {
	testCases := []struct {
		input  string
		result constant.Value
	}{
		{
			"generate_series(1, 5, 2)",
			constant.MakeArray([]constant.Value{
				constant.MakeInt64(1),
				constant.MakeInt64(3),
				constant.MakeInt64(5),
			}),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := template.ParseExpr(tc.input)
			require.NoError(t, err)
			ctx := dbgen.NewCompileContext()
			complied, err := ctx.CompileExpr(expr)
			require.NoError(t, err)
			result, err := complied.Eval(&dbgen.State{})
			require.NoError(t, err)
			require.Equal(t, tc.result, result)
		})
	}
}
