package dbgen_test

import (
	"testing"

	"github.com/gozssky/dbgen"
	"github.com/gozssky/dbgen/constant"
	"github.com/stretchr/testify/require"
)

func TestUnaryFuncArgs(t *testing.T) {
	for _, fn := range dbgen.UnaryFuncs {
		require.Equal(t, 1, fn.NumArgs())
	}
}

func TestBinaryFuncArgs(t *testing.T) {
	for _, fn := range dbgen.BinaryFuncs {
		require.Equal(t, 2, fn.NumArgs())
	}
}

func TestArrayFunc(t *testing.T) {
	testCases := []struct {
		name   string
		args   dbgen.Arguments
		result dbgen.Compiled
	}{
		{
			"nil",
			dbgen.Arguments{},
			&dbgen.Constant{Value: constant.MakeArray(nil)},
		},
		{
			"empty",
			dbgen.Arguments{},
			&dbgen.Constant{Value: constant.MakeArray(nil)},
		},
		{
			"single",
			dbgen.Arguments{
				constant.MakeInt64(1),
			},
			&dbgen.Constant{
				Value: constant.MakeArray([]constant.Value{
					constant.MakeInt64(1),
				}),
			},
		},
		{
			"multiple",
			dbgen.Arguments{
				constant.Null,
				constant.MakeInt64(1),
				constant.MakeFloat(2.0),
			},
			&dbgen.Constant{
				Value: constant.MakeArray([]constant.Value{
					constant.Null,
					constant.MakeInt64(1),
					constant.MakeFloat(2.0),
				}),
			},
		},
	}

	ctx := dbgen.NewCompileContext()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fn := &dbgen.ArrayFunc{}
			result, err := fn.Compile(ctx, tc.args)
			require.NoError(t, err)
			require.Equal(t, tc.result, result)
		})
	}
}

func TestSubscriptFunc(t *testing.T) {

}

func TestGenerateSeriesFunc(t *testing.T) {

}

func TestEncodeFunc(t *testing.T) {

}

func TestDecodeFunc(t *testing.T) {

}

func TestPanicFunc(t *testing.T) {

}

func TestNegFunc(t *testing.T) {

}

func TestCompareLTFunc(t *testing.T) {

}

func TestCompareLEFunc(t *testing.T) {

}

func TestCompareEQFunc(t *testing.T) {

}

func TestCompareNEFunc(t *testing.T) {

}

func TestCompareGEFunc(t *testing.T) {

}

func TestCompareGTFunc(t *testing.T) {

}

func TestIsFunc(t *testing.T) {

}

func TestIsNotFunc(t *testing.T) {

}

func TestBitNotFunc(t *testing.T) {

}

func TestBitAndFunc(t *testing.T) {

}

func TestBitXorFunc(t *testing.T) {

}

func TestLogicalAndFunc(t *testing.T) {

}

func TestLogicalOrFunc(t *testing.T) {

}

func TestAddFunc(t *testing.T) {

}

func TestSubFunc(t *testing.T) {

}

func TestMulFunc(t *testing.T) {

}

func TestFloatDivFunc(t *testing.T) {

}

func TestDivFunc(t *testing.T) {

}

func TestModFunc(t *testing.T) {

}

func TestGreatestFunc(t *testing.T) {

}

func TestLeastFunc(t *testing.T) {

}

func TestRoundFunc(t *testing.T) {

}

func TestCoalesceFunc(t *testing.T) {

}

func TestLastFunc(t *testing.T) {

}

func TestRandRangeFunc(t *testing.T) {

}

func TestRandRangeInclusiveFunc(t *testing.T) {

}

func TestRandUniformFunc(t *testing.T) {

}

func TestRandUniformInclusiveFunc(t *testing.T) {

}

func TestRandZipfFunc(t *testing.T) {

}

func TestRandLogNormalFunc(t *testing.T) {

}

func TestRandBoolFunc(t *testing.T) {

}

func TestRandFiniteF32Func(t *testing.T) {

}

func TestRandFiniteF64Func(t *testing.T) {

}

func TestRandU31TimestampFunc(t *testing.T) {

}

func TestRandUuidFunc(t *testing.T) {

}

func TestRandRegexFunc(t *testing.T) {

}

func TestRandShuffleFunc(t *testing.T) {

}

func TestSubstringFunc(t *testing.T) {

}

func TestCharLengthFunc(t *testing.T) {

}

func TestOctetLengthFunc(t *testing.T) {

}

func TestOverlayFunc(t *testing.T) {

}

func TestConcatFunc(t *testing.T) {

}

func TestTimestampFunc(t *testing.T) {

}

func TestTimestampWithTimeZoneFunc(t *testing.T) {

}
