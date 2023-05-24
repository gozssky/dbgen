package constant_test

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/gozssky/dbgen/constant"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	testCases := []struct {
		val constant.Value
		str string
	}{
		{constant.Null, "NULL"},
		{constant.MakeBool(true), "TRUE"},
		{constant.MakeBool(false), "FALSE"},
		{constant.MakeBytes([]byte("abc")), "abc"},
		{constant.MakeInt64(123), "123"},
		{constant.MakeFloat(123.456), "123.456"},
		{constant.MakeTimestamp(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)), "2019-01-01 00:00:00"},
		{constant.MakeInterval(time.Hour), "1h0m0s"},
		{constant.MakeArray([]constant.Value{constant.MakeInt64(1), constant.MakeInt64(2)}), "[1, 2]"},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.str, tc.val.String())
	}
}

func TestMakeNumericFromLiteral(t *testing.T) {
	testCases := []struct {
		lit string
		val constant.Value
	}{
		{"123", constant.MakeInt64(123)},
		{"123.456", constant.MakeFloat(123.456)},
		{"123.456e7", constant.MakeFloat(123.456e7)},
		{"12345678901234567890123456789012345678901234567890123456789012345678901234567890",
			constant.MakeInt(makeBigInt("12345678901234567890123456789012345678901234567890123456789012345678901234567890", 10))},
	}

	for _, tc := range testCases {
		val, err := constant.MakeNumberFromLiteral(tc.lit)
		require.NoError(t, err)
		require.Equal(t, tc.val, val)
	}
}

func makeBigInt(s string, base int) *big.Int {
	i, ok := new(big.Int).SetString(s, base)
	if !ok {
		panic(fmt.Sprintf("invalid big.Int literal: %s", s))
	}
	return i
}
