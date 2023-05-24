package dbgen

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
	"unicode"

	"github.com/gozssky/dbgen/constant"
	"github.com/gozssky/dbgen/template"
)

// Arguments is a list of arguments to a function.
type Arguments []constant.Value

// Function is a function that can be compiled.
type Function interface {
	// NumArgs returns the number of arguments the function accepts.
	// If the function accepts a variable number of arguments, it returns -1.
	NumArgs() int
	// Compile compiles or evaluates the function.
	Compile(ctx *CompileContext, args Arguments) (Compiled, error)
}

var GenericFuncs = map[string]Function{
	"generate_series":        GenerateSeriesFunc{},
	"encode.hex":             EncodeFunc{Encoding: HexEncoding},
	"encode.base64":          EncodeFunc{Encoding: Base64Encoding},
	"decode.hex":             DecodeFunc{Encoding: HexEncoding},
	"decode.base64":          DecodeFunc{Encoding: Base64Encoding},
	"debug.panic":            PanicFunc{},
	"least":                  LeastFunc{},
	"greatest":               GreatestFunc{},
	"round":                  RoundFunc{},
	"div":                    ArithFunc{Op: constant.Div},
	"mod":                    ArithFunc{Op: constant.Mod},
	"coalesce":               CoalesceFunc{},
	"rand.range":             RandRangeFunc{},
	"rand.range_inclusive":   RandRangeInclusiveFunc{},
	"rand.uniform":           RandUniformFunc{},
	"rand.uniform_inclusive": RandUniformInclusiveFunc{},
	"rand.zipf":              RandZipfFunc{},
	"rand.log_normal":        RandLogNormalFunc{},
	"rand.bool":              RandBoolFunc{},
	"rand.finite_f32":        RandFiniteF32Func{},
	"rand.finite_f64":        RandFiniteF64Func{},
	"rand.u31_timestamp":     RandU31TimestampFunc{},
	"rand.uuid":              RandUuidFunc{},
	"rand.regex":             RandRegexFunc{},
	"rand.shuffle":           RandShuffleFunc{},
	"char_length":            CharLengthFunc{},
	"octet_length":           OctetLengthFunc{},
}

var UnaryFuncs = map[template.Op]Function{
	template.OpSub:    NegFunc{},
	template.OpNot:    NotFunc{},
	template.OpIsNot:  IsNotFunc{},
	template.OpBitNot: BitNotFunc{},
}

var BinaryFuncs = map[template.Op]Function{
	template.OpLT:       CompareFunc{LT: true},
	template.OpLE:       CompareFunc{LT: true, EQ: true},
	template.OpEQ:       CompareFunc{EQ: true},
	template.OpNE:       CompareFunc{LT: true, GT: true},
	template.OpGT:       CompareFunc{GT: true},
	template.OpGE:       CompareFunc{GT: true, EQ: true},
	template.OpBitAnd:   BitwiseFunc{Op: template.OpBitAnd},
	template.OpBitOr:    BitwiseFunc{Op: template.OpBitOr},
	template.OpBitXor:   BitwiseFunc{Op: template.OpBitXor},
	template.OpAdd:      ArithFunc{Op: constant.Add},
	template.OpSub:      ArithFunc{Op: constant.Sub},
	template.OpMul:      ArithFunc{Op: constant.Mul},
	template.OpFloatDiv: ArithFunc{Op: constant.FloatDiv},
	template.OpConcat:   ConcatFunc{},
}

type noArg struct{}

func (noArg) NumArgs() int { return 0 }

type oneArg struct{}

func (oneArg) NumArgs() int { return 1 }

type twoArgs struct{}

func (twoArgs) NumArgs() int { return 2 }

type varArgs struct{}

func (varArgs) NumArgs() int { return -1 }

// ArrayFunc constructs a array.
type ArrayFunc struct {
	varArgs
}

func (ArrayFunc) Compile(_ *CompileContext, args Arguments) (Compiled, error) {
	return &Constant{constant.MakeArray(args)}, nil
}

// SubscriptFunc subscript a array.
type SubscriptFunc struct {
	twoArgs
}

func (SubscriptFunc) Compile(_ *CompileContext, args Arguments) (Compiled, error) {
	base, err := constant.AsArray(args[0])
	if err != nil {
		return nil, err
	}
	if args[1].Kind() != constant.KindInt {
		return nil, fmt.Errorf("subscript must be an integer, got %s", args[1].Kind())
	}
	if !constant.IsInt64(args[1]) {
		return &Constant{constant.Null}, nil
	}
	index, err := constant.AsInt64(args[1])
	if err != nil {
		return nil, err
	}
	if index <= 0 || index > int64(len(base)) {
		return &Constant{constant.Null}, nil
	}
	return &Constant{base[index-1]}, nil
}

// GenerateSeriesFunc implements the `generate_series` SQL function.
type GenerateSeriesFunc struct {
	varArgs
}

func (GenerateSeriesFunc) Compile(_ *CompileContext, args Arguments) (Compiled, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("generate_series requires at least 2 arguments, got %d", len(args))
	}
	if len(args) > 3 {
		return nil, fmt.Errorf("generate_series requires at most 3 arguments, got %d", len(args))
	}
	start, stop := args[0], args[1]

	var (
		step     constant.Value
		stepSign int
	)
	if len(args) == 3 {
		step = args[2]
		stepSign = constant.Sign(step)
		if stepSign == 0 {
			return nil, fmt.Errorf("generate_series step cannot be zero")
		}
	} else {
		step = constant.MakeInt64(1)
		stepSign = 1
	}

	var result []constant.Value
	value := start
	for {
		cmp, isNull, err := constant.Cmp(value, stop)
		if err != nil {
			return nil, err
		}
		if isNull || cmp == stepSign {
			break
		}
		result = append(result, value)
		value, err = constant.Add(value, step)
		if err != nil {
			return nil, err
		}
	}
	return &Constant{constant.MakeArray(result)}, nil
}

// Encoding is an interface for encoding and decoding byte slices.
type Encoding interface {
	// Encode encodes src into EncodedLen(len(src)) bytes of dst.
	Encode(dst, src []byte)
	// EncodedLen returns the length of an encoding of n source bytes.
	EncodedLen(n int) int
	// Decode decodes src into DecodedLen(len(src)) bytes,
	// returning the actual number of bytes written to dst.
	Decode(dst, src []byte) (n int, err error)
	// DecodedLen returns the maximum length in bytes of the decoded data
	// corresponding to n bytes of encoded data.
	DecodedLen(n int) int
}

var (
	HexEncoding    Encoding = hexEncoding{}
	Base64Encoding Encoding = base64.StdEncoding
)

type hexEncoding struct{}

func (hexEncoding) Encode(dst, src []byte) {
	hex.Encode(dst, src)
}

func (hexEncoding) EncodedLen(n int) int {
	return hex.EncodedLen(n)
}

func (hexEncoding) Decode(dst, src []byte) (int, error) {
	return hex.Decode(dst, src)
}

func (hexEncoding) DecodedLen(n int) int {
	return hex.DecodedLen(n)
}

// EncodeFunc implements the `encode.*` SQL function.
type EncodeFunc struct {
	oneArg
	Encoding Encoding
}

func (enc EncodeFunc) Compile(_ *CompileContext, args Arguments) (Compiled, error) {
	src, err := constant.AsBytes(args[0])
	if err != nil {
		return nil, err
	}
	dst := make([]byte, enc.Encoding.EncodedLen(len(src)))
	enc.Encoding.Encode(dst, src)
	return &Constant{constant.MakeBytes(dst)}, nil
}

// DecodeFunc implements the `decode.*` SQL function.
type DecodeFunc struct {
	oneArg
	Encoding Encoding
}

func (dec DecodeFunc) Compile(_ *CompileContext, args Arguments) (Compiled, error) {
	src, err := constant.AsBytes(args[0])
	if err != nil {
		return nil, err
	}
	dst := make([]byte, dec.Encoding.DecodedLen(len(src)))
	n, err := dec.Encoding.Decode(dst, src)
	if err != nil {
		return nil, err
	}
	return &Constant{constant.MakeBytes(dst[:n])}, nil
}

type PanicFunc struct {
	varArgs
}

func (PanicFunc) Compile(_ *CompileContext, args Arguments) (Compiled, error) {
	var sb strings.Builder
	sb.WriteString("runtime panic: ")
	for i, arg := range args {
		fmt.Fprintf(&sb, "%d. %s", i, arg)
	}
	return nil, errors.New(sb.String())
}

type NegFunc struct {
	oneArg
}

func (NegFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	result, err := constant.Neg(args[0])
	if err != nil {
		return nil, err
	}
	return &Constant{result}, nil
}

// CompareFunc implements the value comparison (`<`, `=`, `>`, `<=`, `<>`, `>=`) SQL functions.
type CompareFunc struct {
	twoArgs
	/// Whether a less-than result is considered TRUE.
	LT bool
	/// Whether an equals result is considered TRUE.
	EQ bool
	/// Whether a greater-than result is considered TRUE.
	GT bool
}

func (c CompareFunc) Compile(_ *CompileContext, args Arguments) (Compiled, error) {
	cmp, isNull, err := constant.Cmp(args[0], args[1])
	if err != nil {
		return nil, err
	}
	if isNull {
		return &Constant{constant.Null}, nil
	}
	switch cmp {
	case -1:
		return &Constant{constant.MakeBool(c.LT)}, nil
	case 0:
		return &Constant{constant.MakeBool(c.EQ)}, nil
	default:
		return &Constant{constant.MakeBool(c.GT)}, nil
	}
}

// IsFunc implements the 'IS' SQL function.
type IsFunc struct{}

func (IsFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// IsNotFunc implements the 'IS NOT' SQL function.
type IsNotFunc struct {
	oneArg
}

func (IsNotFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// NotFunc implements the 'NOT' SQL function.
type NotFunc struct {
	oneArg
}

func (NotFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// BitNotFunc implements the '~' SQL function.
type BitNotFunc struct {
	oneArg
}

func (BitNotFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// BitwiseFunc implements the bitwise ('&', '|', '^') SQL functions.
type BitwiseFunc struct {
	twoArgs
	Op template.Op
}

func (b BitwiseFunc) Compile(_ *CompileContext, args Arguments) (Compiled, error) {
	if args[0] == constant.Null || args[1] == constant.Null {
		return &Constant{constant.Null}, nil
	}
	if constant.IsInt64(args[0]) && constant.IsInt64(args[1]) {
		x, _ := constant.AsInt64(args[0])
		y, _ := constant.AsInt64(args[1])
		switch b.Op {
		case template.OpBitAnd:
			return &Constant{constant.MakeInt64(x & y)}, nil
		case template.OpBitOr:
			return &Constant{constant.MakeInt64(x | y)}, nil
		case template.OpBitXor:
			return &Constant{constant.MakeInt64(x ^ y)}, nil
		default:
			return nil, fmt.Errorf("unknown bitwise operator: %v", b.Op)
		}
	}
	x, err := constant.AsInt(args[0])
	if err != nil {
		return nil, err
	}
	y, err := constant.AsInt(args[1])
	if err != nil {
		return nil, err
	}
	switch b.Op {
	case template.OpBitAnd:
		return &Constant{constant.MakeInt(new(big.Int).And(x, y))}, nil
	case template.OpBitOr:
		return &Constant{constant.MakeInt(new(big.Int).Or(x, y))}, nil
	case template.OpBitXor:
		return &Constant{constant.MakeInt(new(big.Int).Xor(x, y))}, nil
	default:
		return nil, fmt.Errorf("unknown bitwise operator: %v", b.Op)
	}
}

// LogicalAndFunc implements the 'AND' SQL function.
type LogicalAndFunc struct{}

func (LogicalAndFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// LogicalOrFunc implements the 'OR' SQL function.
type LogicalOrFunc struct{}

func (LogicalOrFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// ArithFunc implements the arithmetic (`+`, `-`, `*`, `/`, div, mod) SQL functions.
type ArithFunc struct {
	twoArgs
	Op func(constant.Value, constant.Value) (constant.Value, error)
}

func (a ArithFunc) Compile(_ *CompileContext, args Arguments) (Compiled, error) {
	result, err := a.Op(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return &Constant{result}, nil
}

// GreatestFunc implements the 'greatest' SQL function.
type GreatestFunc struct {
	varArgs
}

func (GreatestFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// LeastFunc implements the 'least' SQL function.
type LeastFunc struct {
	varArgs
}

func (LeastFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RoundFunc implements the 'round' SQL function.
type RoundFunc struct {
	oneArg
}

func (RoundFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// CoalesceFunc implements the 'coalesce' SQL function.
type CoalesceFunc struct {
	varArgs
}

func (CoalesceFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// LastFunc is a function that returns the last value in a list of arguments.
type LastFunc struct{}

func (LastFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandRangeFunc implements the 'rand.range' SQL function.
type RandRangeFunc struct {
	twoArgs
}

func (RandRangeFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandRangeInclusiveFunc implements the 'rand.range_inclusive' SQL function.
type RandRangeInclusiveFunc struct {
	twoArgs
}

func (RandRangeInclusiveFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandUniformFunc implements the 'rand.uniform' SQL function.
type RandUniformFunc struct {
	twoArgs
}

func (RandUniformFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandUniformInclusiveFunc implements the 'rand.uniform_inclusive' SQL function.
type RandUniformInclusiveFunc struct {
	twoArgs
}

func (RandUniformInclusiveFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandZipfFunc implements the 'rand.zipf' SQL function.
type RandZipfFunc struct {
	twoArgs
}

func (RandZipfFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandLogNormalFunc implements the 'rand.log_normal' SQL function.
type RandLogNormalFunc struct {
	twoArgs
}

func (RandLogNormalFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandBoolFunc implements the 'rand.bool' SQL function.
type RandBoolFunc struct {
	noArg
}

func (RandBoolFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandFiniteF32Func implements the 'rand.finite_f32' SQL function.
type RandFiniteF32Func struct {
	noArg
}

func (RandFiniteF32Func) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandFiniteF64Func implements the 'rand.finite_f64' SQL function.
type RandFiniteF64Func struct {
	noArg
}

func (RandFiniteF64Func) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandU31TimestampFunc implements the 'rand.u31_timestamp' SQL function.
type RandU31TimestampFunc struct {
	noArg
}

func (RandU31TimestampFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandUuidFunc implements the 'rand.uuid' SQL function.
type RandUuidFunc struct {
	noArg
}

func (RandUuidFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandRegexFunc implements the 'rand.regex' SQL function.
type RandRegexFunc struct {
	oneArg
}

func (RandRegexFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// RandShuffleFunc implements the 'rand.shuffle' SQL function.
type RandShuffleFunc struct {
	oneArg
}

func (RandShuffleFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// SubstringFunc implements the 'substring' SQL function.
type SubstringFunc struct {
	varArgs
	Unit template.StringUnit
}

func (SubstringFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// CharLengthFunc implements the 'char_length' SQL function.
type CharLengthFunc struct {
	oneArg
}

func (CharLengthFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// OctetLengthFunc implements the 'octet_length' SQL function.
type OctetLengthFunc struct {
	oneArg
}

func (OctetLengthFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// OverlayFunc implements the 'overlay' SQL function.
type OverlayFunc struct {
	varArgs
	Unit template.StringUnit
}

func (OverlayFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

// ConcatFunc implements the '||' SQL function.
type ConcatFunc struct {
	twoArgs
}

func (ConcatFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	panic("unimplemented")
}

const timestampFormat = "2006-01-02 15:04:05.999"

// TimestampFunc implements the 'timestamp' SQL function.
type TimestampFunc struct {
	oneArg
}

func (TimestampFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	input, err := constant.AsBytes(args[0])
	if err != nil {
		return nil, err
	}
	t, err := time.ParseInLocation(timestampFormat, string(input), ctx.TimeZone)
	if err != nil {
		return nil, err
	}
	return &Constant{constant.MakeTimestamp(t)}, nil
}

// TimestampWithTimeZoneFunc implements the 'timestamp with time zone' SQL function.
type TimestampWithTimeZoneFunc struct {
	oneArg
}

func (TimestampWithTimeZoneFunc) Compile(ctx *CompileContext, args Arguments) (Compiled, error) {
	input, err := constant.AsBytes(args[0])
	if err != nil {
		return nil, err
	}
	tz := ctx.TimeZone
	if tzIdx := bytes.IndexFunc(input, unicode.IsLetter); tzIdx != -1 {
		tz, err = ctx.ParseTimeZone(string(input[tzIdx:]))
		if err != nil {
			return nil, err
		}
		input = input[:tzIdx]
	}
	t, err := time.ParseInLocation(timestampFormat, strings.TrimSpace(string(input)), tz)
	if err != nil {
		return nil, err
	}
	return &Constant{constant.MakeTimestamp(t)}, nil
}
