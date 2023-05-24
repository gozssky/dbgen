package constant

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/constraints"
)

// Kind specifies the kind of value represented by a Value.
//
//go:generate stringer -type=Kind -trimprefix=Kind
type Kind int

const (
	KindNull Kind = iota
	KindBool
	KindBytes
	KindInt
	KindFloat
	KindTimestamp
	KindInterval
	KindArray
)

type Value interface {
	Kind() Kind
	String() string
}

var Null Value = nullVal{}

type (
	nullVal      struct{}
	boolVal      bool
	bytesVal     []byte
	intVal       struct{ val *big.Int }
	int64Val     int64
	floatVal     float64
	timestampVal struct{ val time.Time }
	intervalVal  struct{ val time.Duration }
	arrayVal     []Value
)

func (nullVal) Kind() Kind      { return KindNull }
func (boolVal) Kind() Kind      { return KindBool }
func (bytesVal) Kind() Kind     { return KindBytes }
func (intVal) Kind() Kind       { return KindInt }
func (int64Val) Kind() Kind     { return KindInt }
func (floatVal) Kind() Kind     { return KindFloat }
func (timestampVal) Kind() Kind { return KindTimestamp }
func (intervalVal) Kind() Kind  { return KindInterval }
func (arrayVal) Kind() Kind     { return KindArray }

func (nullVal) String() string {
	return "NULL"
}

func (b boolVal) String() string {
	if b {
		return "TRUE"
	} else {
		return "FALSE"
	}
}

func (b bytesVal) String() string {
	return string(b)
}

func (i int64Val) String() string {
	return strconv.FormatInt(int64(i), 10)
}

func (i intVal) String() string {
	return i.val.String()
}

func (f floatVal) String() string {
	return strconv.FormatFloat(float64(f), 'g', -1, 64)
}

func (t timestampVal) String() string {
	return t.val.Format("2006-01-02 15:04:05.999")
}

func (i intervalVal) String() string {
	return i.val.String()
}

func (a arrayVal) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, v := range a {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(v.String())
	}
	b.WriteByte(']')
	return b.String()
}

func MakeBool(b bool) Value {
	return boolVal(b)
}

func MakeBytes(b []byte) Value {
	return bytesVal(b)
}

func MakeInt64(i int64) Value {
	return int64Val(i)
}

func MakeInt(i *big.Int) Value {
	if i.IsInt64() {
		return int64Val(i.Int64())
	}
	return intVal{i}
}

func MakeFloat(f float64) Value {
	return floatVal(f)
}

func MakeTimestamp(t time.Time) Value {
	return timestampVal{t}
}

func MakeInterval(d time.Duration) Value {
	return intervalVal{d}
}

func MakeArray(a []Value) Value {
	if len(a) == 0 {
		return arrayVal{nil}
	}
	return arrayVal(a)
}

// MakeNumberFromLiteral makes a numeric value from a string literal.
// The string literal may be a decimal integer, a hexadecimal integer,
// or a floating-point number.
func MakeNumberFromLiteral(s string) (Value, error) {
	i, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return MakeInt64(i), nil
	}

	bi := new(big.Int)
	if _, ok := bi.SetString(s, 0); ok {
		return intVal{bi}, nil
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, err
	}
	return MakeFloat(f), nil
}

type ConvertError struct {
	From Value
	To   string
}

func (e *ConvertError) Error() string {
	return fmt.Sprintf("cannot convert %s(%s) to %s", e.From.Kind(), e.From.String(), e.To)
}

func AsBool(v Value) (bool, error) {
	switch v := v.(type) {
	case boolVal:
		return bool(v), nil
	default:
		return false, &ConvertError{v, "bool"}
	}
}

func AsBytes(v Value) ([]byte, error) {
	switch v := v.(type) {
	case bytesVal:
		return v, nil
	default:
		return nil, &ConvertError{v, "[]byte"}
	}
}

// IsInt64 reports whether v can be represented as an int64.
func IsInt64(v Value) bool {
	switch v := v.(type) {
	case intVal:
		return v.val.IsInt64()
	case int64Val:
		return true
	default:
		return false
	}
}

func AsInt64(v Value) (int64, error) {
	switch v := v.(type) {
	case intVal:
		if !v.val.IsInt64() {
			return 0, &ConvertError{v, "int64"}
		}
		return v.val.Int64(), nil
	case int64Val:
		return int64(v), nil
	default:
		return 0, &ConvertError{v, "int64"}
	}
}

func AsInt(v Value) (*big.Int, error) {
	switch v := v.(type) {
	case intVal:
		return v.val, nil
	case int64Val:
		return big.NewInt(int64(v)), nil
	default:
		return nil, &ConvertError{v, "*big.Int"}
	}
}

func AsFloat(v Value) (float64, error) {
	switch v := v.(type) {
	case floatVal:
		return float64(v), nil
	case intVal:
		f, _ := new(big.Float).SetInt(v.val).Float64()
		return f, nil
	case int64Val:
		return float64(v), nil
	default:
		return 0, &ConvertError{v, "float64"}
	}
}

func AsTimestamp(v Value) (time.Time, error) {
	switch v := v.(type) {
	case timestampVal:
		return v.val, nil
	default:
		return time.Time{}, &ConvertError{v, "time.Time"}
	}
}

func AsInterval(v Value) (time.Duration, error) {
	switch v := v.(type) {
	case intervalVal:
		return v.val, nil
	default:
		return 0, &ConvertError{v, "time.Duration"}
	}
}

func AsArray(v Value) ([]Value, error) {
	switch v := v.(type) {
	case arrayVal:
		return v, nil
	default:
		return nil, &ConvertError{v, "[]Value"}
	}
}

func Sign(v Value) int {
	switch v := v.(type) {
	case intVal:
		return v.val.Sign()
	case int64Val:
		return numberCmp(v, 0)
	case floatVal:
		return numberCmp(v, 0)
	case intervalVal:
		return numberCmp(v.val, 0)
	default:
		return 1
	}
}

type CompareError struct {
	Left, Right Value
}

func (e *CompareError) Error() string {
	return fmt.Sprintf("cannot compare %s(%s) with %s(%s)", e.Left.Kind(), e.Left.String(), e.Right.Kind(), e.Right.String())
}

func Cmp(a, b Value) (_ int, isNull bool, retErr error) {
	if a == Null || b == Null {
		return 0, true, nil
	}

	defer func() {
		if retErr != nil {
			if _, ok := retErr.(*CompareError); !ok {
				retErr = &CompareError{a, b}
			}
		}
	}()

	switch a := a.(type) {
	case boolVal:
		b, err := AsBool(b)
		if err != nil {
			return 0, false, err
		}
		if bool(a) == b {
			return 0, false, nil
		} else if !bool(a) && b {
			return -1, false, nil
		} else {
			return 1, false, nil
		}
	case bytesVal:
		b, err := AsBytes(b)
		if err != nil {
			return 0, false, err
		}
		return bytes.Compare(a, b), false, nil
	case intVal:
		switch b.Kind() {
		case KindInt:
			b, err := AsInt(b)
			if err != nil {
				return 0, false, err
			}
			return a.val.Cmp(b), false, nil
		case KindFloat:
			a1, _ := new(big.Float).SetInt(a.val).Float64()
			b, err := AsFloat(b)
			if err != nil {
				return 0, false, err
			}
			return numberCmp(a1, b), false, nil
		}
	case int64Val:
		if IsInt64(b) {
			b, err := AsInt64(b)
			if err != nil {
				return 0, false, err
			}
			return numberCmp(int64(a), b), false, nil
		}
		switch b.Kind() {
		case KindInt:
			a1 := big.NewInt(int64(a))
			b, err := AsInt(b)
			if err != nil {
				return 0, false, err
			}
			return a1.Cmp(b), false, nil
		case KindFloat:
			b, err := AsFloat(b)
			if err != nil {
				return 0, false, err
			}
			return numberCmp(float64(a), b), false, nil
		}
	case floatVal:
		b, err := AsFloat(b)
		if err != nil {
			return 0, false, err
		}
		return numberCmp(float64(a), b), false, nil
	case timestampVal:
		b, err := AsTimestamp(b)
		if err != nil {
			return 0, false, err
		}
		return a.val.Compare(b), false, nil
	case intervalVal:
		b, err := AsInterval(b)
		if err != nil {
			return 0, false, err
		}
		return numberCmp(a.val, b), false, nil
	case arrayVal:
		b, err := AsArray(b)
		if err != nil {
			return 0, false, err
		}
		l := len(a)
		if len(b) < l {
			l = len(b)
		}
		for i := 0; i < l; i++ {
			if r, isNull, err := Cmp(a[i], b[i]); err != nil || isNull {
				return 0, isNull, err
			} else if r != 0 {
				return r, false, nil
			}
		}
		return numberCmp(len(a), len(b)), false, nil
	}
	return 0, false, &CompareError{a, b}
}

func numberCmp[T constraints.Ordered](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

type BinaryOpError struct {
	Op    string
	Left  Value
	Right Value
	Cause error
}

func (e *BinaryOpError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("cannot perform %s on %s(%s) and %s(%s): %v", e.Op, e.Left.Kind(), e.Left.String(), e.Right.Kind(), e.Right.String(), e.Cause)
	}
	return fmt.Sprintf("cannot perform %s on %s(%s) and %s(%s)", e.Op, e.Left.Kind(), e.Left.String(), e.Right.Kind(), e.Right.String())
}

func (e *BinaryOpError) Unwrap() error {
	return e.Cause
}

func MakeBinaryOpError(op string, left, right Value, cause error) error {
	return &BinaryOpError{op, left, right, cause}
}

func Add(a, b Value) (_ Value, retErr error) {
	if a == Null || b == Null {
		return Null, nil
	}

	defer func() {
		if retErr != nil {
			if _, ok := retErr.(*BinaryOpError); !ok {
				retErr = MakeBinaryOpError("add", a, b, retErr)
			}
		}
	}()

	switch a := a.(type) {
	case intVal:
		switch b.Kind() {
		case KindInt:
			b, err := AsInt(b)
			if err != nil {
				return nil, err
			}
			return intVal{new(big.Int).Add(a.val, b)}, nil
		case KindFloat:
			a1, _ := new(big.Float).SetInt(a.val).Float64()
			b, err := AsFloat(b)
			if err != nil {
				return nil, err
			}
			return floatVal(a1 + b), nil
		}
	case int64Val:
		if IsInt64(b) {
			b, err := AsInt64(b)
			if err != nil {
				return nil, err
			}
			sum := int64(a) + b
			if (sum > int64(a)) == (b > 0) {
				return int64Val(sum), nil
			}
			// Overflow.
		}
		switch b.Kind() {
		case KindInt:
			a1 := big.NewInt(int64(a))
			b, err := AsInt(b)
			if err != nil {
				return nil, err
			}
			return intVal{a1.Add(a1, b)}, nil
		case KindFloat:
			b, err := AsFloat(b)
			if err != nil {
				return nil, err
			}
			return floatVal(float64(a) + b), nil
		}
	case floatVal:
		b, err := AsFloat(b)
		if err != nil {
			return nil, err
		}
		return a + floatVal(b), nil
	case timestampVal:
		b, err := AsInterval(b)
		if err != nil {
			return nil, err
		}
		return timestampVal{a.val.Add(b)}, nil
	case intervalVal:
		t, ok := b.(timestampVal)
		if ok {
			return timestampVal{t.val.Add(a.val)}, nil
		}
		b, err := AsInterval(b)
		if err != nil {
			return nil, err
		}
		return intervalVal{a.val + b}, nil
	}
	return nil, MakeBinaryOpError("add", a, b, nil)
}

func Sub(a, b Value) (_ Value, retErr error) {
	if a == Null || b == Null {
		return Null, nil
	}

	defer func() {
		if retErr != nil {
			if _, ok := retErr.(*BinaryOpError); !ok {
				retErr = MakeBinaryOpError("sub", a, b, retErr)
			}
		}
	}()

	switch a := a.(type) {
	case intVal:
		switch b.Kind() {
		case KindInt:
			b, err := AsInt(b)
			if err != nil {
				return nil, err
			}
			return intVal{new(big.Int).Sub(a.val, b)}, nil
		case KindFloat:
			a1, _ := new(big.Float).SetInt(a.val).Float64()
			b, err := AsFloat(b)
			if err != nil {
				return nil, err
			}
			return floatVal(a1 - b), nil
		}
	case int64Val:
		if IsInt64(b) {
			b, err := AsInt64(b)
			if err != nil {
				return nil, err
			}
			sum := int64(a) + b
			if (sum > int64(a)) == (b < 0) {
				return int64Val(sum), nil
			}
			// Overflow.
		}
		switch b.Kind() {
		case KindInt:
			a1 := big.NewInt(int64(a))
			b, err := AsInt(b)
			if err != nil {
				return nil, err
			}
			return intVal{a1.Sub(a1, b)}, nil
		case KindFloat:
			b, err := AsFloat(b)
			if err != nil {
				return nil, err
			}
			return floatVal(float64(a) - b), nil
		}
	case floatVal:
		b, err := AsFloat(b)
		if err != nil {
			return nil, err
		}
		return a - floatVal(b), nil
	case timestampVal:
		b, err := AsInterval(b)
		if err != nil {
			return nil, err
		}
		return timestampVal{a.val.Add(-b)}, nil
	case intervalVal:
		t, ok := b.(timestampVal)
		if ok {
			return timestampVal{t.val.Add(-a.val)}, nil
		}
		b, err := AsInterval(b)
		if err != nil {
			return nil, err
		}
		return intervalVal{a.val - b}, nil
	}
	return nil, MakeBinaryOpError("sub", a, b, nil)
}

func Mul(a, b Value) (_ Value, retErr error) {
	if a == Null || b == Null {
		return Null, nil
	}

	defer func() {
		if retErr != nil {
			if _, ok := retErr.(*BinaryOpError); !ok {
				retErr = MakeBinaryOpError("mul", a, b, retErr)
			}
		}
	}()

	switch a := a.(type) {
	case intVal:
		switch b.Kind() {
		case KindInt:
			b, err := AsInt(b)
			if err != nil {
				return nil, err
			}
			return intVal{new(big.Int).Mul(a.val, b)}, nil
		case KindFloat:
			a1, _ := new(big.Float).SetInt(a.val).Float64()
			b, err := AsFloat(b)
			if err != nil {
				return nil, err
			}
			return floatVal(a1 * b), nil
		case KindInterval:
			b, err := AsInterval(b)
			if err != nil {
				return nil, err
			}
			result := new(big.Int).Mul(a.val, big.NewInt(int64(b)))
			return intervalVal{time.Duration(result.Int64())}, nil
		}
	case int64Val:
		if IsInt64(b) {
			b, err := AsInt64(b)
			if err != nil {
				return nil, err
			}
			result := int64(a) * b
			if a == 0 || b == 0 || a == 1 || b == 1 || (result/int64(a) == b) {
				return int64Val(result), nil
			}
			// Overflow.
		}
		switch b.Kind() {
		case KindInt:
			a1 := big.NewInt(int64(a))
			b, err := AsInt(b)
			if err != nil {
				return nil, err
			}
			return intVal{a1.Mul(a1, b)}, nil
		case KindFloat:
			b, err := AsFloat(b)
			if err != nil {
				return nil, err
			}
			return floatVal(float64(a) * b), nil
		case KindInterval:
			b, err := AsInterval(b)
			if err != nil {
				return nil, err
			}
			return intervalVal{time.Duration(int64(a) * int64(b))}, nil
		}
	case floatVal:
		if b.Kind() == KindInterval {
			b, err := AsInterval(b)
			if err != nil {
				return nil, err
			}
			return intervalVal{time.Duration(a * floatVal(b))}, nil
		}
		b, err := AsFloat(b)
		if err != nil {
			return nil, err
		}
		return a * floatVal(b), nil
	case intervalVal:
		switch b.Kind() {
		case KindInt:
			if a.val == 0 {
				return intervalVal{0}, nil
			}
			if IsInt64(b) {
				b, err := AsInt64(b)
				if err != nil {
					return nil, err
				}
				result := int64(a.val) * b
				if b == 0 || a.val == 1 || b == 1 || (result/int64(a.val) == b) {
					return intervalVal{time.Duration(result)}, nil
				}
			}
		case KindFloat:
			b, err := AsFloat(b)
			if err != nil {
				return nil, err
			}
			return intervalVal{time.Duration(float64(a.val) * b)}, nil
		}
	}
	return nil, MakeBinaryOpError("mul", a, b, nil)
}

var ErrDivideByZero = errors.New("divide by zero")

func Div(a, b Value) (_ Value, retErr error) {
	if a == Null || b == Null {
		return Null, nil
	}

	defer func() {
		if retErr != nil {
			if _, ok := retErr.(*BinaryOpError); !ok {
				retErr = MakeBinaryOpError("div", a, b, retErr)
			}
		}
	}()

	switch a := a.(type) {
	case intVal:
		switch b.Kind() {
		case KindInt:
			b, err := AsInt(b)
			if err != nil {
				return nil, err
			}
			if b.IsInt64() && b.Int64() == 0 {
				return nil, ErrDivideByZero
			}
			return intVal{new(big.Int).Div(a.val, b)}, nil
		case KindFloat:
			a1, _ := new(big.Float).SetInt(a.val).Float64()
			b, err := AsFloat(b)
			if err != nil {
				return nil, err
			}
			if b == 0 {
				return nil, ErrDivideByZero
			}
			return floatVal(a1 / b), nil
		}
	case int64Val:
		if IsInt64(b) {
			b, err := AsInt64(b)
			if err != nil {
				return nil, err
			}
			if b == 0 {
				return nil, ErrDivideByZero
			}
			return int64Val(int64(a) / b), nil
		}
		switch b.Kind() {
		case KindInt:
			a1 := big.NewInt(int64(a))
			b, err := AsInt(b)
			if err != nil {
				return nil, err
			}
			return intVal{a1.Div(a1, b)}, nil
		case KindFloat:
			b, err := AsFloat(b)
			if err != nil {
				return nil, err
			}
			return floatVal(float64(a) / b), nil
		}
	case floatVal:
		b, err := AsFloat(b)
		if err != nil {
			return nil, err
		}
		if b == 0 {
			return nil, ErrDivideByZero
		}
		return a / floatVal(b), nil
	}
	return nil, MakeBinaryOpError("div", a, b, nil)
}

func FloatDiv(a, b Value) (_ Value, retErr error) {
	if a == Null || b == Null {
		return Null, nil
	}

	defer func() {
		if retErr != nil {
			if _, ok := retErr.(*BinaryOpError); !ok {
				retErr = MakeBinaryOpError("float_div", a, b, retErr)
			}
		}
	}()

	switch a.Kind() {
	case KindInt, KindFloat:
		a1, err := AsFloat(a)
		if err != nil {
			return nil, err
		}
		b1, err := AsFloat(b)
		if err != nil {
			return nil, err
		}
		if b1 == 0 {
			return nil, ErrDivideByZero
		}
		return floatVal(a1 / b1), nil
	case KindInterval:
		a1, err := AsInterval(a)
		if err != nil {
			return nil, err
		}
		b1, err := AsFloat(b)
		if err != nil {
			return nil, err
		}
		if b1 == 0 {
			return nil, errors.New("divide by zero")
		}
		return intervalVal{time.Duration(float64(a1) / b1)}, nil
	}
	return nil, MakeBinaryOpError("float_div", a, b, nil)
}

func Mod(a, b Value) (_ Value, retErr error) {
	if a == Null || b == Null {
		return Null, nil
	}

	defer func() {
		if retErr != nil {
			if _, ok := retErr.(*BinaryOpError); !ok {
				retErr = MakeBinaryOpError("mod", a, b, retErr)
			}
		}
	}()

	switch a := a.(type) {
	case intVal:
		switch b.Kind() {
		case KindInt:
			b, err := AsInt(b)
			if err != nil {
				return nil, err
			}
			if b.IsInt64() && b.Int64() == 0 {
				return nil, ErrDivideByZero
			}
			return intVal{new(big.Int).Mod(a.val, b)}, nil
		case KindFloat:
			a1, _ := new(big.Float).SetInt(a.val).Float64()
			b, err := AsFloat(b)
			if err != nil {
				return nil, err
			}
			if b == 0 {
				return nil, ErrDivideByZero
			}
			return floatVal(math.Mod(a1, b)), nil
		}
	case int64Val:
		if IsInt64(b) {
			b, err := AsInt64(b)
			if err != nil {
				return nil, err
			}
			if b == 0 {
				return nil, ErrDivideByZero
			}
			return int64Val(int64(a) % b), nil
		}
		switch b.Kind() {
		case KindInt:
			a1 := big.NewInt(int64(a))
			b, err := AsInt(b)
			if err != nil {
				return nil, err
			}
			return intVal{a1.Mod(a1, b)}, nil
		case KindFloat:
			b, err := AsFloat(b)
			if err != nil {
				return nil, err
			}
			return floatVal(math.Mod(float64(a), b)), nil
		}
	case floatVal:
		b, err := AsFloat(b)
		if err != nil {
			return nil, err
		}
		if b == 0 {
			return nil, ErrDivideByZero
		}
		return floatVal(math.Mod(float64(a), b)), nil
	}
	return nil, MakeBinaryOpError("mod", a, b, nil)
}

type UnaryOpError struct {
	Op    string
	Value Value
	Cause error
}

func (e *UnaryOpError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("cannot perform %s on %s(%s): %s", e.Op, e.Value.Kind(), e.Value.String(), e.Cause)
	}
	return fmt.Sprintf("cannot perform %s on %s(%s)", e.Op, e.Value.Kind(), e.Value.String())
}

func MakeUnaryOpError(op string, val Value, cause error) error {
	return &UnaryOpError{op, val, cause}
}

func Neg(a Value) (_ Value, retErr error) {
	if a == Null {
		return Null, nil
	}

	defer func() {
		if retErr != nil {
			if _, ok := retErr.(*UnaryOpError); !ok {
				retErr = MakeUnaryOpError("neg", a, retErr)
			}
		}
	}()

	switch a := a.(type) {
	case intVal:
		return intVal{new(big.Int).Neg(a.val)}, nil
	case int64Val:
		if a == math.MinInt64 {
			return intVal{new(big.Int).SetUint64(uint64(math.MaxInt64 + 1))}, nil
		}
		return -a, nil
	case floatVal:
		return -a, nil
	case intervalVal:
		return intervalVal{-a.val}, nil
	}
	return nil, MakeUnaryOpError("neg", a, nil)
}
