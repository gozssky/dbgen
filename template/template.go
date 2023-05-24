package template

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gozssky/dbgen/constant"
	"github.com/samber/lo"
)

// Template represents a template.
type Template struct {
	GlobalExprs []Expr
	Tables      []*Table
}

// Table represents a table in a template.
type Table struct {
	Name *QName
	// Content is the content of whole create table statement
	// with the {{}} and /*{{}}*/ blocks removed.
	Content string
	// Columns is the list of columns in the table.
	Columns []*Column
	// Derived is the indices of derived tables and the number of rows to generate.
	Derived []lo.Tuple2[int, Expr]
}

// Column represents a column definition in a table.
type Column struct {
	// Name is the name of the column. It is the first identifier before the
	// {{}} and /*{{}}*/ block. It can be empty if no identifier is found,
	// which means an anonymous column.
	//
	// e.g. `CREATE TABLE t ( /*{{ rownum }}*/ )` has an anonymous column.
	Name Name
	// Expr is the expression in the stmt block.
	Expr Expr
}

// QName is schema-qualified name with quotation marks still intact.
// e.g. "db"."schema"."table".
type QName struct {
	// Parts represents the name parts of a schema-qualified name.
	Parts []Name
}

// NewQName creates a new QName from the given name parts.
func NewQName(names ...string) *QName {
	parts := make([]Name, len(names))
	for i, n := range names {
		parts[i] = NewName(n)
	}
	return &QName{Parts: parts}
}

func (q *QName) String() string {
	var names []string
	for _, p := range q.Parts {
		names = append(names, p.String())
	}
	return strings.Join(names, ".")
}

func (q *QName) Name(qualified bool) string {
	if qualified {
		return q.String()
	}
	return q.Parts[len(q.Parts)-1].String()
}

func (q *QName) Equal(other *QName) bool {
	return q.UniqueName() == other.UniqueName()
}

func (q *QName) UniqueName() string {
	var names []string
	for _, p := range q.Parts {
		name := strings.ReplaceAll(p.N, ".", "%2E")
		names = append(names, name)
	}
	return strings.Join(names, ".")
}

func (q *QName) SchemaName() string {
	if len(q.Parts) <= 1 {
		return ""
	}
	var names []string
	for _, p := range q.Parts[:len(q.Parts)-1] {
		names = append(names, p.String())
	}
	return strings.Join(names, ".")
}

// Name represents a name parsed from a template.
type Name struct {
	// O is the original name.
	O string
	// N is the normalized name. It is lower case, without quotation marks.
	N string
}

func NewName(name string) Name {
	return Name{
		O: name,
		N: strings.ToLower(unescape(name)),
	}
}

func (n Name) String() string {
	return n.O
}

// Expr represents an expression.
type Expr interface {
	fmt.Stringer
	isExpr()
}

func (*RowNum) isExpr()           {}
func (*SubRowNum) isExpr()        {}
func (*CurrentTimestamp) isExpr() {}
func (*Constant) isExpr()         {}
func (*GetVariable) isExpr()      {}
func (*SetVariable) isExpr()      {}
func (*UnaryExpr) isExpr()        {}
func (*BinaryExpr) isExpr()       {}
func (*ParenExpr) isExpr()        {}
func (*FuncExpr) isExpr()         {}
func (*CaseValueWhen) isExpr()    {}
func (*Timestamp) isExpr()        {}
func (*Interval) isExpr()         {}
func (*Array) isExpr()            {}
func (*Subscript) isExpr()        {}
func (*Substring) isExpr()        {}
func (*Overlay) isExpr()          {}

type RowNum struct{}

func (*RowNum) String() string {
	return "rownum"
}

type SubRowNum struct{}

func (*SubRowNum) String() string {
	return "subrownum"
}

type CurrentTimestamp struct{}

func (*CurrentTimestamp) String() string {
	return "current_timestamp"
}

// Constant represents a constant value, numeric or string.
type Constant struct {
	constant.Value
}

func (c *Constant) String() string {
	switch c.Value.Kind() {
	case constant.KindBytes:
		b, _ := constant.AsBytes(c.Value)
		if utf8.Valid(b) {
			return singleQuote(string(b))
		} else {
			return fmt.Sprintf("X'%x'", b)
		}
	case constant.KindInterval:
		d, _ := constant.AsInterval(c.Value)
		return fmt.Sprintf("INTERVAL %d MICROSECOND", d/time.Microsecond)
	default:
		return c.Value.String()
	}
}

type GetVariable struct {
	Name string
}

func (gv *GetVariable) String() string {
	return fmt.Sprintf("@%s", backQuote(gv.Name))
}

type SetVariable struct {
	Name  string
	Value Expr
}

func (sv *SetVariable) String() string {
	return fmt.Sprintf("@%s := %s", backQuote(sv.Name), sv.Value)
}

func singleQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func backQuote(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func unescape(s string) string {
	if len(s) < 2 {
		return s
	}
	switch s[0] {
	case '"', '`', '\'':
		quote := s[0]
		s = strings.ReplaceAll(s[1:len(s)-1], string(quote)+string(quote), string(quote))
	case '[':
		s = s[1 : len(s)-1]
	}
	return s
}

type Op int

const (
	OpInvalid Op = iota
	OpAssign
	OpLT
	OpLE
	OpGT
	OpGE
	OpEQ
	OpNE
	OpConcat
	OpAdd
	OpSub
	OpMul
	OpFloatDiv
	OpBitAnd
	OpBitOr
	OpBitXor
	OpBitNot
	OpSemicolon
	OpOr
	OpAnd
	OpNot
	OpIs
	OpIsNot
)

func (op Op) String() string {
	switch op {
	case OpAssign:
		return ":="
	case OpLT:
		return "<"
	case OpLE:
		return "<="
	case OpGT:
		return ">"
	case OpGE:
		return ">="
	case OpEQ:
		return "="
	case OpNE:
		return "<>"
	case OpConcat:
		return "||"
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpFloatDiv:
		return "/"
	case OpBitAnd:
		return "&"
	case OpBitOr:
		return "|"
	case OpBitXor:
		return "^"
	case OpBitNot:
		return "~"
	case OpSemicolon:
		return ";"
	case OpOr:
		return "OR"
	case OpAnd:
		return "AND"
	case OpNot:
		return "NOT"
	case OpIs:
		return "IS"
	case OpIsNot:
		return "IS NOT"
	default:
		return fmt.Sprintf("invalid<%d>", op)
	}
}

func (op Op) Prec() int {
	switch op {
	case OpSemicolon:
		return 1
	case OpAssign:
		return 2
	case OpOr:
		return 3
	case OpAnd:
		return 4
	case OpNot:
		return 5
	case OpLT, OpLE, OpGT, OpGE, OpEQ, OpNE, OpIs, OpIsNot:
		return 6
	case OpBitOr, OpBitXor:
		return 7
	case OpBitAnd:
		return 8
	case OpAdd, OpSub, OpConcat:
		return 9
	case OpMul, OpFloatDiv:
		return 10
	case OpBitNot:
		return 11
	default:
		return 0
	}
}

func (op Op) IsRightAssoc() bool {
	switch op {
	case OpAssign:
		return true
	default:
		return false
	}
}

func (op Op) IsBinary() bool {
	switch op {
	case OpAssign, OpLT, OpLE, OpGT, OpGE, OpEQ, OpNE, OpConcat, OpAdd, OpSub, OpMul, OpFloatDiv, OpBitAnd, OpBitOr, OpBitXor, OpSemicolon, OpOr, OpAnd, OpIs, OpIsNot:
		return true
	default:
		return false
	}
}

type UnaryExpr struct {
	Op   Op
	Expr Expr
}

func (expr *UnaryExpr) String() string {
	return fmt.Sprintf("%s %s", expr.Op, expr.Expr)
}

type BinaryExpr struct {
	Op    Op
	Left  Expr
	Right Expr
}

func (expr *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", expr.Left, expr.Op, expr.Right)
}

type ParenExpr struct {
	Expr Expr
}

func (expr *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", expr.Expr)
}

type FuncExpr struct {
	Name *QName
	Args []Expr
}

func (f *FuncExpr) String() string {
	var sb strings.Builder
	sb.WriteString(f.Name.String())
	sb.WriteByte('(')
	for i, arg := range f.Args {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(arg.String())
	}
	sb.WriteByte(')')
	return sb.String()
}

type CaseValueWhen struct {
	Value Expr
	Whens []*When
	Else  Expr
}

func (c *CaseValueWhen) String() string {
	var sb strings.Builder
	sb.WriteString("CASE")
	if c.Value != nil {
		sb.WriteString(" ")
		sb.WriteString(c.Value.String())
	}
	for _, w := range c.Whens {
		sb.WriteByte(' ')
		sb.WriteString(w.String())
	}
	if c.Else != nil {
		sb.WriteString(" ELSE ")
		sb.WriteString(c.Else.String())
	}
	sb.WriteString(" END")
	return sb.String()
}

type When struct {
	Cond Expr
	Then Expr
}

func (w *When) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", w.Cond, w.Then)
}

type Timestamp struct {
	WithTimezone bool
	Value        Expr
}

func (t *Timestamp) String() string {
	if t.WithTimezone {
		return fmt.Sprintf("TIMESTAMP WITH TIME ZONE %s", t.Value)
	}
	return fmt.Sprintf("TIMESTAMP %s", t.Value)
}

type IntervalUnit time.Duration

const (
	IntervalUnitWeek        = IntervalUnit(7 * 24 * time.Hour)
	IntervalUnitDay         = IntervalUnit(24 * time.Hour)
	IntervalUnitHour        = IntervalUnit(time.Hour)
	IntervalUnitMinute      = IntervalUnit(time.Minute)
	IntervalUnitSecond      = IntervalUnit(time.Second)
	IntervalUnitMillisecond = IntervalUnit(time.Millisecond)
	IntervalUnitMicrosecond = IntervalUnit(time.Microsecond)
)

func (u IntervalUnit) String() string {
	switch u {
	case IntervalUnitWeek:
		return "WEEK"
	case IntervalUnitDay:
		return "DAY"
	case IntervalUnitHour:
		return "HOUR"
	case IntervalUnitMinute:
		return "MINUTE"
	case IntervalUnitSecond:
		return "SECOND"
	case IntervalUnitMillisecond:
		return "MILLISECOND"
	case IntervalUnitMicrosecond:
		return "MICROSECOND"
	default:
		return fmt.Sprintf("unknown<%d>", u)
	}
}

type Interval struct {
	Unit  IntervalUnit
	Value Expr
}

func (i *Interval) String() string {
	return fmt.Sprintf("INTERVAL %s %s", i.Value, i.Unit)
}

type Array struct {
	Elems []Expr
}

func (a *Array) String() string {
	var sb strings.Builder
	sb.WriteString("ARRAY[")
	for i, elem := range a.Elems {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(elem.String())
	}
	sb.WriteByte(']')
	return sb.String()
}

type Subscript struct {
	Base  Expr
	Index Expr
}

func (s *Subscript) String() string {
	return fmt.Sprintf("%s[%s]", s.Base, s.Index)
}

// StringUnit specifies how to index a (byte) string.
type StringUnit int

const (
	// StringUnitCharacters indexes the string using characters (code points).
	StringUnitCharacters StringUnit = iota + 1
	// StringUnitOctets indexes the string using bytes (code unit).
	StringUnitOctets
)

type Substring struct {
	Input Expr
	From  Expr
	For   Expr
	Unit  StringUnit
}

func (s *Substring) String() string {
	var sb strings.Builder
	sb.WriteString("substring(")
	sb.WriteString(s.Input.String())
	if s.From != nil {
		sb.WriteString(" FROM ")
		sb.WriteString(s.From.String())
	}
	if s.For != nil {
		sb.WriteString(" FOR ")
		sb.WriteString(s.For.String())
	}
	switch {
	case s.Unit == StringUnitCharacters:
		sb.WriteString(" USING CHARACTERS")
	case s.Unit == StringUnitOctets:
		sb.WriteString(" USING OCTETS")
	}
	sb.WriteByte(')')
	return sb.String()
}

type Overlay struct {
	Input   Expr
	Placing Expr
	From    Expr
	For     Expr
	Unit    StringUnit
}

func (o *Overlay) String() string {
	var sb strings.Builder
	sb.WriteString("overlay(")
	sb.WriteString(o.Input.String())
	sb.WriteString(" PLACING ")
	sb.WriteString(o.Placing.String())
	sb.WriteString(" FROM ")
	sb.WriteString(o.From.String())
	if o.For != nil {
		sb.WriteString(" FOR ")
		sb.WriteString(o.For.String())
	}
	switch o.Unit {
	case StringUnitCharacters:
		sb.WriteString(" USING CHARACTERS")
	case StringUnitOctets:
		sb.WriteString(" USING OCTETS")
	}
	sb.WriteByte(')')
	return sb.String()
}
