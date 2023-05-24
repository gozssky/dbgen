package dbgen

import (
	"fmt"
	"math/rand"
	"regexp"
	"time"

	"github.com/gozssky/dbgen/constant"
	"github.com/gozssky/dbgen/template"
	"github.com/samber/lo"
)

// State is the mutable state used during evaluation.
type State struct {
	RowNum     int64
	SubRowNum  int64
	Rng        rand.Source64
	CompileCtx *CompileContext
}

type Template struct {
	GlobalExprs Row
	Tables      []*Table
}

type Table struct {
	Name    *template.QName
	Content string
	Columns []template.Name
	Row     Row
	Derived []lo.Tuple2[int, Compiled]
}

// CompileContext is the environment information shared by all compilations.
type CompileContext struct {
	// The time zone used to interpret strings into timestamps.
	TimeZone *time.Location
	// The current timestamp in UTC.
	CurrentTimestamp time.Time
	// LoadLocation is the function used to load the Location with the given name.
	LoadLocation func(name string) (*time.Location, error)
	// The global variables.
	Variables []lo.Tuple2[string, constant.Value]
	tzCache   map[string]*time.Location
}

func NewCompileContext() *CompileContext {
	return &CompileContext{
		TimeZone:         time.UTC,
		CurrentTimestamp: time.Now().UTC(),
		LoadLocation:     time.LoadLocation,
		tzCache:          make(map[string]*time.Location),
	}
}

func (ctx *CompileContext) ParseTimeZone(tz string) (*time.Location, error) {
	if loc, ok := ctx.tzCache[tz]; ok {
		return loc, nil
	}
	loc, err := ctx.LoadLocation(tz)
	if err != nil {
		return nil, err
	}
	ctx.tzCache[tz] = loc
	return loc, nil
}

func (ctx *CompileContext) CompileTemplate(t *template.Template) (*Template, error) {
	row, err := ctx.CompileRow(t.GlobalExprs)
	if err != nil {
		return nil, err
	}
	tables := make([]*Table, 0, len(t.Tables))
	for _, t := range t.Tables {
		table, err := ctx.CompileTable(t)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return &Template{
		GlobalExprs: row,
		Tables:      tables,
	}, nil
}

func (ctx *CompileContext) CompileTable(t *template.Table) (*Table, error) {
	exprs := lo.Map(t.Columns, func(col *template.Column, _ int) template.Expr {
		return col.Expr
	})
	row, err := ctx.CompileRow(exprs)
	if err != nil {
		return nil, err
	}
	derived := make([]lo.Tuple2[int, Compiled], 0, len(t.Derived))
	for _, d := range t.Derived {
		index, count := d.Unpack()
		compiled, err := ctx.CompileExpr(count)
		if err != nil {
			return nil, err
		}
		derived = append(derived, lo.T2(index, compiled))
	}
	return &Table{
		Name:    t.Name,
		Content: t.Content,
		Columns: lo.Map(t.Columns, func(col *template.Column, _ int) template.Name {
			return col.Name
		}),
		Row:     row,
		Derived: derived,
	}, nil
}

func (ctx *CompileContext) CompileRow(exprs []template.Expr) (Row, error) {
	row := make(Row, 0, len(exprs))
	for _, expr := range exprs {
		compiled, err := ctx.CompileExpr(expr)
		if err != nil {
			return nil, err
		}
		row = append(row, compiled)
	}
	return row, nil
}

func (ctx *CompileContext) CompileExpr(expr template.Expr) (Compiled, error) {
	switch expr := expr.(type) {
	case *template.RowNum:
		return &RowNum{}, nil
	case *template.SubRowNum:
		return &SubRowNum{}, nil
	case *template.CurrentTimestamp:
		return &Constant{constant.MakeTimestamp(ctx.CurrentTimestamp)}, nil
	case *template.Constant:
		return &Constant{expr.Value}, nil
	case *template.GetVariable:
		_, index, _ := lo.FindIndexOf(ctx.Variables, func(v lo.Tuple2[string, constant.Value]) bool {
			return v.A == expr.Name
		})
		if index == -1 {
			index = len(ctx.Variables)
			ctx.Variables = append(ctx.Variables, lo.T2(expr.Name, constant.Null))
		}
		return &GetVariable{Index: index}, nil
	case *template.SetVariable:
		_, index, _ := lo.FindIndexOf(ctx.Variables, func(v lo.Tuple2[string, constant.Value]) bool {
			return v.A == expr.Name
		})
		if index == -1 {
			index = len(ctx.Variables)
			ctx.Variables = append(ctx.Variables, lo.T2(expr.Name, constant.Null))
		}
		return &SetVariable{Index: index}, nil
	case *template.UnaryExpr:
		fn, ok := UnaryFuncs[expr.Op]
		if !ok {
			return nil, fmt.Errorf("unknown unary operator: %s", expr.Op)
		}
		return ctx.compileRawFunction(fn, expr.Expr)
	case *template.BinaryExpr:
		fn, ok := BinaryFuncs[expr.Op]
		if !ok {
			return nil, fmt.Errorf("unknown binary operator: %s", expr.Op)
		}
		return ctx.compileRawFunction(fn, expr.Left, expr.Right)
	case *template.ParenExpr:
		return ctx.CompileExpr(expr.Expr)
	case *template.FuncExpr:
		fn, ok := GenericFuncs[expr.Name.UniqueName()]
		if !ok {
			return nil, fmt.Errorf("unknown function: %s", expr.Name)
		}
		if fn.NumArgs() >= 0 && len(expr.Args) != fn.NumArgs() {
			return nil, fmt.Errorf("wrong number of arguments for function %s: expected %d, got %d", expr.Name, fn.NumArgs(), len(expr.Args))
		}
		return ctx.compileRawFunction(fn, expr.Args...)
	case *template.CaseValueWhen:
		return ctx.compileCaseValueWhen(expr)
	case *template.Timestamp:
		var fn Function
		if expr.WithTimezone {
			fn = &TimestampWithTimeZoneFunc{}
		} else {
			fn = &TimestampFunc{}
		}
		return ctx.compileRawFunction(fn, expr.Value)
	case *template.Interval:
		fn := BinaryFuncs[template.OpMul]
		unit := constant.MakeInterval(time.Duration(expr.Unit))
		return ctx.compileRawFunction(fn, expr.Value, &template.Constant{Value: unit})
	case *template.Array:
		fn := &ArrayFunc{}
		return ctx.compileRawFunction(fn, expr.Elems...)
	case *template.Subscript:
		fn := &SubscriptFunc{}
		return ctx.compileRawFunction(fn, expr.Base, expr.Index)
	case *template.Substring:
		fn := &SubstringFunc{Unit: expr.Unit}
		return ctx.compileRawFunction(fn, expr.Input, expr.From, expr.For)
	case *template.Overlay:
		fn := &OverlayFunc{Unit: expr.Unit}
		return ctx.compileRawFunction(fn, expr.Input, expr.Placing, expr.From, expr.For)
	default:
		return nil, fmt.Errorf("unknown expression: %T", expr)
	}
}

func (ctx *CompileContext) compileRawFunction(fn Function, args ...template.Expr) (Compiled, error) {
	isConst := true
	compiledArgs := make([]Compiled, 0, len(args))
	for _, arg := range args {
		if arg == nil {
			compiledArgs = append(compiledArgs, &Constant{constant.Null})
			continue
		}
		compiled, err := ctx.CompileExpr(arg)
		if err != nil {
			return nil, err
		}
		compiledArgs = append(compiledArgs, compiled)
		isConst = isConst && IsConstant(compiled)
	}
	if isConst {
		constArgs := make([]constant.Value, 0, len(args))
		for _, arg := range compiledArgs {
			constArgs = append(constArgs, arg.(*Constant).Value)
		}
		compiled, err := fn.Compile(ctx, constArgs)
		if err != nil {
			return nil, err
		}
		return compiled, nil
	}
	return &RawFunction{Fn: fn, Args: compiledArgs}, nil
}

func (ctx *CompileContext) compileCaseValueWhen(expr *template.CaseValueWhen) (Compiled, error) {
	var (
		value Compiled
		else_ Compiled
		err   error
	)
	if expr.Value != nil {
		value, err = ctx.CompileExpr(expr.Value)
		if err != nil {
			return nil, err
		}
	} else {
		value = &Constant{constant.Null}
	}
	whens := make([]*When, 0, len(expr.Whens))
	for _, when := range expr.Whens {
		cond, err := ctx.CompileExpr(when.Cond)
		if err != nil {
			return nil, err
		}
		then, err := ctx.CompileExpr(when.Then)
		if err != nil {
			return nil, err
		}
		whens = append(whens, &When{Cond: cond, Then: then})
	}
	if expr.Else != nil {
		else_, err = ctx.CompileExpr(expr.Else)
		if err != nil {
			return nil, err
		}
	} else {
		else_ = &Constant{constant.Null}
	}

	compiled := &CaseValueWhen{Value: value, Whens: whens, Else: else_}
	isConstWhen := func(w *When) bool {
		return IsConstant(w.Cond) && IsConstant(w.Then)
	}
	if IsConstant(value) && lo.EveryBy(whens, isConstWhen) && IsConstant(else_) {
		value, err := compiled.Eval(nil)
		if err != nil {
			return nil, err
		}
		return &Constant{Value: value}, nil
	}
	return compiled, nil
}

// Row represents a row of compiled values.
type Row []Compiled

func (r Row) Eval(state *State) ([]constant.Value, error) {
	result := make([]constant.Value, 0, len(r))
	for _, compiled := range r {
		value, err := compiled.Eval(state)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

// Compiled is a compiled expression.
type Compiled interface {
	// Eval evaluates a compiled expression and updates the state. Returns the evaluated value.
	Eval(state *State) (constant.Value, error)
}

// Compiled expression types.
type (
	// RowNum is the row number.
	RowNum struct{}
	// SubRowNum is the derived row number.
	SubRowNum struct{}
	// Constant is a evaluated constant.
	Constant struct{ Value constant.Value }
	// RawFunction is a function that has not been compiled.
	RawFunction struct {
		Fn   Function
		Args []Compiled
	}
	// GetVariable is a local variable reference.
	GetVariable struct{ Index int }
	// SetVariable assigns a value to a local variable.
	SetVariable struct {
		Index int
		Value Compiled
	}
	// CaseValueWhen is a `CASE _ WHEN` expression.
	CaseValueWhen struct {
		Value Compiled
		Whens []*When
		Else  Compiled
	}
	// RandRegex is a regex-based random string.
	RandRegex struct {
		Regex *regexp.Regexp
	}
	RandUniformU64   struct{}
	RandUniformI64   struct{}
	RandUniformF64   struct{}
	RandZipf         struct{}
	RandLogNormal    struct{}
	RandBool         struct{}
	RandFinite32     struct{}
	RandFinite64     struct{}
	RandU31Timestamp struct{}
	RandShuffle      struct{}
	RandUuid         struct{}
)

// IsConstant returns true if the compiled expression is a constant.
func IsConstant(compiled Compiled) bool {
	_, ok := compiled.(*Constant)
	return ok
}

type When struct {
	Cond Compiled
	Then Compiled
}

func (*RowNum) Eval(state *State) (constant.Value, error) {
	return constant.MakeInt64(state.RowNum), nil
}

func (*SubRowNum) Eval(state *State) (constant.Value, error) {
	return constant.MakeInt64(state.SubRowNum), nil
}

func (c *Constant) Eval(_ *State) (constant.Value, error) {
	return c.Value, nil
}

func (r *RawFunction) Eval(state *State) (constant.Value, error) {
	args := make([]constant.Value, 0, len(r.Args))
	for _, arg := range r.Args {
		value, err := arg.Eval(state)
		if err != nil {
			return nil, err
		}
		args = append(args, value)
	}
	c, err := r.Fn.Compile(state.CompileCtx, args)
	if err != nil {
		return nil, err
	}
	return c.Eval(state)
}

func (g *GetVariable) Eval(state *State) (constant.Value, error) {
	return state.CompileCtx.Variables[g.Index].B, nil
}

func (s *SetVariable) Eval(state *State) (constant.Value, error) {
	value, err := s.Value.Eval(state)
	if err != nil {
		return nil, err
	}
	state.CompileCtx.Variables[s.Index].B = value
	return value, nil
}

func (c *CaseValueWhen) Eval(_ *State) (constant.Value, error) {
	panic("unimplemented")
}

func (r *RandRegex) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}

func (*RandUniformU64) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}

func (*RandUniformI64) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}

func (*RandUniformF64) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}

func (*RandZipf) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}

func (*RandLogNormal) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}

func (*RandBool) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}

func (*RandFinite32) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}

func (*RandFinite64) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}

func (*RandU31Timestamp) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}

func (*RandShuffle) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}

func (*RandUuid) Eval(state *State) (constant.Value, error) {
	panic("unimplemented")
}
