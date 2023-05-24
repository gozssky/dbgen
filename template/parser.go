package template

import (
	"fmt"
	"strings"

	"github.com/gozssky/dbgen/constant"
	"github.com/samber/lo"
)

// Parser is a parser for templates.
type Parser struct {
	lex   *lexer
	input string
	tok   token // one token look-ahead
	tok1  token // two token look-ahead
}

func (p *Parser) init(input string) {
	lex := newLexer(input)
	p.lex = lex
	p.input = input
	p.next()
	p.next()
}

func (p *Parser) Parse(input string) (*Template, error) {
	p.init(input)
	return p.parseTemplate()
}

func (p *Parser) ParseExpr(input string) (Expr, error) {
	p.init(input)
	return p.parseExpr()
}

func (p *Parser) next() {
	p.tok = p.tok1
	p.tok1 = p.lex.lex()
	for p.tok1.typ == tokenComment {
		p.tok1 = p.lex.lex()
	}
}

func (p *Parser) expect(tokTyp tokenType) error {
	if p.tok.typ != tokTyp {
		return p.errorExpected(tokTyp.String())
	}
	p.next()
	return nil
}

func (p *Parser) errorExpected(msg string) error {
	return p.errorf("expected %s, found %s", msg, p.tok)
}

func (p *Parser) errorUnexpected() error {
	return p.errorf("unexpected %s", p.tok)
}

type SyntaxError struct {
	Line   int
	Column int
	Near   string
	Cause  string
}

func (e *SyntaxError) Error() string {
	if len(e.Near) > 0 {
		return fmt.Sprintf("syntax error on line %d at column %d near %q: %s", e.Line, e.Column, e.Near, e.Cause)
	} else {
		return fmt.Sprintf("syntax error on line %d at column %d: %s", e.Line, e.Column, e.Cause)
	}
}

const maxNearContentLength = 10

func (p *Parser) errorf(format string, args ...interface{}) error {
	near := p.input[p.tok.pos:]
	if len(near) > maxNearContentLength {
		near = near[:maxNearContentLength]
	}
	err := &SyntaxError{Line: p.tok.line, Column: p.tok.col, Near: near}
	if p.tok.typ == tokenError {
		err.Cause = p.tok.val // return the underlying lexer error
	} else {
		err.Cause = fmt.Sprintf(format, args...)
	}
	return err
}

func (p *Parser) parseTemplate() (*Template, error) {
	tmpl := &Template{}
	stmts, err := p.parseStmtBlockList()
	if err != nil {
		return nil, err
	}
	tmpl.GlobalExprs = stmts

	table, err := p.parseSingleTable()
	if err != nil {
		return nil, err
	}
	tmpl.Tables = append(tmpl.Tables, table)

	for {
		if p.tok.typ == tokenEOF {
			return tmpl, nil
		}
		if p.tok.typ != tokenLeftDelim {
			return nil, p.errorUnexpected()
		}
		result, err := p.parseDependencyDeriveBlock()
		if err != nil {
			return nil, err
		}
		parentName, childName, count := result.Unpack()

		table, err := p.parseSingleTable()
		if err != nil {
			return nil, err
		}

		if !table.Name.Equal(childName) {
			return nil, p.errorf("derived table name in the FOR EACH ROW and CREATE TABLE statements do not match (%s vs %s)", childName.String(), table.Name.UniqueName())
		}

		index := len(tmpl.Tables)
		parent, ok := lo.Find(tmpl.Tables, func(parent *Table) bool {
			return parent.Name.Equal(parentName)
		})
		if !ok {
			return nil, p.errorf("cannot find parent table %s to generate derived rows", parentName.String())
		}
		parent.Derived = append(parent.Derived, lo.T2(index, count))
		tmpl.Tables = append(tmpl.Tables, table)
	}
}

func (p *Parser) parseStmtBlockList() ([]Expr, error) {
	var list []Expr
	for p.tok.typ == tokenLeftDelim {
		stmt, _, _, err := p.parseStmtBlock()
		if err != nil {
			return nil, err
		}
		list = append(list, stmt)
	}
	return list, nil
}

func (p *Parser) parseStmtBlock() (_ Expr, start, end int, _ error) {
	start = p.tok.pos
	lDelim := p.tok.val
	if err := p.expect(tokenLeftDelim); err != nil {
		return nil, 0, 0, err
	}

	stmt, err := p.parseStmt()
	if err != nil {
		return nil, 0, 0, err
	}

	rDelim := p.tok.val
	if lDelim == leftDelim && rDelim != rightDelim {
		return nil, 0, 0, p.errorExpected(rightDelim)
	} else if lDelim == leftCommentedDelim && rDelim != rightCommentedDelim {
		return nil, 0, 0, p.errorExpected(rightCommentedDelim)
	}
	end = p.tok.pos + len(p.tok.val)
	if err := p.expect(tokenRightDelim); err != nil {
		return nil, 0, 0, err
	}
	return stmt, start, end, nil
}

func (p *Parser) parseSingleTable() (*Table, error) {
	tableStart := p.tok.pos
	if err := p.expect(tokenCreate); err != nil {
		return nil, err
	}
	if err := p.expect(tokenTable); err != nil {
		return nil, err
	}

	qname, err := p.parseQName()
	if err != nil {
		return nil, err
	}
	table := &Table{
		Name: qname,
	}

	// parse table elements
	if err := p.expect(tokenLeftParen); err != nil {
		return nil, err
	}

	var colName string
	var blockSpans []lo.Tuple2[int, int]

elemLoop:
	for {
		switch p.tok.typ {
		case tokenIdent:
			if colName == "" {
				colName = p.tok.val
			}
			p.next()
		case tokenLeftDelim:
			stmt, blockStart, blockEnd, err := p.parseStmtBlock()
			if err != nil {
				return nil, err
			}
			table.Columns = append(table.Columns, &Column{
				Name: NewName(colName),
				Expr: stmt,
			})
			colName = ""
			blockSpans = append(blockSpans, lo.T2(blockStart, blockEnd))
		case tokenRightParen:
			break elemLoop
		case tokenRightBrack, tokenRightBrace, tokenRightDelim:
			return nil, p.errorUnexpected()
		case tokenLeftParen, tokenLeftBrack, tokenLeftBrace:
			if err := p.skipAnyBalancedText(); err != nil {
				return nil, err
			}
		case tokenError, tokenEOF:
			return nil, p.errorUnexpected()
		default:
			if p.tok.isIdent() {
				if colName == "" {
					colName = p.tok.val
				}
			}
			// allow any other token
			p.next()
		}
	}
	if err := p.expect(tokenRightParen); err != nil {
		return nil, err
	}

	// parse table options
	var tableEnd int
optLoop:
	for {
		switch p.tok.typ {
		case tokenLeftParen, tokenLeftBrack, tokenLeftBrace:
			if err := p.skipAnyBalancedText(); err != nil {
				return nil, err
			}
		case tokenSemicolon:
			tableEnd = p.tok.pos + len(p.tok.val)
			p.next()
			break optLoop
		case tokenEOF:
			tableEnd = p.tok.pos
			break optLoop
		case tokenError:
			return nil, p.errorUnexpected()
		default:
			// allow any other token
			p.next()
		}
	}

	table.Content = p.extractTableContent(tableStart, tableEnd, blockSpans)
	return table, nil
}

// extractTableContent extracts table definition from input, removing {{}} and /*{{}}*/ blocks.
func (p *Parser) extractTableContent(start, end int, blockSpans []lo.Tuple2[int, int]) string {
	var buf strings.Builder

	blockSpans = p.mergeBlockSpans(blockSpans)
	for _, span := range blockSpans {
		l, r := span.Unpack()

		isEmptyLine := false
		leftNewline := strings.LastIndexByte(p.input[start:l], '\n')
		rightNewLine := strings.IndexByte(p.input[r:end], '\n')
		if leftNewline != -1 && rightNewLine != -1 {
			leftNewline = start + leftNewline
			rightNewLine = r + rightNewLine
			isEmptyLine = true
			for i := leftNewline + 1; i < l; i++ {
				if !isSpace(int(p.input[i])) {
					isEmptyLine = false
					break
				}
			}
			for i := r; i < rightNewLine && isEmptyLine; i++ {
				if !isSpace(int(p.input[i])) {
					isEmptyLine = false
					break
				}
			}
		}

		replace := byte(' ')
		if isEmptyLine {
			// If line only contains spaces after removing {{}} and /*{{}}*/, remove the whole line.
			l = leftNewline
			r = rightNewLine + 1
			replace = '\n'
		} else {
			// Remove extra spaces around {{}} and /*{{}}*/.
			if l > start && isSpace(int(p.input[l-1])) {
				l--
			}
			if r < end && isSpace(int(p.input[r])) {
				r++
			}
		}

		buf.WriteString(p.input[start:l])
		buf.WriteByte(replace)
		start = r
	}
	buf.WriteString(p.input[start:end])
	return buf.String()
}

// mergeBlockSpans merges adjacent block spans.
// Two spans are adjacent if they are separated by only whitespace.
func (p *Parser) mergeBlockSpans(blockSpans []lo.Tuple2[int, int]) []lo.Tuple2[int, int] {
	if len(blockSpans) == 0 {
		return blockSpans
	}
	merged := make([]lo.Tuple2[int, int], 0, len(blockSpans))
	merged = append(merged, blockSpans[0])
	for _, span := range blockSpans[1:] {
		last := merged[len(merged)-1]
		prevEnd, curStart := last.B, span.A
		adjacent := true
		for i := prevEnd; i < curStart; i++ {
			if !isWhitespace(int(p.input[i])) {
				adjacent = false
				break
			}
		}
		if adjacent {
			last.B = span.B
			merged[len(merged)-1] = last
		} else {
			merged = append(merged, span)
		}
	}
	return merged
}

func (p *Parser) parseDependencyDeriveBlock() (*lo.Tuple3[*QName, *QName, Expr], error) {
	lDelim := p.tok.val
	if err := p.expect(tokenLeftDelim); err != nil {
		return nil, err
	}

	// FOR EACH ROW OF
	for _, tokTyp := range []tokenType{tokenFor, tokenEach, tokenRow, tokenOf} {
		if err := p.expect(tokTyp); err != nil {
			return nil, err
		}
	}

	parent, err := p.parseQName()
	if err != nil {
		return nil, err
	}

	// GENERATE
	if err := p.expect(tokenGenerate); err != nil {
		return nil, err
	}

	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	// ROWS OF
	if p.tok.typ != tokenRow && p.tok.typ != tokenRows {
		return nil, p.errorExpected("ROW or ROWS")
	}
	p.next()
	if err := p.expect(tokenOf); err != nil {
		return nil, err
	}

	child, err := p.parseQName()
	if err != nil {
		return nil, err
	}

	rDelim := p.tok.val
	if lDelim == leftDelim && rDelim != rightDelim {
		return nil, p.errorExpected(rightDelim)
	} else if lDelim == leftCommentedDelim && rDelim != rightCommentedDelim {
		return nil, p.errorExpected(rightCommentedDelim)
	}
	if err := p.expect(tokenRightDelim); err != nil {
		return nil, err
	}

	result := lo.T3(parent, child, expr)
	return &result, nil
}

func (p *Parser) parseQName() (*QName, error) {
	names := []string{p.tok.val}
	if !p.tok.isIdent() {
		return nil, p.errorExpected("identifier")
	}
	p.next()
	for i := 0; i < 2; i++ {
		if p.tok.typ != tokenPeriod {
			break
		}
		p.next()
		if !p.tok.isIdent() {
			return nil, p.errorExpected("identifier")
		}
		names = append(names, p.tok.val)
		p.next()
	}
	return NewQName(names...), nil
}

// skipAnyBalancedText skips over balanced text. Balanced text is text
// that is enclosed by () or {} or []. Nested balanced text is allowed.
func (p *Parser) skipAnyBalancedText() error {
	switch p.tok.typ {
	case tokenLeftParen:
		return p.skipBalancedText(tokenRightParen)
	case tokenLeftBrack:
		return p.skipBalancedText(tokenRightBrack)
	case tokenLeftBrace:
		return p.skipBalancedText(tokenRightBrace)
	default:
		return nil
	}
}

func (p *Parser) skipBalancedText(closeTok tokenType) error {
	p.next()
	for {
		switch p.tok.typ {
		case tokenEOF:
			return p.errorf("unbalanced text")
		case tokenRightParen, tokenRightBrack, tokenRightBrace:
			if p.tok.typ != closeTok {
				return p.errorf("unbalanced text")
			}
			p.next()
			return nil
		case tokenLeftParen:
			if err := p.skipBalancedText(tokenRightParen); err != nil {
				return err
			}
		case tokenLeftBrack:
			if err := p.skipBalancedText(tokenRightBrack); err != nil {
				return err
			}
		case tokenLeftBrace:
			if err := p.skipBalancedText(tokenRightBrace); err != nil {
				return err
			}
		case tokenError:
			return p.errorUnexpected()
		default:
			// Skip any other token.
			p.next()
		}
	}
}

func (p *Parser) parseStmt() (Expr, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	for {
		if p.tok.typ != tokenSemicolon {
			return expr, nil
		}
		p.next()
		nextExpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpr{
			Op:    OpSemicolon,
			Left:  expr,
			Right: nextExpr,
		}
	}
}

func (p *Parser) parseExpr() (Expr, error) {
	return p.parseBinaryExpr(OpSemicolon.Prec())
}

func (p *Parser) parseBinaryExpr(prec int) (Expr, error) {
	left, err := p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}
	op, next := p.parseOp()
	for op.IsBinary() && (op.Prec() > prec || op.Prec() == prec && op.IsRightAssoc()) {
		next()
		right, err := p.parseBinaryExpr(op.Prec())
		if err != nil {
			return nil, err
		}
		if getV, ok := left.(*GetVariable); ok && op == OpAssign {
			left = &SetVariable{Name: getV.Name, Value: right}
		} else {
			left = &BinaryExpr{Op: op, Left: left, Right: right}
		}
		op, next = p.parseOp()
	}
	return left, nil
}

func (p *Parser) parseOp() (_ Op, next func()) {
	op := p.tok.Op()
	if op == OpIs && p.tok1.Op() == OpNot {
		return OpIsNot, func() {
			p.next()
			p.next()
		}
	}
	return op, p.next
}

func (p *Parser) parseUnaryExpr() (Expr, error) {
	op := p.tok.Op()
	switch op {
	case OpNot:
		p.next()
		expr, err := p.parseBinaryExpr(op.Prec())
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: op, Expr: expr}, nil
	case OpAdd, OpSub, OpBitNot:
		p.next()
		expr, err := p.parsePrimaryExpr()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: op, Expr: expr}, nil
	}
	expr, err := p.parsePrimaryExpr()
	if err != nil {
		return nil, err
	}

	if p.tok.typ != tokenLeftBrack {
		return expr, nil
	}

	p.next()
	index, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenRightBrack); err != nil {
		return nil, err
	}
	return &Subscript{Base: expr, Index: index}, nil
}

func (p *Parser) parsePrimaryExpr() (Expr, error) {
	switch p.tok.typ {
	case tokenRowNum:
		p.next()
		return &RowNum{}, nil
	case tokenSubRowNum:
		p.next()
		return &SubRowNum{}, nil
	case tokenNull:
		p.next()
		return &Constant{constant.Null}, nil
	case tokenTrue:
		p.next()
		return &Constant{constant.MakeBool(true)}, nil
	case tokenFalse:
		p.next()
		return &Constant{constant.MakeBool(false)}, nil
	case tokenCurrentTimestamp:
		p.next()
		return &CurrentTimestamp{}, nil
	case tokenLeftParen:
		p.next()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenRightParen); err != nil {
			return nil, err
		}
		return &ParenExpr{expr}, nil
	case tokenString:
		expr := &Constant{constant.MakeBytes([]byte(unescape(p.tok.val)))}
		p.next()
		return expr, nil
	case tokenNumber:
		val, err := constant.MakeNumberFromLiteral(p.tok.val)
		if err != nil {
			return nil, err
		}
		expr := &Constant{val}
		p.next()
		return expr, nil
	case tokenCase:
		expr, err := p.parseCaseValueWhen()
		if err != nil {
			return nil, err
		}
		return expr, nil
	case tokenTimestamp:
		p.next()
		timestamp := &Timestamp{}
		if p.tok.typ == tokenWith {
			p.next()
			if err := p.expect(tokenTime); err != nil {
				return nil, err
			}
			if err := p.expect(tokenZone); err != nil {
				return nil, err
			}
			timestamp.WithTimezone = true
		}
		expr, err := p.parsePrimaryExpr()
		if err != nil {
			return nil, err
		}
		timestamp.Value = expr
		return timestamp, nil
	case tokenInterval:
		p.next()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		interval := &Interval{Value: expr}
		switch p.tok.typ {
		case tokenWeek:
			interval.Unit = IntervalUnitWeek
		case tokenDay:
			interval.Unit = IntervalUnitDay
		case tokenHour:
			interval.Unit = IntervalUnitHour
		case tokenMinute:
			interval.Unit = IntervalUnitMinute
		case tokenSecond:
			interval.Unit = IntervalUnitSecond
		case tokenMillisecond:
			interval.Unit = IntervalUnitMillisecond
		case tokenMicrosecond:
			interval.Unit = IntervalUnitMicrosecond
		default:
			return nil, p.errorExpected("interval unit")
		}
		return interval, nil
	case tokenX:
		p.next()
		expr, err := p.parsePrimaryExpr()
		if err != nil {
			return nil, err
		}
		return &FuncExpr{
			NewQName("hex", "decode"),
			[]Expr{expr},
		}, nil
	case tokenAt:
		p.next()
		if !p.tok.isIdent() {
			return nil, p.errorExpected("identifier")
		}
		name := unescape(p.tok.val)
		p.next()
		return &GetVariable{Name: unescape(name)}, nil
	case tokenArray:
		p.next()
		if err := p.expect(tokenLeftBrack); err != nil {
			return nil, err
		}
		array := &Array{}
		for {
			if p.tok.typ == tokenRightBrack {
				p.next()
				return array, nil
			}
			if len(array.Elems) > 0 {
				if err := p.expect(tokenComma); err != nil {
					return nil, err
				}
			}
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			array.Elems = append(array.Elems, expr)
		}
	case tokenSubstring:
		return p.parseSubstring()
	case tokenOverlay:
		return p.parseOverlay()
	default:
		name, err := p.parseQName()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenLeftParen); err != nil {
			return nil, err
		}
		funcExpr := &FuncExpr{
			Name: name,
		}
		for {
			if p.tok.typ == tokenRightParen {
				p.next()
				return funcExpr, nil
			}
			if len(funcExpr.Args) > 0 {
				if err := p.expect(tokenComma); err != nil {
					return nil, err
				}
			}
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			funcExpr.Args = append(funcExpr.Args, expr)
		}
	}
}

func (p *Parser) parseCaseValueWhen() (Expr, error) {
	if err := p.expect(tokenCase); err != nil {
		return nil, err
	}
	var sawValueOrWhen bool
	expr := &CaseValueWhen{}
	for {
		switch p.tok.typ {
		case tokenWhen:
			sawValueOrWhen = true
			p.next()
			cond, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if err := p.expect(tokenThen); err != nil {
				return nil, err
			}
			then, err := p.parseStmt()
			if err != nil {
				return nil, err
			}
			expr.Whens = append(expr.Whens, &When{cond, then})

		case tokenElse:
			p.next()
			else_, err := p.parseStmt()
			if err != nil {
				return nil, err
			}
			expr.Else = else_
			if err := p.expect(tokenEnd); err != nil {
				return nil, err
			}
			return expr, nil
		case tokenEnd:
			p.next()
			return expr, nil
		default:
			if sawValueOrWhen {
				return nil, p.errorExpected("WHEN, ELSE or END")
			}
			value, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			expr.Value = value
			sawValueOrWhen = true
		}
	}
}

func (p *Parser) parseSubstring() (Expr, error) {
	if err := p.expect(tokenSubstring); err != nil {
		return nil, err
	}
	substring := &Substring{}
	if err := p.expect(tokenLeftParen); err != nil {
		return nil, err
	}
	input, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	substring.Input = input

	if p.tok.typ == tokenFrom {
		p.next()
		from, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		substring.From = from
	}

	if p.tok.typ == tokenFor {
		p.next()
		for_, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		substring.For = for_
	}

	if p.tok.typ == tokenUsing {
		p.next()
		switch p.tok.typ {
		case tokenOctets:
			substring.Unit = StringUnitOctets
		case tokenCharacters:
			substring.Unit = StringUnitCharacters
		default:
			return nil, p.errorExpected("OCTETS or CHARACTERS")
		}
		p.next()
	}

	if err := p.expect(tokenRightParen); err != nil {
		return nil, err
	}
	return substring, nil
}

func (p *Parser) parseOverlay() (Expr, error) {
	if err := p.expect(tokenOverlay); err != nil {
		return nil, err
	}
	overlay := &Overlay{}
	if err := p.expect(tokenLeftParen); err != nil {
		return nil, err
	}
	input, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	overlay.Input = input

	if err := p.expect(tokenPlacing); err != nil {
		return nil, err
	}
	placing, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	overlay.Placing = placing

	if err := p.expect(tokenFrom); err != nil {
		return nil, err
	}
	from, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	overlay.From = from

	if p.tok.typ == tokenFor {
		p.next()
		for_, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		overlay.For = for_
	}

	if p.tok.typ == tokenUsing {
		p.next()
		switch p.tok.typ {
		case tokenOctets:
			overlay.Unit = StringUnitOctets
		case tokenCharacters:
			overlay.Unit = StringUnitCharacters
		default:
			return nil, p.errorExpected("OCTETS or CHARACTERS")
		}
		p.next()
	}
	if err := p.expect(tokenRightParen); err != nil {
		return nil, err
	}
	return overlay, nil
}

func Parse(input string) (*Template, error) {
	var p Parser
	return p.Parse(input)
}

func ParseExpr(input string) (Expr, error) {
	var p Parser
	return p.ParseExpr(input)
}
