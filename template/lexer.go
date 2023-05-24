package template

import (
	"fmt"
	"strings"
)

type token struct {
	typ  tokenType
	pos  int
	val  string
	line int
	col  int
}

func (t token) String() string {
	if t.typ == tokenEOF {
		return "EOF"
	}
	return fmt.Sprintf("%q", t.val)
}

func (t token) Op() Op {
	switch t.typ {
	case tokenAssign:
		return OpAssign
	case tokenLT:
		return OpLT
	case tokenLE:
		return OpLE
	case tokenEQ:
		return OpEQ
	case tokenNE:
		return OpNE
	case tokenGT:
		return OpGT
	case tokenGE:
		return OpGE
	case tokenConcat:
		return OpConcat
	case tokenAdd:
		return OpAdd
	case tokenSub:
		return OpSub
	case tokenMul:
		return OpMul
	case tokenFloatDiv:
		return OpFloatDiv
	case tokenBitAnd:
		return OpBitAnd
	case tokenBitOr:
		return OpBitOr
	case tokenBitXor:
		return OpBitXor
	case tokenBitNot:
		return OpBitNot
	case tokenSemicolon:
		return OpSemicolon
	case tokenOr:
		return OpOr
	case tokenAnd:
		return OpAnd
	case tokenNot:
		return OpNot
	case tokenIs:
		return OpIs
	default:
		return OpInvalid
	}
}

func (t token) isIdent() bool {
	// All keywords are unreserved, so they are also valid identifiers.
	if _, ok := keywords[strings.ToLower(t.val)]; ok {
		return true
	}
	return t.typ == tokenIdent
}

//go:generate stringer -type=tokenType -trimprefix=token
type tokenType int

const (
	tokenError tokenType = iota
	tokenEOF
	tokenChar
	tokenComment
	tokenIdent      // identifier
	tokenString     // string literal
	tokenNumber     // number literal
	tokenLeftDelim  // {{ or /*{{
	tokenRightDelim // }} or }}*/
	tokenLeftParen  // (
	tokenRightParen // )
	tokenLeftBrack  // [
	tokenRightBrack // ]
	tokenLeftBrace  // {
	tokenRightBrace // }
	tokenComma      // ,
	tokenPeriod     // .
	tokenAt         // @

	// operators
	tokenAssign    // :=
	tokenLT        // <
	tokenLE        // <=
	tokenEQ        // =
	tokenNE        // <>
	tokenGT        // >
	tokenGE        // >=
	tokenConcat    // ||
	tokenAdd       // +
	tokenSub       // -
	tokenMul       // *
	tokenFloatDiv  // /
	tokenBitAnd    // &
	tokenBitOr     // |
	tokenBitXor    // ^
	tokenBitNot    // ~
	tokenSemicolon // ;

	// Keywords
	tokenCreate           // CREATE
	tokenTable            // TABLE
	tokenOr               // OR
	tokenAnd              // AND
	tokenNot              // NOT
	tokenIs               // IS
	tokenRowNum           // ROWNUM
	tokenSubRowNum        // SUBROWNUM
	tokenNull             // NULL
	tokenTrue             // TRUE
	tokenFalse            // FALSE
	tokenCase             // CASE
	tokenWhen             // WHEN
	tokenThen             // THEN
	tokenElse             // ELSE
	tokenEnd              // END
	tokenTimestamp        // TIMESTAMP
	tokenInterval         // INTERVAL
	tokenWeek             // WEEK
	tokenDay              // DAY
	tokenHour             // HOUR
	tokenMinute           // MINUTE
	tokenSecond           // SECOND
	tokenMillisecond      // MILLISECOND
	tokenMicrosecond      // MICROSECOND
	tokenWith             // WITH
	tokenTime             // TIME
	tokenZone             // ZONE
	tokenSubstring        // SUBSTRING
	tokenFrom             // FROM
	tokenFor              // FOR
	tokenUsing            // USING
	tokenCharacters       // CHARACTERS
	tokenOctets           // OCTETS
	tokenOverlay          // OVERLAY
	tokenPlacing          // PLACING
	tokenCurrentTimestamp // CURRENT_TIMESTAMP
	tokenArray            // ARRAY
	tokenEach             // EACH
	tokenRow              // ROW
	tokenOf               // OF
	tokenGenerate         // GENERATE
	tokenRows             // ROWS
	tokenX                // X
)

var keywords = map[string]tokenType{
	"create":            tokenCreate,
	"table":             tokenTable,
	"or":                tokenOr,
	"and":               tokenAnd,
	"not":               tokenNot,
	"is":                tokenIs,
	"rownum":            tokenRowNum,
	"subrownum":         tokenSubRowNum,
	"null":              tokenNull,
	"true":              tokenTrue,
	"false":             tokenFalse,
	"case":              tokenCase,
	"when":              tokenWhen,
	"then":              tokenThen,
	"else":              tokenElse,
	"end":               tokenEnd,
	"timestamp":         tokenTimestamp,
	"interval":          tokenInterval,
	"week":              tokenWeek,
	"day":               tokenDay,
	"hour":              tokenHour,
	"minute":            tokenMinute,
	"second":            tokenSecond,
	"millisecond":       tokenMillisecond,
	"microsecond":       tokenMicrosecond,
	"with":              tokenWith,
	"time":              tokenTime,
	"zone":              tokenZone,
	"substring":         tokenSubstring,
	"from":              tokenFrom,
	"for":               tokenFor,
	"using":             tokenUsing,
	"characters":        tokenCharacters,
	"octets":            tokenOctets,
	"overlay":           tokenOverlay,
	"placing":           tokenPlacing,
	"current_timestamp": tokenCurrentTimestamp,
	"array":             tokenArray,
	"each":              tokenEach,
	"row":               tokenRow,
	"of":                tokenOf,
	"generate":          tokenGenerate,
	"rows":              tokenRows,
	"x":                 tokenX,
}

var specialChars = map[int]tokenType{
	'(': tokenLeftParen,
	')': tokenRightParen,
	'[': tokenLeftBrack,
	']': tokenRightBrack,
	'{': tokenLeftBrace,
	'}': tokenRightBrace,
	';': tokenSemicolon,
	',': tokenComma,
	'<': tokenLT,
	'>': tokenGT,
	'=': tokenEQ,
	'&': tokenBitAnd,
	'|': tokenBitOr,
	'^': tokenBitXor,
	'~': tokenBitNot,
	'+': tokenAdd,
	'-': tokenSub,
	'*': tokenMul,
	'/': tokenFloatDiv,
	'.': tokenPeriod,
	'@': tokenAt,
}

const (
	eof = -1

	rightComment        = "*/"
	leftDelim           = "{{"
	rightDelim          = "}}"
	leftCommentedDelim  = "/*{{"
	rightCommentedDelim = "}}*/"
)

type lexer struct {
	in          string
	pos         int
	line        int
	linePos     int
	start       int
	startLine   int
	startCol    int
	insideBlock bool
}

func newLexer(in string) *lexer {
	return &lexer{
		in:        in,
		pos:       0,
		line:      1,
		linePos:   -1,
		start:     0,
		startLine: 1,
		startCol:  1,
	}
}

func (l *lexer) lex() token {
	l.skipWhitespace()
	l.start = l.pos
	l.startLine = l.line
	l.startCol = l.pos - l.linePos

	var tok token
	switch ch := l.peek(); ch {
	case eof:
		tok = l.emit(tokenEOF)
	case '-':
		if l.peekN(1) == '-' { // --
			tok = l.lexComment()
		} else {
			tok = l.lexChar()
		}
	case '/':
		if l.insideBlock || l.peekN(1) != '*' {
			tok = l.lexChar()
		} else { // /*
			tok = l.lexComment()
		}
	case '`', '"', '\'':
		tok = l.lexQuote()
	case '{':
		if l.peekN(1) == '{' {
			tok = l.lexLeftDelim()
		} else {
			tok = l.lexChar()
		}
	case '}':
		if l.peekN(1) == '}' {
			tok = l.lexRightDelim()
		} else {
			tok = l.lexChar()
		}
	case '|':
		if l.peekN(1) == '|' { // ||
			tok = l.lexTwoChars(tokenConcat)
		} else {
			tok = l.lexChar()
		}
	case '<':
		if l.peekN(1) == '=' { // <=
			tok = l.lexTwoChars(tokenLE)
		} else if l.peekN(1) == '>' { // <>
			tok = l.lexTwoChars(tokenNE)
		} else {
			tok = l.lexChar()
		}
	case '>':
		if l.peekN(1) == '=' { // >=
			tok = l.lexTwoChars(tokenGE)
		} else {
			tok = l.lexChar()
		}
	case '.':
		if isDigit(l.peekN(1)) {
			tok = l.lexNumber()
		} else {
			tok = l.lexChar()
		}
	case ':':
		if l.peekN(1) == '=' {
			tok = l.lexTwoChars(tokenAssign)
		} else {
			tok = l.lexChar()
		}
	default:
		if isDigit(ch) {
			tok = l.lexNumber()
		} else if isIdentStart(ch) {
			tok = l.lexIdent()
		} else {
			tok = l.lexChar()
		}
	}
	return tok
}

func (l *lexer) next() int {
	ch := l.peek()
	if ch == '\n' {
		l.line++
		l.linePos = l.pos
	}
	if l.pos < len(l.in) {
		l.pos++
	}
	return ch
}

func (l *lexer) advance(n int) {
	for i := 0; i < n; i++ {
		l.next()
	}
}

func (l *lexer) peek() int {
	if l.pos >= len(l.in) {
		return eof
	}
	return int(l.in[l.pos])
}

func (l *lexer) peekN(n int) int {
	if l.pos+n >= len(l.in) {
		return eof
	}
	return int(l.in[l.pos+n])
}

func (l *lexer) skipWhitespace() {
	for {
		if !isWhitespace(l.peek()) {
			return
		}
		l.next()
	}
}

func (l *lexer) emit(t tokenType) token {
	tok := token{t, l.start, l.in[l.start:l.pos], l.startLine, l.startCol}
	l.start = l.pos
	l.startLine = l.line
	l.startCol = l.pos - l.linePos
	return tok
}

// errorf returns an error token and terminates the scan.
func (l *lexer) errorf(format string, args ...any) token {
	tok := token{tokenError, l.start, fmt.Sprintf(format, args...), l.startLine, l.startCol}
	// reset the lexer, so it will return EOF next time it is called.
	*l = *newLexer("")
	return tok
}

// lexChar scans a single character.
func (l *lexer) lexChar() token {
	ch := l.next()
	if typ, ok := specialChars[ch]; ok {
		return l.emit(typ)
	}
	return l.emit(tokenChar)
}

// lexTwoChars scans two characters and emits the given type.
func (l *lexer) lexTwoChars(typ tokenType) token {
	l.advance(2)
	return l.emit(typ)
}

// lexComment scans a comment. The left comment marker "--" or "/*" is known is to be present.
func (l *lexer) lexComment() token {
	switch l.peek() {
	case '-':
		l.advance(2)
		for {
			switch l.next() {
			case eof, '\n':
				return l.emit(tokenComment)
			}
		}
	case '/':
		rightCommentIdx := strings.Index(l.in[l.pos:], rightComment)
		if rightCommentIdx == -1 {
			return l.errorf("unclosed comment")
		}

		// Check if this is a commented stmt block like /*{{ ... }}*/.
		// Note that "*/" is not allowed inside the stmt block.
		leftAtDelim := strings.HasPrefix(l.in[l.pos:], leftCommentedDelim)
		rightDelimIdx := strings.Index(l.in[l.pos:], rightCommentedDelim)
		if leftAtDelim && rightDelimIdx != -1 && rightDelimIdx+2 == rightCommentIdx {
			return l.lexLeftDelim()
		}
		commentLen := l.pos + rightCommentIdx + len(rightComment) - l.pos
		l.advance(commentLen)
		return l.emit(tokenComment)
	default:
		panic("unreachable")
	}
}

// lexQuote scans a quoted string. The opening quote is known to be present.
// The opening quote is either a single quote, a double quote, or a back quote.
func (l *lexer) lexQuote() token {
	ch0 := l.next()
	for {
		switch l.next() {
		case eof:
			return l.errorf("unterminated quoted string")
		case ch0:
			if l.peek() == ch0 {
				// double quote is the escape sequence for a single quote
				l.next()
				continue
			}
			switch ch0 {
			case '`', '"':
				return l.emit(tokenIdent)
			case '\'':
				return l.emit(tokenString)
			}
		}
	}
}

// lexIdent scans an unquoted identifier. The first character is known to be valid.
func (l *lexer) lexIdent() token {
	l.next()
	l.accept(isIdentMiddle)
	if typ, ok := keywords[strings.ToLower(l.in[l.start:l.pos])]; ok {
		return l.emit(typ)
	}
	return l.emit(tokenIdent)
}

// lexLeftDelim scans a left delimiter, either "{{" or "/*{{" is known to be present.
func (l *lexer) lexLeftDelim() token {
	if l.peek() == '/' {
		l.pos += len(leftCommentedDelim)
		l.insideBlock = true
	} else {
		l.pos += len(leftDelim)
	}
	return l.emit(tokenLeftDelim)
}

// lexRightDelim scans a right delimiter, either "}}" or "}}*/" is known to be present.
// If currently inside a comment, the right delimiter must be "}}*/", otherwise it is "}}".
func (l *lexer) lexRightDelim() token {
	if l.insideBlock {
		l.insideBlock = false
		l.pos += len(rightCommentedDelim)
	} else {
		l.pos += len(rightDelim)
	}
	return l.emit(tokenRightDelim)
}

// lexNumber scans
func (l *lexer) lexNumber() token {
	ch0 := l.peek()
	ch1 := l.peekN(1)
	if ch0 == '0' && (ch1 == 'x' || ch1 == 'X') && isHexDigit(l.peekN(2)) {
		l.pos += 3
		l.accept(isHexDigit)
		return l.emit(tokenNumber)
	}

	for isDigit(l.peek()) {
		l.pos++
	}
	if l.peek() == '.' {
		l.pos++
		l.accept(isDigit)
	}

	ch0 = l.peek()
	ch1 = l.peekN(1)
	ch2 := l.peekN(2)
	if ch0 == 'e' || ch0 == 'E' {
		if isDigit(ch1) {
			l.pos += 2
		} else if (ch1 == '+' || ch1 == '-') && isDigit(ch2) {
			l.pos += 3
		}
		l.accept(isDigit)
	}

	return l.emit(tokenNumber)
}

func (l *lexer) accept(pred func(ch int) bool) {
	for pred(l.peek()) {
		l.next()
	}
}

func isSpace(ch int) bool {
	return ch == ' ' || ch == '\t' || ch == '\x0b' || ch == '\x0c'
}

func isWhitespace(ch int) bool {
	return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == '\x0b' || ch == '\x0c'
}

func isDigit(ch int) bool {
	return ch >= '0' && ch <= '9'
}

func isHexDigit(ch int) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

func isIdentStart(ch int) bool {
	return ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch == '_' || ch >= 128 && ch <= 255
}

func isIdentMiddle(ch int) bool {
	return isIdentStart(ch) || ch >= '0' && ch <= '9'
}
