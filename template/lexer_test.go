package template

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLexer(t *testing.T) {
	var testCases = []struct {
		input  string
		tokens []token
	}{
		// All keywords
		{"create", []token{mkToken(tokenCreate, "create"), mkToken(tokenEOF, "")}},
		{"cReaTe", []token{mkToken(tokenCreate, "cReaTe"), mkToken(tokenEOF, "")}},
		{"CREATE", []token{mkToken(tokenCreate, "CREATE"), mkToken(tokenEOF, "")}},
		{"table", []token{mkToken(tokenTable, "table"), mkToken(tokenEOF, "")}},
		{"or", []token{mkToken(tokenOr, "or"), mkToken(tokenEOF, "")}},
		{"and", []token{mkToken(tokenAnd, "and"), mkToken(tokenEOF, "")}},
		{"not", []token{mkToken(tokenNot, "not"), mkToken(tokenEOF, "")}},
		{"is", []token{mkToken(tokenIs, "is"), mkToken(tokenEOF, "")}},
		{"rownum", []token{mkToken(tokenRowNum, "rownum"), mkToken(tokenEOF, "")}},
		{"subrownum", []token{mkToken(tokenSubRowNum, "subrownum"), mkToken(tokenEOF, "")}},
		{"null", []token{mkToken(tokenNull, "null"), mkToken(tokenEOF, "")}},
		{"true", []token{mkToken(tokenTrue, "true"), mkToken(tokenEOF, "")}},
		{"false", []token{mkToken(tokenFalse, "false"), mkToken(tokenEOF, "")}},
		{"case", []token{mkToken(tokenCase, "case"), mkToken(tokenEOF, "")}},
		{"when", []token{mkToken(tokenWhen, "when"), mkToken(tokenEOF, "")}},
		{"then", []token{mkToken(tokenThen, "then"), mkToken(tokenEOF, "")}},
		{"else", []token{mkToken(tokenElse, "else"), mkToken(tokenEOF, "")}},
		{"end", []token{mkToken(tokenEnd, "end"), mkToken(tokenEOF, "")}},
		{"timestamp", []token{mkToken(tokenTimestamp, "timestamp"), mkToken(tokenEOF, "")}},
		{"interval", []token{mkToken(tokenInterval, "interval"), mkToken(tokenEOF, "")}},
		{"week", []token{mkToken(tokenWeek, "week"), mkToken(tokenEOF, "")}},
		{"day", []token{mkToken(tokenDay, "day"), mkToken(tokenEOF, "")}},
		{"hour", []token{mkToken(tokenHour, "hour"), mkToken(tokenEOF, "")}},
		{"minute", []token{mkToken(tokenMinute, "minute"), mkToken(tokenEOF, "")}},
		{"second", []token{mkToken(tokenSecond, "second"), mkToken(tokenEOF, "")}},
		{"millisecond", []token{mkToken(tokenMillisecond, "millisecond"), mkToken(tokenEOF, "")}},
		{"microsecond", []token{mkToken(tokenMicrosecond, "microsecond"), mkToken(tokenEOF, "")}},
		{"with", []token{mkToken(tokenWith, "with"), mkToken(tokenEOF, "")}},
		{"time", []token{mkToken(tokenTime, "time"), mkToken(tokenEOF, "")}},
		{"zone", []token{mkToken(tokenZone, "zone"), mkToken(tokenEOF, "")}},
		{"from", []token{mkToken(tokenFrom, "from"), mkToken(tokenEOF, "")}},
		{"for", []token{mkToken(tokenFor, "for"), mkToken(tokenEOF, "")}},
		{"using", []token{mkToken(tokenUsing, "using"), mkToken(tokenEOF, "")}},
		{"characters", []token{mkToken(tokenCharacters, "characters"), mkToken(tokenEOF, "")}},
		{"octets", []token{mkToken(tokenOctets, "octets"), mkToken(tokenEOF, "")}},
		{"overlay", []token{mkToken(tokenOverlay, "overlay"), mkToken(tokenEOF, "")}},
		{"placing", []token{mkToken(tokenPlacing, "placing"), mkToken(tokenEOF, "")}},
		{"current_timestamp", []token{mkToken(tokenCurrentTimestamp, "current_timestamp"), mkToken(tokenEOF, "")}},
		{"array", []token{mkToken(tokenArray, "array"), mkToken(tokenEOF, "")}},
		{"each", []token{mkToken(tokenEach, "each"), mkToken(tokenEOF, "")}},
		{"row", []token{mkToken(tokenRow, "row"), mkToken(tokenEOF, "")}},
		{"of", []token{mkToken(tokenOf, "of"), mkToken(tokenEOF, "")}},
		{"generate", []token{mkToken(tokenGenerate, "generate"), mkToken(tokenEOF, "")}},
		{"rows", []token{mkToken(tokenRows, "rows"), mkToken(tokenEOF, "")}},
		{"x", []token{mkToken(tokenX, "x"), mkToken(tokenEOF, "")}},
		// Identifiers
		{"abc", []token{mkToken(tokenIdent, "abc"), mkToken(tokenEOF, "")}},
		{"_abc", []token{mkToken(tokenIdent, "_abc"), mkToken(tokenEOF, "")}},
		{"_aBc", []token{mkToken(tokenIdent, "_aBc"), mkToken(tokenEOF, "")}},
		{"`abc`", []token{mkToken(tokenIdent, "`abc`"), mkToken(tokenEOF, "")}},
		{"`ab``c`", []token{mkToken(tokenIdent, "`ab``c`"), mkToken(tokenEOF, "")}},
		{`"ab""c"`, []token{mkToken(tokenIdent, `"ab""c"`), mkToken(tokenEOF, "")}},
		{"`ab`c`", []token{mkToken(tokenIdent, "`ab`"), mkToken(tokenIdent, "c"), mkToken(tokenError, "unterminated quoted string")}},
		{`"ab"c"`, []token{mkToken(tokenIdent, `"ab"`), mkToken(tokenIdent, "c"), mkToken(tokenError, "unterminated quoted string")}},
		{"`abc+123`", []token{mkToken(tokenIdent, "`abc+123`"), mkToken(tokenEOF, "")}},
		{"`abc\n123`", []token{mkToken(tokenIdent, "`abc\n123`"), mkToken(tokenEOF, "")}},
		{"你好", []token{mkToken(tokenIdent, "你好"), mkToken(tokenEOF, "")}},
		{"🤔", []token{mkToken(tokenIdent, "🤔"), mkToken(tokenEOF, "")}},
		{"`🤔`", []token{mkToken(tokenIdent, "`🤔`"), mkToken(tokenEOF, "")}},
		{"\"abc\"", []token{mkToken(tokenIdent, "\"abc\""), mkToken(tokenEOF, "")}},
		// Strings
		{"'abc'", []token{mkToken(tokenString, "'abc'"), mkToken(tokenEOF, "")}},
		{"'ab''c'", []token{mkToken(tokenString, "'ab''c'"), mkToken(tokenEOF, "")}},
		{"'ab'c'def", []token{mkToken(tokenString, "'ab'"), mkToken(tokenIdent, "c"), mkToken(tokenError, "unterminated quoted string")}},
		{"'🤔'", []token{mkToken(tokenString, "'🤔'"), mkToken(tokenEOF, "")}},
		{"'你好'", []token{mkToken(tokenString, "'你好'"), mkToken(tokenEOF, "")}},
		// Comments and delimiters
		{"/* abc */", []token{mkToken(tokenComment, "/* abc */"), mkToken(tokenEOF, "")}},
		{"/* abc */ /* 123 */", []token{mkToken(tokenComment, "/* abc */"), mkToken(tokenComment, "/* 123 */"), mkToken(tokenEOF, "")}},
		{"/* abc  /* 123 */", []token{mkToken(tokenComment, "/* abc  /* 123 */"), mkToken(tokenEOF, "")}},
		{"create table /* abc */ t /* 123 */ ()", []token{
			mkToken(tokenCreate, "create"), mkToken(tokenTable, "table"),
			mkToken(tokenComment, "/* abc */"), mkToken(tokenIdent, "t"),
			mkToken(tokenComment, "/* 123 */"), mkToken(tokenLeftParen, "("),
			mkToken(tokenRightParen, ")"), mkToken(tokenEOF, "")},
		},
		{"/* abc 123  ", []token{mkToken(tokenError, "unclosed comment")}},
		{"-- abc", []token{mkToken(tokenComment, "-- abc"), mkToken(tokenEOF, "")}},
		{"--abc", []token{mkToken(tokenComment, "--abc"), mkToken(tokenEOF, "")}},
		{"create table t --abc edf", []token{
			mkToken(tokenCreate, "create"), mkToken(tokenTable, "table"),
			mkToken(tokenIdent, "t"), mkToken(tokenComment, "--abc edf"), mkToken(tokenEOF, ""),
		}},
		{"{{}}", []token{mkToken(tokenLeftDelim, "{{"), mkToken(tokenRightDelim, "}}"), mkToken(tokenEOF, "")}},
		{"/*{{}}*/", []token{mkToken(tokenLeftDelim, "/*{{"), mkToken(tokenRightDelim, "}}*/"), mkToken(tokenEOF, "")}},
		{"/*{{ abc }}*/", []token{
			mkToken(tokenLeftDelim, "/*{{"), mkToken(tokenIdent, "abc"),
			mkToken(tokenRightDelim, "}}*/"), mkToken(tokenEOF, ""),
		}},
		{"/*{{ abc */ }}*/", []token{
			mkToken(tokenComment, "/*{{ abc */"), mkToken(tokenRightDelim, "}}"),
			mkToken(tokenMul, "*"), mkToken(tokenFloatDiv, "/"), mkToken(tokenEOF, ""),
		}},
		{"/*{{ /* abc }}*/", []token{
			mkToken(tokenLeftDelim, "/*{{"), mkToken(tokenFloatDiv, "/"), mkToken(tokenMul, "*"),
			mkToken(tokenIdent, "abc"), mkToken(tokenRightDelim, "}}*/"), mkToken(tokenEOF, ""),
		}},
		// Operators
		{"a + b", []token{mkToken(tokenIdent, "a"), mkToken(tokenAdd, "+"), mkToken(tokenIdent, "b"), mkToken(tokenEOF, "")}},
		{"a+b", []token{mkToken(tokenIdent, "a"), mkToken(tokenAdd, "+"), mkToken(tokenIdent, "b"), mkToken(tokenEOF, "")}},
		{"a+b* c/d", []token{
			mkToken(tokenIdent, "a"), mkToken(tokenAdd, "+"), mkToken(tokenIdent, "b"),
			mkToken(tokenMul, "*"), mkToken(tokenIdent, "c"), mkToken(tokenFloatDiv, "/"),
			mkToken(tokenIdent, "d"), mkToken(tokenEOF, ""),
		}},
		{"a>=b<c || d", []token{
			mkToken(tokenIdent, "a"), mkToken(tokenGE, ">="), mkToken(tokenIdent, "b"),
			mkToken(tokenLT, "<"), mkToken(tokenIdent, "c"), mkToken(tokenConcat, "||"),
			mkToken(tokenIdent, "d"), mkToken(tokenEOF, ""),
		}},
		{"a<=b>c=d<>e & 1 | 2^3~, 1+2;", []token{
			mkToken(tokenIdent, "a"), mkToken(tokenLE, "<="), mkToken(tokenIdent, "b"),
			mkToken(tokenGT, ">"), mkToken(tokenIdent, "c"), mkToken(tokenEQ, "="),
			mkToken(tokenIdent, "d"), mkToken(tokenNE, "<>"), mkToken(tokenIdent, "e"),
			mkToken(tokenBitAnd, "&"), mkToken(tokenNumber, "1"), mkToken(tokenBitOr, "|"),
			mkToken(tokenNumber, "2"), mkToken(tokenBitXor, "^"), mkToken(tokenNumber, "3"),
			mkToken(tokenBitNot, "~"), mkToken(tokenComma, ","), mkToken(tokenNumber, "1"),
			mkToken(tokenAdd, "+"), mkToken(tokenNumber, "2"), mkToken(tokenSemicolon, ";"),
			mkToken(tokenEOF, ""),
		}},
		// Numbers
		{"{{1 02 0x14 0X14 -7.2 1e3 1E3 +1.2e-4 . .0 1. 0xabchi}}", []token{
			mkToken(tokenLeftDelim, "{{"), mkToken(tokenNumber, "1"), mkToken(tokenNumber, "02"),
			mkToken(tokenNumber, "0x14"), mkToken(tokenNumber, "0X14"), mkToken(tokenSub, "-"),
			mkToken(tokenNumber, "7.2"), mkToken(tokenNumber, "1e3"), mkToken(tokenNumber, "1E3"),
			mkToken(tokenAdd, "+"), mkToken(tokenNumber, "1.2e-4"), mkToken(tokenPeriod, "."),
			mkToken(tokenNumber, ".0"), mkToken(tokenNumber, "1."), mkToken(tokenNumber, "0xabc"),
			mkToken(tokenIdent, "hi"), mkToken(tokenRightDelim, "}}"), mkToken(tokenEOF, ""),
		}},
		// Brackets
		{"{([)]}", []token{
			mkToken(tokenLeftBrace, "{"), mkToken(tokenLeftParen, "("), mkToken(tokenLeftBrack, "["),
			mkToken(tokenRightParen, ")"), mkToken(tokenRightBrack, "]"), mkToken(tokenRightBrace, "}"),
			mkToken(tokenEOF, ""),
		}},
		// Assignments
		{"@a := b", []token{
			mkToken(tokenAt, "@"), mkToken(tokenIdent, "a"), mkToken(tokenAssign, ":="),
			mkToken(tokenIdent, "b"), mkToken(tokenEOF, ""),
		}},
		// Single char
		{"!#$%?:", []token{
			mkToken(tokenChar, "!"), mkToken(tokenChar, "#"), mkToken(tokenChar, "$"),
			mkToken(tokenChar, "%"), mkToken(tokenChar, "?"), mkToken(tokenChar, ":"),
			mkToken(tokenEOF, ""),
		}},
	}

	t.Parallel()

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			tokens := lexAll(newLexer(tc.input))
			require.Len(t, tokens, len(tc.tokens))
			for i := 0; i < len(tokens); i++ {
				actual := tokens[i]
				expected := tc.tokens[i]
				require.Equal(t, expected.typ, actual.typ, "token type")
				require.Equal(t, expected.val, actual.val)
				if actual.typ != tokenError {
					valAtPos := tc.input[actual.pos : actual.pos+len(actual.val)]
					require.Equal(t, valAtPos, actual.val, "token pos")
					line := strings.Count(tc.input[:actual.pos], "\n") + 1
					require.Equal(t, line, actual.line, "token line")
					col := actual.pos - strings.LastIndexByte(tc.input[:actual.pos], '\n')
					require.Equal(t, col, actual.col, "token col")
				}
			}
		})
	}
}

func mkToken(typ tokenType, val string) token {
	return token{typ: typ, val: val}
}

func lexAll(l *lexer) []token {
	var tokens []token
	for {
		token := l.lex()
		tokens = append(tokens, token)
		if token.typ == tokenError || token.typ == tokenEOF {
			break
		}
	}
	return tokens
}
