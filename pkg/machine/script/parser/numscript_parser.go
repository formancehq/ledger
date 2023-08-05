// Code generated from NumScript.g4 by ANTLR 4.10.1. DO NOT EDIT.

package parser // NumScript

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/antlr/antlr4/runtime/Go/antlr"
)

// Suppress unused import errors
var _ = fmt.Printf
var _ = strconv.Itoa
var _ = sync.Once{}

type NumScriptParser struct {
	*antlr.BaseParser
}

var numscriptParserStaticData struct {
	once                   sync.Once
	serializedATN          []int32
	literalNames           []string
	symbolicNames          []string
	ruleNames              []string
	predictionContextCache *antlr.PredictionContextCache
	atn                    *antlr.ATN
	decisionToDFA          []*antlr.DFA
}

func numscriptParserInit() {
	staticData := &numscriptParserStaticData
	staticData.literalNames = []string{
		"", "'*'", "'?'", "':'", "'allowing overdraft up to'", "'allowing unbounded overdraft'",
		"','", "", "", "", "", "'vars'", "'meta'", "'set_tx_meta'", "'set_account_meta'",
		"'print'", "'fail'", "'send'", "'source'", "'from'", "'max'", "'destination'",
		"'to'", "'allocate'", "'+'", "'-'", "'=='", "'!='", "'<'", "'<='", "'>'",
		"'>='", "'!'", "'&&'", "'||'", "'('", "')'", "'['", "']'", "'{'", "'}'",
		"'='", "'account'", "'asset'", "'number'", "'monetary'", "'portion'",
		"'string'", "'bool'", "", "", "'remaining'", "'kept'", "'balance'",
		"'save'", "", "'%'",
	}
	staticData.symbolicNames = []string{
		"", "", "", "", "", "", "", "NEWLINE", "WHITESPACE", "MULTILINE_COMMENT",
		"LINE_COMMENT", "VARS", "META", "SET_TX_META", "SET_ACCOUNT_META", "PRINT",
		"FAIL", "SEND", "SOURCE", "FROM", "MAX", "DESTINATION", "TO", "ALLOCATE",
		"OP_ADD", "OP_SUB", "OP_EQ", "OP_NEQ", "OP_LT", "OP_LTE", "OP_GT", "OP_GTE",
		"OP_NOT", "OP_AND", "OP_OR", "LPAREN", "RPAREN", "LBRACK", "RBRACK",
		"LBRACE", "RBRACE", "EQ", "TY_ACCOUNT", "TY_ASSET", "TY_NUMBER", "TY_MONETARY",
		"TY_PORTION", "TY_STRING", "TY_BOOL", "STRING", "PORTION", "REMAINING",
		"KEPT", "BALANCE", "SAVE", "NUMBER", "PERCENT", "VARIABLE_NAME", "ACCOUNT",
		"ASSET",
	}
	staticData.ruleNames = []string{
		"monetary", "monetaryAll", "literal", "variable", "expression", "allotmentPortion",
		"destinationInOrder", "destinationAllotment", "keptOrDestination", "destination",
		"sourceAccountOverdraft", "sourceAccount", "sourceInOrder", "sourceMaxed",
		"source", "sourceAllotment", "valueAwareSource", "statement", "type_",
		"origin", "varDecl", "varListDecl", "script",
	}
	staticData.predictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 1, 59, 335, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 7,
		4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2, 10, 7,
		10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 2, 14, 7, 14, 2, 15, 7, 15,
		2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 18, 2, 19, 7, 19, 2, 20, 7, 20, 2,
		21, 7, 21, 2, 22, 7, 22, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 62, 8, 2, 1, 3, 1, 3, 1,
		4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 3, 4, 76, 8, 4,
		1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4,
		1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 5, 4, 96, 8, 4, 10, 4, 12, 4, 99, 9,
		4, 1, 5, 1, 5, 1, 5, 3, 5, 104, 8, 5, 1, 6, 1, 6, 1, 6, 1, 6, 1, 6, 1,
		6, 1, 6, 4, 6, 113, 8, 6, 11, 6, 12, 6, 114, 1, 6, 1, 6, 1, 6, 1, 6, 1,
		6, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 4, 7, 128, 8, 7, 11, 7, 12, 7, 129,
		1, 7, 1, 7, 1, 8, 1, 8, 1, 8, 3, 8, 137, 8, 8, 1, 9, 1, 9, 1, 9, 3, 9,
		142, 8, 9, 1, 10, 1, 10, 1, 10, 3, 10, 147, 8, 10, 1, 11, 1, 11, 3, 11,
		151, 8, 11, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 4, 12, 158, 8, 12, 11, 12,
		12, 12, 159, 1, 12, 1, 12, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 14, 1,
		14, 1, 14, 3, 14, 172, 8, 14, 1, 15, 1, 15, 1, 15, 1, 15, 1, 15, 1, 15,
		1, 15, 4, 15, 181, 8, 15, 11, 15, 12, 15, 182, 1, 15, 1, 15, 1, 16, 1,
		16, 3, 16, 189, 8, 16, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 3, 17, 196, 8,
		17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17,
		1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1,
		17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17,
		1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 3, 17, 238,
		8, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1,
		17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17,
		1, 17, 1, 17, 1, 17, 3, 17, 263, 8, 17, 1, 17, 1, 17, 1, 17, 3, 17, 268,
		8, 17, 1, 18, 1, 18, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1,
		19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 3, 19, 286, 8, 19, 1, 20,
		1, 20, 1, 20, 1, 20, 3, 20, 292, 8, 20, 1, 21, 1, 21, 1, 21, 1, 21, 1,
		21, 4, 21, 299, 8, 21, 11, 21, 12, 21, 300, 4, 21, 303, 8, 21, 11, 21,
		12, 21, 304, 1, 21, 1, 21, 1, 21, 1, 22, 5, 22, 311, 8, 22, 10, 22, 12,
		22, 314, 9, 22, 1, 22, 3, 22, 317, 8, 22, 1, 22, 1, 22, 1, 22, 5, 22, 322,
		8, 22, 10, 22, 12, 22, 325, 9, 22, 1, 22, 5, 22, 328, 8, 22, 10, 22, 12,
		22, 331, 9, 22, 1, 22, 1, 22, 1, 22, 0, 1, 8, 23, 0, 2, 4, 6, 8, 10, 12,
		14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 0, 3, 1,
		0, 24, 25, 1, 0, 26, 31, 1, 0, 42, 48, 355, 0, 46, 1, 0, 0, 0, 2, 51, 1,
		0, 0, 0, 4, 61, 1, 0, 0, 0, 6, 63, 1, 0, 0, 0, 8, 75, 1, 0, 0, 0, 10, 103,
		1, 0, 0, 0, 12, 105, 1, 0, 0, 0, 14, 121, 1, 0, 0, 0, 16, 136, 1, 0, 0,
		0, 18, 141, 1, 0, 0, 0, 20, 146, 1, 0, 0, 0, 22, 148, 1, 0, 0, 0, 24, 152,
		1, 0, 0, 0, 26, 163, 1, 0, 0, 0, 28, 171, 1, 0, 0, 0, 30, 173, 1, 0, 0,
		0, 32, 188, 1, 0, 0, 0, 34, 267, 1, 0, 0, 0, 36, 269, 1, 0, 0, 0, 38, 285,
		1, 0, 0, 0, 40, 287, 1, 0, 0, 0, 42, 293, 1, 0, 0, 0, 44, 312, 1, 0, 0,
		0, 46, 47, 5, 37, 0, 0, 47, 48, 3, 8, 4, 0, 48, 49, 3, 8, 4, 0, 49, 50,
		5, 38, 0, 0, 50, 1, 1, 0, 0, 0, 51, 52, 5, 37, 0, 0, 52, 53, 3, 8, 4, 0,
		53, 54, 5, 1, 0, 0, 54, 55, 5, 38, 0, 0, 55, 3, 1, 0, 0, 0, 56, 62, 5,
		58, 0, 0, 57, 62, 5, 59, 0, 0, 58, 62, 5, 55, 0, 0, 59, 62, 5, 49, 0, 0,
		60, 62, 5, 50, 0, 0, 61, 56, 1, 0, 0, 0, 61, 57, 1, 0, 0, 0, 61, 58, 1,
		0, 0, 0, 61, 59, 1, 0, 0, 0, 61, 60, 1, 0, 0, 0, 62, 5, 1, 0, 0, 0, 63,
		64, 5, 57, 0, 0, 64, 7, 1, 0, 0, 0, 65, 66, 6, 4, -1, 0, 66, 67, 5, 32,
		0, 0, 67, 76, 3, 8, 4, 8, 68, 76, 3, 4, 2, 0, 69, 76, 3, 6, 3, 0, 70, 76,
		3, 0, 0, 0, 71, 72, 5, 35, 0, 0, 72, 73, 3, 8, 4, 0, 73, 74, 5, 36, 0,
		0, 74, 76, 1, 0, 0, 0, 75, 65, 1, 0, 0, 0, 75, 68, 1, 0, 0, 0, 75, 69,
		1, 0, 0, 0, 75, 70, 1, 0, 0, 0, 75, 71, 1, 0, 0, 0, 76, 97, 1, 0, 0, 0,
		77, 78, 10, 10, 0, 0, 78, 79, 7, 0, 0, 0, 79, 96, 3, 8, 4, 11, 80, 81,
		10, 9, 0, 0, 81, 82, 7, 1, 0, 0, 82, 96, 3, 8, 4, 10, 83, 84, 10, 7, 0,
		0, 84, 85, 5, 33, 0, 0, 85, 96, 3, 8, 4, 8, 86, 87, 10, 6, 0, 0, 87, 88,
		5, 34, 0, 0, 88, 96, 3, 8, 4, 7, 89, 90, 10, 2, 0, 0, 90, 91, 5, 2, 0,
		0, 91, 92, 3, 8, 4, 0, 92, 93, 5, 3, 0, 0, 93, 94, 3, 8, 4, 3, 94, 96,
		1, 0, 0, 0, 95, 77, 1, 0, 0, 0, 95, 80, 1, 0, 0, 0, 95, 83, 1, 0, 0, 0,
		95, 86, 1, 0, 0, 0, 95, 89, 1, 0, 0, 0, 96, 99, 1, 0, 0, 0, 97, 95, 1,
		0, 0, 0, 97, 98, 1, 0, 0, 0, 98, 9, 1, 0, 0, 0, 99, 97, 1, 0, 0, 0, 100,
		104, 5, 50, 0, 0, 101, 104, 3, 6, 3, 0, 102, 104, 5, 51, 0, 0, 103, 100,
		1, 0, 0, 0, 103, 101, 1, 0, 0, 0, 103, 102, 1, 0, 0, 0, 104, 11, 1, 0,
		0, 0, 105, 106, 5, 39, 0, 0, 106, 112, 5, 7, 0, 0, 107, 108, 5, 20, 0,
		0, 108, 109, 3, 8, 4, 0, 109, 110, 3, 16, 8, 0, 110, 111, 5, 7, 0, 0, 111,
		113, 1, 0, 0, 0, 112, 107, 1, 0, 0, 0, 113, 114, 1, 0, 0, 0, 114, 112,
		1, 0, 0, 0, 114, 115, 1, 0, 0, 0, 115, 116, 1, 0, 0, 0, 116, 117, 5, 51,
		0, 0, 117, 118, 3, 16, 8, 0, 118, 119, 5, 7, 0, 0, 119, 120, 5, 40, 0,
		0, 120, 13, 1, 0, 0, 0, 121, 122, 5, 39, 0, 0, 122, 127, 5, 7, 0, 0, 123,
		124, 3, 10, 5, 0, 124, 125, 3, 16, 8, 0, 125, 126, 5, 7, 0, 0, 126, 128,
		1, 0, 0, 0, 127, 123, 1, 0, 0, 0, 128, 129, 1, 0, 0, 0, 129, 127, 1, 0,
		0, 0, 129, 130, 1, 0, 0, 0, 130, 131, 1, 0, 0, 0, 131, 132, 5, 40, 0, 0,
		132, 15, 1, 0, 0, 0, 133, 134, 5, 22, 0, 0, 134, 137, 3, 18, 9, 0, 135,
		137, 5, 52, 0, 0, 136, 133, 1, 0, 0, 0, 136, 135, 1, 0, 0, 0, 137, 17,
		1, 0, 0, 0, 138, 142, 3, 8, 4, 0, 139, 142, 3, 12, 6, 0, 140, 142, 3, 14,
		7, 0, 141, 138, 1, 0, 0, 0, 141, 139, 1, 0, 0, 0, 141, 140, 1, 0, 0, 0,
		142, 19, 1, 0, 0, 0, 143, 144, 5, 4, 0, 0, 144, 147, 3, 8, 4, 0, 145, 147,
		5, 5, 0, 0, 146, 143, 1, 0, 0, 0, 146, 145, 1, 0, 0, 0, 147, 21, 1, 0,
		0, 0, 148, 150, 3, 8, 4, 0, 149, 151, 3, 20, 10, 0, 150, 149, 1, 0, 0,
		0, 150, 151, 1, 0, 0, 0, 151, 23, 1, 0, 0, 0, 152, 153, 5, 39, 0, 0, 153,
		157, 5, 7, 0, 0, 154, 155, 3, 28, 14, 0, 155, 156, 5, 7, 0, 0, 156, 158,
		1, 0, 0, 0, 157, 154, 1, 0, 0, 0, 158, 159, 1, 0, 0, 0, 159, 157, 1, 0,
		0, 0, 159, 160, 1, 0, 0, 0, 160, 161, 1, 0, 0, 0, 161, 162, 5, 40, 0, 0,
		162, 25, 1, 0, 0, 0, 163, 164, 5, 20, 0, 0, 164, 165, 3, 8, 4, 0, 165,
		166, 5, 19, 0, 0, 166, 167, 3, 28, 14, 0, 167, 27, 1, 0, 0, 0, 168, 172,
		3, 22, 11, 0, 169, 172, 3, 26, 13, 0, 170, 172, 3, 24, 12, 0, 171, 168,
		1, 0, 0, 0, 171, 169, 1, 0, 0, 0, 171, 170, 1, 0, 0, 0, 172, 29, 1, 0,
		0, 0, 173, 174, 5, 39, 0, 0, 174, 180, 5, 7, 0, 0, 175, 176, 3, 10, 5,
		0, 176, 177, 5, 19, 0, 0, 177, 178, 3, 28, 14, 0, 178, 179, 5, 7, 0, 0,
		179, 181, 1, 0, 0, 0, 180, 175, 1, 0, 0, 0, 181, 182, 1, 0, 0, 0, 182,
		180, 1, 0, 0, 0, 182, 183, 1, 0, 0, 0, 183, 184, 1, 0, 0, 0, 184, 185,
		5, 40, 0, 0, 185, 31, 1, 0, 0, 0, 186, 189, 3, 28, 14, 0, 187, 189, 3,
		30, 15, 0, 188, 186, 1, 0, 0, 0, 188, 187, 1, 0, 0, 0, 189, 33, 1, 0, 0,
		0, 190, 191, 5, 15, 0, 0, 191, 268, 3, 8, 4, 0, 192, 195, 5, 54, 0, 0,
		193, 196, 3, 8, 4, 0, 194, 196, 3, 2, 1, 0, 195, 193, 1, 0, 0, 0, 195,
		194, 1, 0, 0, 0, 196, 197, 1, 0, 0, 0, 197, 198, 5, 19, 0, 0, 198, 199,
		3, 8, 4, 0, 199, 268, 1, 0, 0, 0, 200, 201, 5, 13, 0, 0, 201, 202, 5, 35,
		0, 0, 202, 203, 5, 49, 0, 0, 203, 204, 5, 6, 0, 0, 204, 205, 3, 8, 4, 0,
		205, 206, 5, 36, 0, 0, 206, 268, 1, 0, 0, 0, 207, 208, 5, 14, 0, 0, 208,
		209, 5, 35, 0, 0, 209, 210, 3, 8, 4, 0, 210, 211, 5, 6, 0, 0, 211, 212,
		5, 49, 0, 0, 212, 213, 5, 6, 0, 0, 213, 214, 3, 8, 4, 0, 214, 215, 5, 36,
		0, 0, 215, 268, 1, 0, 0, 0, 216, 268, 5, 16, 0, 0, 217, 218, 5, 17, 0,
		0, 218, 219, 3, 8, 4, 0, 219, 220, 5, 35, 0, 0, 220, 237, 5, 7, 0, 0, 221,
		222, 5, 18, 0, 0, 222, 223, 5, 41, 0, 0, 223, 224, 3, 32, 16, 0, 224, 225,
		5, 7, 0, 0, 225, 226, 5, 21, 0, 0, 226, 227, 5, 41, 0, 0, 227, 228, 3,
		18, 9, 0, 228, 238, 1, 0, 0, 0, 229, 230, 5, 21, 0, 0, 230, 231, 5, 41,
		0, 0, 231, 232, 3, 18, 9, 0, 232, 233, 5, 7, 0, 0, 233, 234, 5, 18, 0,
		0, 234, 235, 5, 41, 0, 0, 235, 236, 3, 32, 16, 0, 236, 238, 1, 0, 0, 0,
		237, 221, 1, 0, 0, 0, 237, 229, 1, 0, 0, 0, 238, 239, 1, 0, 0, 0, 239,
		240, 5, 7, 0, 0, 240, 241, 5, 36, 0, 0, 241, 268, 1, 0, 0, 0, 242, 243,
		5, 17, 0, 0, 243, 244, 3, 2, 1, 0, 244, 245, 5, 35, 0, 0, 245, 262, 5,
		7, 0, 0, 246, 247, 5, 18, 0, 0, 247, 248, 5, 41, 0, 0, 248, 249, 3, 28,
		14, 0, 249, 250, 5, 7, 0, 0, 250, 251, 5, 21, 0, 0, 251, 252, 5, 41, 0,
		0, 252, 253, 3, 18, 9, 0, 253, 263, 1, 0, 0, 0, 254, 255, 5, 21, 0, 0,
		255, 256, 5, 41, 0, 0, 256, 257, 3, 18, 9, 0, 257, 258, 5, 7, 0, 0, 258,
		259, 5, 18, 0, 0, 259, 260, 5, 41, 0, 0, 260, 261, 3, 28, 14, 0, 261, 263,
		1, 0, 0, 0, 262, 246, 1, 0, 0, 0, 262, 254, 1, 0, 0, 0, 263, 264, 1, 0,
		0, 0, 264, 265, 5, 7, 0, 0, 265, 266, 5, 36, 0, 0, 266, 268, 1, 0, 0, 0,
		267, 190, 1, 0, 0, 0, 267, 192, 1, 0, 0, 0, 267, 200, 1, 0, 0, 0, 267,
		207, 1, 0, 0, 0, 267, 216, 1, 0, 0, 0, 267, 217, 1, 0, 0, 0, 267, 242,
		1, 0, 0, 0, 268, 35, 1, 0, 0, 0, 269, 270, 7, 2, 0, 0, 270, 37, 1, 0, 0,
		0, 271, 272, 5, 12, 0, 0, 272, 273, 5, 35, 0, 0, 273, 274, 3, 8, 4, 0,
		274, 275, 5, 6, 0, 0, 275, 276, 5, 49, 0, 0, 276, 277, 5, 36, 0, 0, 277,
		286, 1, 0, 0, 0, 278, 279, 5, 53, 0, 0, 279, 280, 5, 35, 0, 0, 280, 281,
		3, 8, 4, 0, 281, 282, 5, 6, 0, 0, 282, 283, 3, 8, 4, 0, 283, 284, 5, 36,
		0, 0, 284, 286, 1, 0, 0, 0, 285, 271, 1, 0, 0, 0, 285, 278, 1, 0, 0, 0,
		286, 39, 1, 0, 0, 0, 287, 288, 3, 36, 18, 0, 288, 291, 3, 6, 3, 0, 289,
		290, 5, 41, 0, 0, 290, 292, 3, 38, 19, 0, 291, 289, 1, 0, 0, 0, 291, 292,
		1, 0, 0, 0, 292, 41, 1, 0, 0, 0, 293, 294, 5, 11, 0, 0, 294, 295, 5, 39,
		0, 0, 295, 302, 5, 7, 0, 0, 296, 298, 3, 40, 20, 0, 297, 299, 5, 7, 0,
		0, 298, 297, 1, 0, 0, 0, 299, 300, 1, 0, 0, 0, 300, 298, 1, 0, 0, 0, 300,
		301, 1, 0, 0, 0, 301, 303, 1, 0, 0, 0, 302, 296, 1, 0, 0, 0, 303, 304,
		1, 0, 0, 0, 304, 302, 1, 0, 0, 0, 304, 305, 1, 0, 0, 0, 305, 306, 1, 0,
		0, 0, 306, 307, 5, 40, 0, 0, 307, 308, 5, 7, 0, 0, 308, 43, 1, 0, 0, 0,
		309, 311, 5, 7, 0, 0, 310, 309, 1, 0, 0, 0, 311, 314, 1, 0, 0, 0, 312,
		310, 1, 0, 0, 0, 312, 313, 1, 0, 0, 0, 313, 316, 1, 0, 0, 0, 314, 312,
		1, 0, 0, 0, 315, 317, 3, 42, 21, 0, 316, 315, 1, 0, 0, 0, 316, 317, 1,
		0, 0, 0, 317, 318, 1, 0, 0, 0, 318, 323, 3, 34, 17, 0, 319, 320, 5, 7,
		0, 0, 320, 322, 3, 34, 17, 0, 321, 319, 1, 0, 0, 0, 322, 325, 1, 0, 0,
		0, 323, 321, 1, 0, 0, 0, 323, 324, 1, 0, 0, 0, 324, 329, 1, 0, 0, 0, 325,
		323, 1, 0, 0, 0, 326, 328, 5, 7, 0, 0, 327, 326, 1, 0, 0, 0, 328, 331,
		1, 0, 0, 0, 329, 327, 1, 0, 0, 0, 329, 330, 1, 0, 0, 0, 330, 332, 1, 0,
		0, 0, 331, 329, 1, 0, 0, 0, 332, 333, 5, 0, 0, 1, 333, 45, 1, 0, 0, 0,
		27, 61, 75, 95, 97, 103, 114, 129, 136, 141, 146, 150, 159, 171, 182, 188,
		195, 237, 262, 267, 285, 291, 300, 304, 312, 316, 323, 329,
	}
	deserializer := antlr.NewATNDeserializer(nil)
	staticData.atn = deserializer.Deserialize(staticData.serializedATN)
	atn := staticData.atn
	staticData.decisionToDFA = make([]*antlr.DFA, len(atn.DecisionToState))
	decisionToDFA := staticData.decisionToDFA
	for index, state := range atn.DecisionToState {
		decisionToDFA[index] = antlr.NewDFA(state, index)
	}
}

// NumScriptParserInit initializes any static state used to implement NumScriptParser. By default the
// static state used to implement the parser is lazily initialized during the first call to
// NewNumScriptParser(). You can call this function if you wish to initialize the static state ahead
// of time.
func NumScriptParserInit() {
	staticData := &numscriptParserStaticData
	staticData.once.Do(numscriptParserInit)
}

// NewNumScriptParser produces a new parser instance for the optional input antlr.TokenStream.
func NewNumScriptParser(input antlr.TokenStream) *NumScriptParser {
	NumScriptParserInit()
	this := new(NumScriptParser)
	this.BaseParser = antlr.NewBaseParser(input)
	staticData := &numscriptParserStaticData
	this.Interpreter = antlr.NewParserATNSimulator(this, staticData.atn, staticData.decisionToDFA, staticData.predictionContextCache)
	this.RuleNames = staticData.ruleNames
	this.LiteralNames = staticData.literalNames
	this.SymbolicNames = staticData.symbolicNames
	this.GrammarFileName = "NumScript.g4"

	return this
}

// NumScriptParser tokens.
const (
	NumScriptParserEOF               = antlr.TokenEOF
	NumScriptParserT__0              = 1
	NumScriptParserT__1              = 2
	NumScriptParserT__2              = 3
	NumScriptParserT__3              = 4
	NumScriptParserT__4              = 5
	NumScriptParserT__5              = 6
	NumScriptParserNEWLINE           = 7
	NumScriptParserWHITESPACE        = 8
	NumScriptParserMULTILINE_COMMENT = 9
	NumScriptParserLINE_COMMENT      = 10
	NumScriptParserVARS              = 11
	NumScriptParserMETA              = 12
	NumScriptParserSET_TX_META       = 13
	NumScriptParserSET_ACCOUNT_META  = 14
	NumScriptParserPRINT             = 15
	NumScriptParserFAIL              = 16
	NumScriptParserSEND              = 17
	NumScriptParserSOURCE            = 18
	NumScriptParserFROM              = 19
	NumScriptParserMAX               = 20
	NumScriptParserDESTINATION       = 21
	NumScriptParserTO                = 22
	NumScriptParserALLOCATE          = 23
	NumScriptParserOP_ADD            = 24
	NumScriptParserOP_SUB            = 25
	NumScriptParserOP_EQ             = 26
	NumScriptParserOP_NEQ            = 27
	NumScriptParserOP_LT             = 28
	NumScriptParserOP_LTE            = 29
	NumScriptParserOP_GT             = 30
	NumScriptParserOP_GTE            = 31
	NumScriptParserOP_NOT            = 32
	NumScriptParserOP_AND            = 33
	NumScriptParserOP_OR             = 34
	NumScriptParserLPAREN            = 35
	NumScriptParserRPAREN            = 36
	NumScriptParserLBRACK            = 37
	NumScriptParserRBRACK            = 38
	NumScriptParserLBRACE            = 39
	NumScriptParserRBRACE            = 40
	NumScriptParserEQ                = 41
	NumScriptParserTY_ACCOUNT        = 42
	NumScriptParserTY_ASSET          = 43
	NumScriptParserTY_NUMBER         = 44
	NumScriptParserTY_MONETARY       = 45
	NumScriptParserTY_PORTION        = 46
	NumScriptParserTY_STRING         = 47
	NumScriptParserTY_BOOL           = 48
	NumScriptParserSTRING            = 49
	NumScriptParserPORTION           = 50
	NumScriptParserREMAINING         = 51
	NumScriptParserKEPT              = 52
	NumScriptParserBALANCE           = 53
	NumScriptParserSAVE              = 54
	NumScriptParserNUMBER            = 55
	NumScriptParserPERCENT           = 56
	NumScriptParserVARIABLE_NAME     = 57
	NumScriptParserACCOUNT           = 58
	NumScriptParserASSET             = 59
)

// NumScriptParser rules.
const (
	NumScriptParserRULE_monetary               = 0
	NumScriptParserRULE_monetaryAll            = 1
	NumScriptParserRULE_literal                = 2
	NumScriptParserRULE_variable               = 3
	NumScriptParserRULE_expression             = 4
	NumScriptParserRULE_allotmentPortion       = 5
	NumScriptParserRULE_destinationInOrder     = 6
	NumScriptParserRULE_destinationAllotment   = 7
	NumScriptParserRULE_keptOrDestination      = 8
	NumScriptParserRULE_destination            = 9
	NumScriptParserRULE_sourceAccountOverdraft = 10
	NumScriptParserRULE_sourceAccount          = 11
	NumScriptParserRULE_sourceInOrder          = 12
	NumScriptParserRULE_sourceMaxed            = 13
	NumScriptParserRULE_source                 = 14
	NumScriptParserRULE_sourceAllotment        = 15
	NumScriptParserRULE_valueAwareSource       = 16
	NumScriptParserRULE_statement              = 17
	NumScriptParserRULE_type_                  = 18
	NumScriptParserRULE_origin                 = 19
	NumScriptParserRULE_varDecl                = 20
	NumScriptParserRULE_varListDecl            = 21
	NumScriptParserRULE_script                 = 22
)

// IMonetaryContext is an interface to support dynamic dispatch.
type IMonetaryContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// GetAsset returns the asset rule contexts.
	GetAsset() IExpressionContext

	// GetAmt returns the amt rule contexts.
	GetAmt() IExpressionContext

	// SetAsset sets the asset rule contexts.
	SetAsset(IExpressionContext)

	// SetAmt sets the amt rule contexts.
	SetAmt(IExpressionContext)

	// IsMonetaryContext differentiates from other interfaces.
	IsMonetaryContext()
}

type MonetaryContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
	asset  IExpressionContext
	amt    IExpressionContext
}

func NewEmptyMonetaryContext() *MonetaryContext {
	var p = new(MonetaryContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_monetary
	return p
}

func (*MonetaryContext) IsMonetaryContext() {}

func NewMonetaryContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *MonetaryContext {
	var p = new(MonetaryContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_monetary

	return p
}

func (s *MonetaryContext) GetParser() antlr.Parser { return s.parser }

func (s *MonetaryContext) GetAsset() IExpressionContext { return s.asset }

func (s *MonetaryContext) GetAmt() IExpressionContext { return s.amt }

func (s *MonetaryContext) SetAsset(v IExpressionContext) { s.asset = v }

func (s *MonetaryContext) SetAmt(v IExpressionContext) { s.amt = v }

func (s *MonetaryContext) LBRACK() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLBRACK, 0)
}

func (s *MonetaryContext) RBRACK() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRBRACK, 0)
}

func (s *MonetaryContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *MonetaryContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *MonetaryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MonetaryContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *MonetaryContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterMonetary(s)
	}
}

func (s *MonetaryContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitMonetary(s)
	}
}

func (p *NumScriptParser) Monetary() (localctx IMonetaryContext) {
	this := p
	_ = this

	localctx = NewMonetaryContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 0, NumScriptParserRULE_monetary)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(46)
		p.Match(NumScriptParserLBRACK)
	}
	{
		p.SetState(47)

		var _x = p.expression(0)

		localctx.(*MonetaryContext).asset = _x
	}
	{
		p.SetState(48)

		var _x = p.expression(0)

		localctx.(*MonetaryContext).amt = _x
	}
	{
		p.SetState(49)
		p.Match(NumScriptParserRBRACK)
	}

	return localctx
}

// IMonetaryAllContext is an interface to support dynamic dispatch.
type IMonetaryAllContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// GetAsset returns the asset rule contexts.
	GetAsset() IExpressionContext

	// SetAsset sets the asset rule contexts.
	SetAsset(IExpressionContext)

	// IsMonetaryAllContext differentiates from other interfaces.
	IsMonetaryAllContext()
}

type MonetaryAllContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
	asset  IExpressionContext
}

func NewEmptyMonetaryAllContext() *MonetaryAllContext {
	var p = new(MonetaryAllContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_monetaryAll
	return p
}

func (*MonetaryAllContext) IsMonetaryAllContext() {}

func NewMonetaryAllContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *MonetaryAllContext {
	var p = new(MonetaryAllContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_monetaryAll

	return p
}

func (s *MonetaryAllContext) GetParser() antlr.Parser { return s.parser }

func (s *MonetaryAllContext) GetAsset() IExpressionContext { return s.asset }

func (s *MonetaryAllContext) SetAsset(v IExpressionContext) { s.asset = v }

func (s *MonetaryAllContext) LBRACK() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLBRACK, 0)
}

func (s *MonetaryAllContext) RBRACK() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRBRACK, 0)
}

func (s *MonetaryAllContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *MonetaryAllContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *MonetaryAllContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *MonetaryAllContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterMonetaryAll(s)
	}
}

func (s *MonetaryAllContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitMonetaryAll(s)
	}
}

func (p *NumScriptParser) MonetaryAll() (localctx IMonetaryAllContext) {
	this := p
	_ = this

	localctx = NewMonetaryAllContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 2, NumScriptParserRULE_monetaryAll)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(51)
		p.Match(NumScriptParserLBRACK)
	}
	{
		p.SetState(52)

		var _x = p.expression(0)

		localctx.(*MonetaryAllContext).asset = _x
	}
	{
		p.SetState(53)
		p.Match(NumScriptParserT__0)
	}
	{
		p.SetState(54)
		p.Match(NumScriptParserRBRACK)
	}

	return localctx
}

// ILiteralContext is an interface to support dynamic dispatch.
type ILiteralContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsLiteralContext differentiates from other interfaces.
	IsLiteralContext()
}

type LiteralContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyLiteralContext() *LiteralContext {
	var p = new(LiteralContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_literal
	return p
}

func (*LiteralContext) IsLiteralContext() {}

func NewLiteralContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *LiteralContext {
	var p = new(LiteralContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_literal

	return p
}

func (s *LiteralContext) GetParser() antlr.Parser { return s.parser }

func (s *LiteralContext) CopyFrom(ctx *LiteralContext) {
	s.BaseParserRuleContext.CopyFrom(ctx.BaseParserRuleContext)
}

func (s *LiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LiteralContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type LitPortionContext struct {
	*LiteralContext
}

func NewLitPortionContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LitPortionContext {
	var p = new(LitPortionContext)

	p.LiteralContext = NewEmptyLiteralContext()
	p.parser = parser
	p.CopyFrom(ctx.(*LiteralContext))

	return p
}

func (s *LitPortionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LitPortionContext) PORTION() antlr.TerminalNode {
	return s.GetToken(NumScriptParserPORTION, 0)
}

func (s *LitPortionContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterLitPortion(s)
	}
}

func (s *LitPortionContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitLitPortion(s)
	}
}

type LitStringContext struct {
	*LiteralContext
}

func NewLitStringContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LitStringContext {
	var p = new(LitStringContext)

	p.LiteralContext = NewEmptyLiteralContext()
	p.parser = parser
	p.CopyFrom(ctx.(*LiteralContext))

	return p
}

func (s *LitStringContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LitStringContext) STRING() antlr.TerminalNode {
	return s.GetToken(NumScriptParserSTRING, 0)
}

func (s *LitStringContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterLitString(s)
	}
}

func (s *LitStringContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitLitString(s)
	}
}

type LitAccountContext struct {
	*LiteralContext
}

func NewLitAccountContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LitAccountContext {
	var p = new(LitAccountContext)

	p.LiteralContext = NewEmptyLiteralContext()
	p.parser = parser
	p.CopyFrom(ctx.(*LiteralContext))

	return p
}

func (s *LitAccountContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LitAccountContext) ACCOUNT() antlr.TerminalNode {
	return s.GetToken(NumScriptParserACCOUNT, 0)
}

func (s *LitAccountContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterLitAccount(s)
	}
}

func (s *LitAccountContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitLitAccount(s)
	}
}

type LitAssetContext struct {
	*LiteralContext
}

func NewLitAssetContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LitAssetContext {
	var p = new(LitAssetContext)

	p.LiteralContext = NewEmptyLiteralContext()
	p.parser = parser
	p.CopyFrom(ctx.(*LiteralContext))

	return p
}

func (s *LitAssetContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LitAssetContext) ASSET() antlr.TerminalNode {
	return s.GetToken(NumScriptParserASSET, 0)
}

func (s *LitAssetContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterLitAsset(s)
	}
}

func (s *LitAssetContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitLitAsset(s)
	}
}

type LitNumberContext struct {
	*LiteralContext
}

func NewLitNumberContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LitNumberContext {
	var p = new(LitNumberContext)

	p.LiteralContext = NewEmptyLiteralContext()
	p.parser = parser
	p.CopyFrom(ctx.(*LiteralContext))

	return p
}

func (s *LitNumberContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LitNumberContext) NUMBER() antlr.TerminalNode {
	return s.GetToken(NumScriptParserNUMBER, 0)
}

func (s *LitNumberContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterLitNumber(s)
	}
}

func (s *LitNumberContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitLitNumber(s)
	}
}

func (p *NumScriptParser) Literal() (localctx ILiteralContext) {
	this := p
	_ = this

	localctx = NewLiteralContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 4, NumScriptParserRULE_literal)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(61)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case NumScriptParserACCOUNT:
		localctx = NewLitAccountContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(56)
			p.Match(NumScriptParserACCOUNT)
		}

	case NumScriptParserASSET:
		localctx = NewLitAssetContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(57)
			p.Match(NumScriptParserASSET)
		}

	case NumScriptParserNUMBER:
		localctx = NewLitNumberContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(58)
			p.Match(NumScriptParserNUMBER)
		}

	case NumScriptParserSTRING:
		localctx = NewLitStringContext(p, localctx)
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(59)
			p.Match(NumScriptParserSTRING)
		}

	case NumScriptParserPORTION:
		localctx = NewLitPortionContext(p, localctx)
		p.EnterOuterAlt(localctx, 5)
		{
			p.SetState(60)
			p.Match(NumScriptParserPORTION)
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// IVariableContext is an interface to support dynamic dispatch.
type IVariableContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsVariableContext differentiates from other interfaces.
	IsVariableContext()
}

type VariableContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyVariableContext() *VariableContext {
	var p = new(VariableContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_variable
	return p
}

func (*VariableContext) IsVariableContext() {}

func NewVariableContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *VariableContext {
	var p = new(VariableContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_variable

	return p
}

func (s *VariableContext) GetParser() antlr.Parser { return s.parser }

func (s *VariableContext) VARIABLE_NAME() antlr.TerminalNode {
	return s.GetToken(NumScriptParserVARIABLE_NAME, 0)
}

func (s *VariableContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *VariableContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *VariableContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterVariable(s)
	}
}

func (s *VariableContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitVariable(s)
	}
}

func (p *NumScriptParser) Variable() (localctx IVariableContext) {
	this := p
	_ = this

	localctx = NewVariableContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 6, NumScriptParserRULE_variable)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(63)
		p.Match(NumScriptParserVARIABLE_NAME)
	}

	return localctx
}

// IExpressionContext is an interface to support dynamic dispatch.
type IExpressionContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsExpressionContext differentiates from other interfaces.
	IsExpressionContext()
}

type ExpressionContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyExpressionContext() *ExpressionContext {
	var p = new(ExpressionContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_expression
	return p
}

func (*ExpressionContext) IsExpressionContext() {}

func NewExpressionContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ExpressionContext {
	var p = new(ExpressionContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_expression

	return p
}

func (s *ExpressionContext) GetParser() antlr.Parser { return s.parser }

func (s *ExpressionContext) CopyFrom(ctx *ExpressionContext) {
	s.BaseParserRuleContext.CopyFrom(ctx.BaseParserRuleContext)
}

func (s *ExpressionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExpressionContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type ExprAddSubContext struct {
	*ExpressionContext
	lhs IExpressionContext
	op  antlr.Token
	rhs IExpressionContext
}

func NewExprAddSubContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExprAddSubContext {
	var p = new(ExprAddSubContext)

	p.ExpressionContext = NewEmptyExpressionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ExpressionContext))

	return p
}

func (s *ExprAddSubContext) GetOp() antlr.Token { return s.op }

func (s *ExprAddSubContext) SetOp(v antlr.Token) { s.op = v }

func (s *ExprAddSubContext) GetLhs() IExpressionContext { return s.lhs }

func (s *ExprAddSubContext) GetRhs() IExpressionContext { return s.rhs }

func (s *ExprAddSubContext) SetLhs(v IExpressionContext) { s.lhs = v }

func (s *ExprAddSubContext) SetRhs(v IExpressionContext) { s.rhs = v }

func (s *ExprAddSubContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprAddSubContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *ExprAddSubContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ExprAddSubContext) OP_ADD() antlr.TerminalNode {
	return s.GetToken(NumScriptParserOP_ADD, 0)
}

func (s *ExprAddSubContext) OP_SUB() antlr.TerminalNode {
	return s.GetToken(NumScriptParserOP_SUB, 0)
}

func (s *ExprAddSubContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterExprAddSub(s)
	}
}

func (s *ExprAddSubContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitExprAddSub(s)
	}
}

type ExprTernaryContext struct {
	*ExpressionContext
	cond    IExpressionContext
	ifTrue  IExpressionContext
	ifFalse IExpressionContext
}

func NewExprTernaryContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExprTernaryContext {
	var p = new(ExprTernaryContext)

	p.ExpressionContext = NewEmptyExpressionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ExpressionContext))

	return p
}

func (s *ExprTernaryContext) GetCond() IExpressionContext { return s.cond }

func (s *ExprTernaryContext) GetIfTrue() IExpressionContext { return s.ifTrue }

func (s *ExprTernaryContext) GetIfFalse() IExpressionContext { return s.ifFalse }

func (s *ExprTernaryContext) SetCond(v IExpressionContext) { s.cond = v }

func (s *ExprTernaryContext) SetIfTrue(v IExpressionContext) { s.ifTrue = v }

func (s *ExprTernaryContext) SetIfFalse(v IExpressionContext) { s.ifFalse = v }

func (s *ExprTernaryContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprTernaryContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *ExprTernaryContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ExprTernaryContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterExprTernary(s)
	}
}

func (s *ExprTernaryContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitExprTernary(s)
	}
}

type ExprLogicalNotContext struct {
	*ExpressionContext
	lhs IExpressionContext
}

func NewExprLogicalNotContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExprLogicalNotContext {
	var p = new(ExprLogicalNotContext)

	p.ExpressionContext = NewEmptyExpressionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ExpressionContext))

	return p
}

func (s *ExprLogicalNotContext) GetLhs() IExpressionContext { return s.lhs }

func (s *ExprLogicalNotContext) SetLhs(v IExpressionContext) { s.lhs = v }

func (s *ExprLogicalNotContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprLogicalNotContext) OP_NOT() antlr.TerminalNode {
	return s.GetToken(NumScriptParserOP_NOT, 0)
}

func (s *ExprLogicalNotContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ExprLogicalNotContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterExprLogicalNot(s)
	}
}

func (s *ExprLogicalNotContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitExprLogicalNot(s)
	}
}

type ExprArithmeticConditionContext struct {
	*ExpressionContext
	lhs IExpressionContext
	op  antlr.Token
	rhs IExpressionContext
}

func NewExprArithmeticConditionContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExprArithmeticConditionContext {
	var p = new(ExprArithmeticConditionContext)

	p.ExpressionContext = NewEmptyExpressionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ExpressionContext))

	return p
}

func (s *ExprArithmeticConditionContext) GetOp() antlr.Token { return s.op }

func (s *ExprArithmeticConditionContext) SetOp(v antlr.Token) { s.op = v }

func (s *ExprArithmeticConditionContext) GetLhs() IExpressionContext { return s.lhs }

func (s *ExprArithmeticConditionContext) GetRhs() IExpressionContext { return s.rhs }

func (s *ExprArithmeticConditionContext) SetLhs(v IExpressionContext) { s.lhs = v }

func (s *ExprArithmeticConditionContext) SetRhs(v IExpressionContext) { s.rhs = v }

func (s *ExprArithmeticConditionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprArithmeticConditionContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *ExprArithmeticConditionContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ExprArithmeticConditionContext) OP_EQ() antlr.TerminalNode {
	return s.GetToken(NumScriptParserOP_EQ, 0)
}

func (s *ExprArithmeticConditionContext) OP_NEQ() antlr.TerminalNode {
	return s.GetToken(NumScriptParserOP_NEQ, 0)
}

func (s *ExprArithmeticConditionContext) OP_LT() antlr.TerminalNode {
	return s.GetToken(NumScriptParserOP_LT, 0)
}

func (s *ExprArithmeticConditionContext) OP_LTE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserOP_LTE, 0)
}

func (s *ExprArithmeticConditionContext) OP_GT() antlr.TerminalNode {
	return s.GetToken(NumScriptParserOP_GT, 0)
}

func (s *ExprArithmeticConditionContext) OP_GTE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserOP_GTE, 0)
}

func (s *ExprArithmeticConditionContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterExprArithmeticCondition(s)
	}
}

func (s *ExprArithmeticConditionContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitExprArithmeticCondition(s)
	}
}

type ExprLiteralContext struct {
	*ExpressionContext
	lit ILiteralContext
}

func NewExprLiteralContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExprLiteralContext {
	var p = new(ExprLiteralContext)

	p.ExpressionContext = NewEmptyExpressionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ExpressionContext))

	return p
}

func (s *ExprLiteralContext) GetLit() ILiteralContext { return s.lit }

func (s *ExprLiteralContext) SetLit(v ILiteralContext) { s.lit = v }

func (s *ExprLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprLiteralContext) Literal() ILiteralContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ILiteralContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ILiteralContext)
}

func (s *ExprLiteralContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterExprLiteral(s)
	}
}

func (s *ExprLiteralContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitExprLiteral(s)
	}
}

type ExprLogicalOrContext struct {
	*ExpressionContext
	lhs IExpressionContext
	op  antlr.Token
	rhs IExpressionContext
}

func NewExprLogicalOrContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExprLogicalOrContext {
	var p = new(ExprLogicalOrContext)

	p.ExpressionContext = NewEmptyExpressionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ExpressionContext))

	return p
}

func (s *ExprLogicalOrContext) GetOp() antlr.Token { return s.op }

func (s *ExprLogicalOrContext) SetOp(v antlr.Token) { s.op = v }

func (s *ExprLogicalOrContext) GetLhs() IExpressionContext { return s.lhs }

func (s *ExprLogicalOrContext) GetRhs() IExpressionContext { return s.rhs }

func (s *ExprLogicalOrContext) SetLhs(v IExpressionContext) { s.lhs = v }

func (s *ExprLogicalOrContext) SetRhs(v IExpressionContext) { s.rhs = v }

func (s *ExprLogicalOrContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprLogicalOrContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *ExprLogicalOrContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ExprLogicalOrContext) OP_OR() antlr.TerminalNode {
	return s.GetToken(NumScriptParserOP_OR, 0)
}

func (s *ExprLogicalOrContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterExprLogicalOr(s)
	}
}

func (s *ExprLogicalOrContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitExprLogicalOr(s)
	}
}

type ExprVariableContext struct {
	*ExpressionContext
	var_ IVariableContext
}

func NewExprVariableContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExprVariableContext {
	var p = new(ExprVariableContext)

	p.ExpressionContext = NewEmptyExpressionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ExpressionContext))

	return p
}

func (s *ExprVariableContext) GetVar_() IVariableContext { return s.var_ }

func (s *ExprVariableContext) SetVar_(v IVariableContext) { s.var_ = v }

func (s *ExprVariableContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprVariableContext) Variable() IVariableContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IVariableContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IVariableContext)
}

func (s *ExprVariableContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterExprVariable(s)
	}
}

func (s *ExprVariableContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitExprVariable(s)
	}
}

type ExprMonetaryNewContext struct {
	*ExpressionContext
	mon IMonetaryContext
}

func NewExprMonetaryNewContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExprMonetaryNewContext {
	var p = new(ExprMonetaryNewContext)

	p.ExpressionContext = NewEmptyExpressionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ExpressionContext))

	return p
}

func (s *ExprMonetaryNewContext) GetMon() IMonetaryContext { return s.mon }

func (s *ExprMonetaryNewContext) SetMon(v IMonetaryContext) { s.mon = v }

func (s *ExprMonetaryNewContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprMonetaryNewContext) Monetary() IMonetaryContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IMonetaryContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IMonetaryContext)
}

func (s *ExprMonetaryNewContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterExprMonetaryNew(s)
	}
}

func (s *ExprMonetaryNewContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitExprMonetaryNew(s)
	}
}

type ExprEnclosedContext struct {
	*ExpressionContext
	expr IExpressionContext
}

func NewExprEnclosedContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExprEnclosedContext {
	var p = new(ExprEnclosedContext)

	p.ExpressionContext = NewEmptyExpressionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ExpressionContext))

	return p
}

func (s *ExprEnclosedContext) GetExpr() IExpressionContext { return s.expr }

func (s *ExprEnclosedContext) SetExpr(v IExpressionContext) { s.expr = v }

func (s *ExprEnclosedContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprEnclosedContext) LPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLPAREN, 0)
}

func (s *ExprEnclosedContext) RPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRPAREN, 0)
}

func (s *ExprEnclosedContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ExprEnclosedContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterExprEnclosed(s)
	}
}

func (s *ExprEnclosedContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitExprEnclosed(s)
	}
}

type ExprLogicalAndContext struct {
	*ExpressionContext
	lhs IExpressionContext
	op  antlr.Token
	rhs IExpressionContext
}

func NewExprLogicalAndContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *ExprLogicalAndContext {
	var p = new(ExprLogicalAndContext)

	p.ExpressionContext = NewEmptyExpressionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ExpressionContext))

	return p
}

func (s *ExprLogicalAndContext) GetOp() antlr.Token { return s.op }

func (s *ExprLogicalAndContext) SetOp(v antlr.Token) { s.op = v }

func (s *ExprLogicalAndContext) GetLhs() IExpressionContext { return s.lhs }

func (s *ExprLogicalAndContext) GetRhs() IExpressionContext { return s.rhs }

func (s *ExprLogicalAndContext) SetLhs(v IExpressionContext) { s.lhs = v }

func (s *ExprLogicalAndContext) SetRhs(v IExpressionContext) { s.rhs = v }

func (s *ExprLogicalAndContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprLogicalAndContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *ExprLogicalAndContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *ExprLogicalAndContext) OP_AND() antlr.TerminalNode {
	return s.GetToken(NumScriptParserOP_AND, 0)
}

func (s *ExprLogicalAndContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterExprLogicalAnd(s)
	}
}

func (s *ExprLogicalAndContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitExprLogicalAnd(s)
	}
}

func (p *NumScriptParser) Expression() (localctx IExpressionContext) {
	return p.expression(0)
}

func (p *NumScriptParser) expression(_p int) (localctx IExpressionContext) {
	this := p
	_ = this

	var _parentctx antlr.ParserRuleContext = p.GetParserRuleContext()
	_parentState := p.GetState()
	localctx = NewExpressionContext(p, p.GetParserRuleContext(), _parentState)
	var _prevctx IExpressionContext = localctx
	var _ antlr.ParserRuleContext = _prevctx // TODO: To prevent unused variable warning.
	_startState := 8
	p.EnterRecursionRule(localctx, 8, NumScriptParserRULE_expression, _p)
	var _la int

	defer func() {
		p.UnrollRecursionContexts(_parentctx)
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(75)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case NumScriptParserOP_NOT:
		localctx = NewExprLogicalNotContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx

		{
			p.SetState(66)
			p.Match(NumScriptParserOP_NOT)
		}
		{
			p.SetState(67)

			var _x = p.expression(8)

			localctx.(*ExprLogicalNotContext).lhs = _x
		}

	case NumScriptParserSTRING, NumScriptParserPORTION, NumScriptParserNUMBER, NumScriptParserACCOUNT, NumScriptParserASSET:
		localctx = NewExprLiteralContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(68)

			var _x = p.Literal()

			localctx.(*ExprLiteralContext).lit = _x
		}

	case NumScriptParserVARIABLE_NAME:
		localctx = NewExprVariableContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(69)

			var _x = p.Variable()

			localctx.(*ExprVariableContext).var_ = _x
		}

	case NumScriptParserLBRACK:
		localctx = NewExprMonetaryNewContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(70)

			var _x = p.Monetary()

			localctx.(*ExprMonetaryNewContext).mon = _x
		}

	case NumScriptParserLPAREN:
		localctx = NewExprEnclosedContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(71)
			p.Match(NumScriptParserLPAREN)
		}
		{
			p.SetState(72)

			var _x = p.expression(0)

			localctx.(*ExprEnclosedContext).expr = _x
		}
		{
			p.SetState(73)
			p.Match(NumScriptParserRPAREN)
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}
	p.GetParserRuleContext().SetStop(p.GetTokenStream().LT(-1))
	p.SetState(97)
	p.GetErrorHandler().Sync(p)
	_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 3, p.GetParserRuleContext())

	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			if p.GetParseListeners() != nil {
				p.TriggerExitRuleEvent()
			}
			_prevctx = localctx
			p.SetState(95)
			p.GetErrorHandler().Sync(p)
			switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 2, p.GetParserRuleContext()) {
			case 1:
				localctx = NewExprAddSubContext(p, NewExpressionContext(p, _parentctx, _parentState))
				localctx.(*ExprAddSubContext).lhs = _prevctx

				p.PushNewRecursionContext(localctx, _startState, NumScriptParserRULE_expression)
				p.SetState(77)

				if !(p.Precpred(p.GetParserRuleContext(), 10)) {
					panic(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 10)", ""))
				}
				{
					p.SetState(78)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*ExprAddSubContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !(_la == NumScriptParserOP_ADD || _la == NumScriptParserOP_SUB) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*ExprAddSubContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(79)

					var _x = p.expression(11)

					localctx.(*ExprAddSubContext).rhs = _x
				}

			case 2:
				localctx = NewExprArithmeticConditionContext(p, NewExpressionContext(p, _parentctx, _parentState))
				localctx.(*ExprArithmeticConditionContext).lhs = _prevctx

				p.PushNewRecursionContext(localctx, _startState, NumScriptParserRULE_expression)
				p.SetState(80)

				if !(p.Precpred(p.GetParserRuleContext(), 9)) {
					panic(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 9)", ""))
				}
				{
					p.SetState(81)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*ExprArithmeticConditionContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !(((_la)&-(0x1f+1)) == 0 && ((1<<uint(_la))&((1<<NumScriptParserOP_EQ)|(1<<NumScriptParserOP_NEQ)|(1<<NumScriptParserOP_LT)|(1<<NumScriptParserOP_LTE)|(1<<NumScriptParserOP_GT)|(1<<NumScriptParserOP_GTE))) != 0) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*ExprArithmeticConditionContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(82)

					var _x = p.expression(10)

					localctx.(*ExprArithmeticConditionContext).rhs = _x
				}

			case 3:
				localctx = NewExprLogicalAndContext(p, NewExpressionContext(p, _parentctx, _parentState))
				localctx.(*ExprLogicalAndContext).lhs = _prevctx

				p.PushNewRecursionContext(localctx, _startState, NumScriptParserRULE_expression)
				p.SetState(83)

				if !(p.Precpred(p.GetParserRuleContext(), 7)) {
					panic(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 7)", ""))
				}
				{
					p.SetState(84)

					var _m = p.Match(NumScriptParserOP_AND)

					localctx.(*ExprLogicalAndContext).op = _m
				}
				{
					p.SetState(85)

					var _x = p.expression(8)

					localctx.(*ExprLogicalAndContext).rhs = _x
				}

			case 4:
				localctx = NewExprLogicalOrContext(p, NewExpressionContext(p, _parentctx, _parentState))
				localctx.(*ExprLogicalOrContext).lhs = _prevctx

				p.PushNewRecursionContext(localctx, _startState, NumScriptParserRULE_expression)
				p.SetState(86)

				if !(p.Precpred(p.GetParserRuleContext(), 6)) {
					panic(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 6)", ""))
				}
				{
					p.SetState(87)

					var _m = p.Match(NumScriptParserOP_OR)

					localctx.(*ExprLogicalOrContext).op = _m
				}
				{
					p.SetState(88)

					var _x = p.expression(7)

					localctx.(*ExprLogicalOrContext).rhs = _x
				}

			case 5:
				localctx = NewExprTernaryContext(p, NewExpressionContext(p, _parentctx, _parentState))
				localctx.(*ExprTernaryContext).cond = _prevctx

				p.PushNewRecursionContext(localctx, _startState, NumScriptParserRULE_expression)
				p.SetState(89)

				if !(p.Precpred(p.GetParserRuleContext(), 2)) {
					panic(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 2)", ""))
				}
				{
					p.SetState(90)
					p.Match(NumScriptParserT__1)
				}
				{
					p.SetState(91)

					var _x = p.expression(0)

					localctx.(*ExprTernaryContext).ifTrue = _x
				}
				{
					p.SetState(92)
					p.Match(NumScriptParserT__2)
				}
				{
					p.SetState(93)

					var _x = p.expression(3)

					localctx.(*ExprTernaryContext).ifFalse = _x
				}

			}

		}
		p.SetState(99)
		p.GetErrorHandler().Sync(p)
		_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 3, p.GetParserRuleContext())
	}

	return localctx
}

// IAllotmentPortionContext is an interface to support dynamic dispatch.
type IAllotmentPortionContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsAllotmentPortionContext differentiates from other interfaces.
	IsAllotmentPortionContext()
}

type AllotmentPortionContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyAllotmentPortionContext() *AllotmentPortionContext {
	var p = new(AllotmentPortionContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_allotmentPortion
	return p
}

func (*AllotmentPortionContext) IsAllotmentPortionContext() {}

func NewAllotmentPortionContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *AllotmentPortionContext {
	var p = new(AllotmentPortionContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_allotmentPortion

	return p
}

func (s *AllotmentPortionContext) GetParser() antlr.Parser { return s.parser }

func (s *AllotmentPortionContext) CopyFrom(ctx *AllotmentPortionContext) {
	s.BaseParserRuleContext.CopyFrom(ctx.BaseParserRuleContext)
}

func (s *AllotmentPortionContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *AllotmentPortionContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type AllotmentPortionRemainingContext struct {
	*AllotmentPortionContext
}

func NewAllotmentPortionRemainingContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *AllotmentPortionRemainingContext {
	var p = new(AllotmentPortionRemainingContext)

	p.AllotmentPortionContext = NewEmptyAllotmentPortionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*AllotmentPortionContext))

	return p
}

func (s *AllotmentPortionRemainingContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *AllotmentPortionRemainingContext) REMAINING() antlr.TerminalNode {
	return s.GetToken(NumScriptParserREMAINING, 0)
}

func (s *AllotmentPortionRemainingContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterAllotmentPortionRemaining(s)
	}
}

func (s *AllotmentPortionRemainingContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitAllotmentPortionRemaining(s)
	}
}

type AllotmentPortionVarContext struct {
	*AllotmentPortionContext
	por IVariableContext
}

func NewAllotmentPortionVarContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *AllotmentPortionVarContext {
	var p = new(AllotmentPortionVarContext)

	p.AllotmentPortionContext = NewEmptyAllotmentPortionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*AllotmentPortionContext))

	return p
}

func (s *AllotmentPortionVarContext) GetPor() IVariableContext { return s.por }

func (s *AllotmentPortionVarContext) SetPor(v IVariableContext) { s.por = v }

func (s *AllotmentPortionVarContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *AllotmentPortionVarContext) Variable() IVariableContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IVariableContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IVariableContext)
}

func (s *AllotmentPortionVarContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterAllotmentPortionVar(s)
	}
}

func (s *AllotmentPortionVarContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitAllotmentPortionVar(s)
	}
}

type AllotmentPortionConstContext struct {
	*AllotmentPortionContext
}

func NewAllotmentPortionConstContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *AllotmentPortionConstContext {
	var p = new(AllotmentPortionConstContext)

	p.AllotmentPortionContext = NewEmptyAllotmentPortionContext()
	p.parser = parser
	p.CopyFrom(ctx.(*AllotmentPortionContext))

	return p
}

func (s *AllotmentPortionConstContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *AllotmentPortionConstContext) PORTION() antlr.TerminalNode {
	return s.GetToken(NumScriptParserPORTION, 0)
}

func (s *AllotmentPortionConstContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterAllotmentPortionConst(s)
	}
}

func (s *AllotmentPortionConstContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitAllotmentPortionConst(s)
	}
}

func (p *NumScriptParser) AllotmentPortion() (localctx IAllotmentPortionContext) {
	this := p
	_ = this

	localctx = NewAllotmentPortionContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 10, NumScriptParserRULE_allotmentPortion)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(103)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case NumScriptParserPORTION:
		localctx = NewAllotmentPortionConstContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(100)
			p.Match(NumScriptParserPORTION)
		}

	case NumScriptParserVARIABLE_NAME:
		localctx = NewAllotmentPortionVarContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(101)

			var _x = p.Variable()

			localctx.(*AllotmentPortionVarContext).por = _x
		}

	case NumScriptParserREMAINING:
		localctx = NewAllotmentPortionRemainingContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(102)
			p.Match(NumScriptParserREMAINING)
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// IDestinationInOrderContext is an interface to support dynamic dispatch.
type IDestinationInOrderContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Get_expression returns the _expression rule contexts.
	Get_expression() IExpressionContext

	// Get_keptOrDestination returns the _keptOrDestination rule contexts.
	Get_keptOrDestination() IKeptOrDestinationContext

	// GetRemainingDest returns the remainingDest rule contexts.
	GetRemainingDest() IKeptOrDestinationContext

	// Set_expression sets the _expression rule contexts.
	Set_expression(IExpressionContext)

	// Set_keptOrDestination sets the _keptOrDestination rule contexts.
	Set_keptOrDestination(IKeptOrDestinationContext)

	// SetRemainingDest sets the remainingDest rule contexts.
	SetRemainingDest(IKeptOrDestinationContext)

	// GetAmounts returns the amounts rule context list.
	GetAmounts() []IExpressionContext

	// GetDests returns the dests rule context list.
	GetDests() []IKeptOrDestinationContext

	// SetAmounts sets the amounts rule context list.
	SetAmounts([]IExpressionContext)

	// SetDests sets the dests rule context list.
	SetDests([]IKeptOrDestinationContext)

	// IsDestinationInOrderContext differentiates from other interfaces.
	IsDestinationInOrderContext()
}

type DestinationInOrderContext struct {
	*antlr.BaseParserRuleContext
	parser             antlr.Parser
	_expression        IExpressionContext
	amounts            []IExpressionContext
	_keptOrDestination IKeptOrDestinationContext
	dests              []IKeptOrDestinationContext
	remainingDest      IKeptOrDestinationContext
}

func NewEmptyDestinationInOrderContext() *DestinationInOrderContext {
	var p = new(DestinationInOrderContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_destinationInOrder
	return p
}

func (*DestinationInOrderContext) IsDestinationInOrderContext() {}

func NewDestinationInOrderContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DestinationInOrderContext {
	var p = new(DestinationInOrderContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_destinationInOrder

	return p
}

func (s *DestinationInOrderContext) GetParser() antlr.Parser { return s.parser }

func (s *DestinationInOrderContext) Get_expression() IExpressionContext { return s._expression }

func (s *DestinationInOrderContext) Get_keptOrDestination() IKeptOrDestinationContext {
	return s._keptOrDestination
}

func (s *DestinationInOrderContext) GetRemainingDest() IKeptOrDestinationContext {
	return s.remainingDest
}

func (s *DestinationInOrderContext) Set_expression(v IExpressionContext) { s._expression = v }

func (s *DestinationInOrderContext) Set_keptOrDestination(v IKeptOrDestinationContext) {
	s._keptOrDestination = v
}

func (s *DestinationInOrderContext) SetRemainingDest(v IKeptOrDestinationContext) {
	s.remainingDest = v
}

func (s *DestinationInOrderContext) GetAmounts() []IExpressionContext { return s.amounts }

func (s *DestinationInOrderContext) GetDests() []IKeptOrDestinationContext { return s.dests }

func (s *DestinationInOrderContext) SetAmounts(v []IExpressionContext) { s.amounts = v }

func (s *DestinationInOrderContext) SetDests(v []IKeptOrDestinationContext) { s.dests = v }

func (s *DestinationInOrderContext) LBRACE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLBRACE, 0)
}

func (s *DestinationInOrderContext) AllNEWLINE() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserNEWLINE)
}

func (s *DestinationInOrderContext) NEWLINE(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserNEWLINE, i)
}

func (s *DestinationInOrderContext) REMAINING() antlr.TerminalNode {
	return s.GetToken(NumScriptParserREMAINING, 0)
}

func (s *DestinationInOrderContext) RBRACE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRBRACE, 0)
}

func (s *DestinationInOrderContext) AllKeptOrDestination() []IKeptOrDestinationContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IKeptOrDestinationContext); ok {
			len++
		}
	}

	tst := make([]IKeptOrDestinationContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IKeptOrDestinationContext); ok {
			tst[i] = t.(IKeptOrDestinationContext)
			i++
		}
	}

	return tst
}

func (s *DestinationInOrderContext) KeptOrDestination(i int) IKeptOrDestinationContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IKeptOrDestinationContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IKeptOrDestinationContext)
}

func (s *DestinationInOrderContext) AllMAX() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserMAX)
}

func (s *DestinationInOrderContext) MAX(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserMAX, i)
}

func (s *DestinationInOrderContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *DestinationInOrderContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *DestinationInOrderContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DestinationInOrderContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *DestinationInOrderContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterDestinationInOrder(s)
	}
}

func (s *DestinationInOrderContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitDestinationInOrder(s)
	}
}

func (p *NumScriptParser) DestinationInOrder() (localctx IDestinationInOrderContext) {
	this := p
	_ = this

	localctx = NewDestinationInOrderContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 12, NumScriptParserRULE_destinationInOrder)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(105)
		p.Match(NumScriptParserLBRACE)
	}
	{
		p.SetState(106)
		p.Match(NumScriptParserNEWLINE)
	}
	p.SetState(112)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for ok := true; ok; ok = _la == NumScriptParserMAX {
		{
			p.SetState(107)
			p.Match(NumScriptParserMAX)
		}
		{
			p.SetState(108)

			var _x = p.expression(0)

			localctx.(*DestinationInOrderContext)._expression = _x
		}
		localctx.(*DestinationInOrderContext).amounts = append(localctx.(*DestinationInOrderContext).amounts, localctx.(*DestinationInOrderContext)._expression)
		{
			p.SetState(109)

			var _x = p.KeptOrDestination()

			localctx.(*DestinationInOrderContext)._keptOrDestination = _x
		}
		localctx.(*DestinationInOrderContext).dests = append(localctx.(*DestinationInOrderContext).dests, localctx.(*DestinationInOrderContext)._keptOrDestination)
		{
			p.SetState(110)
			p.Match(NumScriptParserNEWLINE)
		}

		p.SetState(114)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(116)
		p.Match(NumScriptParserREMAINING)
	}
	{
		p.SetState(117)

		var _x = p.KeptOrDestination()

		localctx.(*DestinationInOrderContext).remainingDest = _x
	}
	{
		p.SetState(118)
		p.Match(NumScriptParserNEWLINE)
	}
	{
		p.SetState(119)
		p.Match(NumScriptParserRBRACE)
	}

	return localctx
}

// IDestinationAllotmentContext is an interface to support dynamic dispatch.
type IDestinationAllotmentContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Get_allotmentPortion returns the _allotmentPortion rule contexts.
	Get_allotmentPortion() IAllotmentPortionContext

	// Get_keptOrDestination returns the _keptOrDestination rule contexts.
	Get_keptOrDestination() IKeptOrDestinationContext

	// Set_allotmentPortion sets the _allotmentPortion rule contexts.
	Set_allotmentPortion(IAllotmentPortionContext)

	// Set_keptOrDestination sets the _keptOrDestination rule contexts.
	Set_keptOrDestination(IKeptOrDestinationContext)

	// GetPortions returns the portions rule context list.
	GetPortions() []IAllotmentPortionContext

	// GetDests returns the dests rule context list.
	GetDests() []IKeptOrDestinationContext

	// SetPortions sets the portions rule context list.
	SetPortions([]IAllotmentPortionContext)

	// SetDests sets the dests rule context list.
	SetDests([]IKeptOrDestinationContext)

	// IsDestinationAllotmentContext differentiates from other interfaces.
	IsDestinationAllotmentContext()
}

type DestinationAllotmentContext struct {
	*antlr.BaseParserRuleContext
	parser             antlr.Parser
	_allotmentPortion  IAllotmentPortionContext
	portions           []IAllotmentPortionContext
	_keptOrDestination IKeptOrDestinationContext
	dests              []IKeptOrDestinationContext
}

func NewEmptyDestinationAllotmentContext() *DestinationAllotmentContext {
	var p = new(DestinationAllotmentContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_destinationAllotment
	return p
}

func (*DestinationAllotmentContext) IsDestinationAllotmentContext() {}

func NewDestinationAllotmentContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DestinationAllotmentContext {
	var p = new(DestinationAllotmentContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_destinationAllotment

	return p
}

func (s *DestinationAllotmentContext) GetParser() antlr.Parser { return s.parser }

func (s *DestinationAllotmentContext) Get_allotmentPortion() IAllotmentPortionContext {
	return s._allotmentPortion
}

func (s *DestinationAllotmentContext) Get_keptOrDestination() IKeptOrDestinationContext {
	return s._keptOrDestination
}

func (s *DestinationAllotmentContext) Set_allotmentPortion(v IAllotmentPortionContext) {
	s._allotmentPortion = v
}

func (s *DestinationAllotmentContext) Set_keptOrDestination(v IKeptOrDestinationContext) {
	s._keptOrDestination = v
}

func (s *DestinationAllotmentContext) GetPortions() []IAllotmentPortionContext { return s.portions }

func (s *DestinationAllotmentContext) GetDests() []IKeptOrDestinationContext { return s.dests }

func (s *DestinationAllotmentContext) SetPortions(v []IAllotmentPortionContext) { s.portions = v }

func (s *DestinationAllotmentContext) SetDests(v []IKeptOrDestinationContext) { s.dests = v }

func (s *DestinationAllotmentContext) LBRACE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLBRACE, 0)
}

func (s *DestinationAllotmentContext) AllNEWLINE() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserNEWLINE)
}

func (s *DestinationAllotmentContext) NEWLINE(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserNEWLINE, i)
}

func (s *DestinationAllotmentContext) RBRACE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRBRACE, 0)
}

func (s *DestinationAllotmentContext) AllAllotmentPortion() []IAllotmentPortionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IAllotmentPortionContext); ok {
			len++
		}
	}

	tst := make([]IAllotmentPortionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IAllotmentPortionContext); ok {
			tst[i] = t.(IAllotmentPortionContext)
			i++
		}
	}

	return tst
}

func (s *DestinationAllotmentContext) AllotmentPortion(i int) IAllotmentPortionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IAllotmentPortionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IAllotmentPortionContext)
}

func (s *DestinationAllotmentContext) AllKeptOrDestination() []IKeptOrDestinationContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IKeptOrDestinationContext); ok {
			len++
		}
	}

	tst := make([]IKeptOrDestinationContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IKeptOrDestinationContext); ok {
			tst[i] = t.(IKeptOrDestinationContext)
			i++
		}
	}

	return tst
}

func (s *DestinationAllotmentContext) KeptOrDestination(i int) IKeptOrDestinationContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IKeptOrDestinationContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IKeptOrDestinationContext)
}

func (s *DestinationAllotmentContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DestinationAllotmentContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *DestinationAllotmentContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterDestinationAllotment(s)
	}
}

func (s *DestinationAllotmentContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitDestinationAllotment(s)
	}
}

func (p *NumScriptParser) DestinationAllotment() (localctx IDestinationAllotmentContext) {
	this := p
	_ = this

	localctx = NewDestinationAllotmentContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 14, NumScriptParserRULE_destinationAllotment)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(121)
		p.Match(NumScriptParserLBRACE)
	}
	{
		p.SetState(122)
		p.Match(NumScriptParserNEWLINE)
	}
	p.SetState(127)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for ok := true; ok; ok = (((_la-50)&-(0x1f+1)) == 0 && ((1<<uint((_la-50)))&((1<<(NumScriptParserPORTION-50))|(1<<(NumScriptParserREMAINING-50))|(1<<(NumScriptParserVARIABLE_NAME-50)))) != 0) {
		{
			p.SetState(123)

			var _x = p.AllotmentPortion()

			localctx.(*DestinationAllotmentContext)._allotmentPortion = _x
		}
		localctx.(*DestinationAllotmentContext).portions = append(localctx.(*DestinationAllotmentContext).portions, localctx.(*DestinationAllotmentContext)._allotmentPortion)
		{
			p.SetState(124)

			var _x = p.KeptOrDestination()

			localctx.(*DestinationAllotmentContext)._keptOrDestination = _x
		}
		localctx.(*DestinationAllotmentContext).dests = append(localctx.(*DestinationAllotmentContext).dests, localctx.(*DestinationAllotmentContext)._keptOrDestination)
		{
			p.SetState(125)
			p.Match(NumScriptParserNEWLINE)
		}

		p.SetState(129)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(131)
		p.Match(NumScriptParserRBRACE)
	}

	return localctx
}

// IKeptOrDestinationContext is an interface to support dynamic dispatch.
type IKeptOrDestinationContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsKeptOrDestinationContext differentiates from other interfaces.
	IsKeptOrDestinationContext()
}

type KeptOrDestinationContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyKeptOrDestinationContext() *KeptOrDestinationContext {
	var p = new(KeptOrDestinationContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_keptOrDestination
	return p
}

func (*KeptOrDestinationContext) IsKeptOrDestinationContext() {}

func NewKeptOrDestinationContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *KeptOrDestinationContext {
	var p = new(KeptOrDestinationContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_keptOrDestination

	return p
}

func (s *KeptOrDestinationContext) GetParser() antlr.Parser { return s.parser }

func (s *KeptOrDestinationContext) CopyFrom(ctx *KeptOrDestinationContext) {
	s.BaseParserRuleContext.CopyFrom(ctx.BaseParserRuleContext)
}

func (s *KeptOrDestinationContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *KeptOrDestinationContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type IsKeptContext struct {
	*KeptOrDestinationContext
}

func NewIsKeptContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IsKeptContext {
	var p = new(IsKeptContext)

	p.KeptOrDestinationContext = NewEmptyKeptOrDestinationContext()
	p.parser = parser
	p.CopyFrom(ctx.(*KeptOrDestinationContext))

	return p
}

func (s *IsKeptContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IsKeptContext) KEPT() antlr.TerminalNode {
	return s.GetToken(NumScriptParserKEPT, 0)
}

func (s *IsKeptContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterIsKept(s)
	}
}

func (s *IsKeptContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitIsKept(s)
	}
}

type IsDestinationContext struct {
	*KeptOrDestinationContext
}

func NewIsDestinationContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IsDestinationContext {
	var p = new(IsDestinationContext)

	p.KeptOrDestinationContext = NewEmptyKeptOrDestinationContext()
	p.parser = parser
	p.CopyFrom(ctx.(*KeptOrDestinationContext))

	return p
}

func (s *IsDestinationContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IsDestinationContext) TO() antlr.TerminalNode {
	return s.GetToken(NumScriptParserTO, 0)
}

func (s *IsDestinationContext) Destination() IDestinationContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDestinationContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDestinationContext)
}

func (s *IsDestinationContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterIsDestination(s)
	}
}

func (s *IsDestinationContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitIsDestination(s)
	}
}

func (p *NumScriptParser) KeptOrDestination() (localctx IKeptOrDestinationContext) {
	this := p
	_ = this

	localctx = NewKeptOrDestinationContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 16, NumScriptParserRULE_keptOrDestination)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(136)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case NumScriptParserTO:
		localctx = NewIsDestinationContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(133)
			p.Match(NumScriptParserTO)
		}
		{
			p.SetState(134)
			p.Destination()
		}

	case NumScriptParserKEPT:
		localctx = NewIsKeptContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(135)
			p.Match(NumScriptParserKEPT)
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// IDestinationContext is an interface to support dynamic dispatch.
type IDestinationContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsDestinationContext differentiates from other interfaces.
	IsDestinationContext()
}

type DestinationContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyDestinationContext() *DestinationContext {
	var p = new(DestinationContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_destination
	return p
}

func (*DestinationContext) IsDestinationContext() {}

func NewDestinationContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *DestinationContext {
	var p = new(DestinationContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_destination

	return p
}

func (s *DestinationContext) GetParser() antlr.Parser { return s.parser }

func (s *DestinationContext) CopyFrom(ctx *DestinationContext) {
	s.BaseParserRuleContext.CopyFrom(ctx.BaseParserRuleContext)
}

func (s *DestinationContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DestinationContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type DestAccountContext struct {
	*DestinationContext
}

func NewDestAccountContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *DestAccountContext {
	var p = new(DestAccountContext)

	p.DestinationContext = NewEmptyDestinationContext()
	p.parser = parser
	p.CopyFrom(ctx.(*DestinationContext))

	return p
}

func (s *DestAccountContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DestAccountContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *DestAccountContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterDestAccount(s)
	}
}

func (s *DestAccountContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitDestAccount(s)
	}
}

type DestAllotmentContext struct {
	*DestinationContext
}

func NewDestAllotmentContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *DestAllotmentContext {
	var p = new(DestAllotmentContext)

	p.DestinationContext = NewEmptyDestinationContext()
	p.parser = parser
	p.CopyFrom(ctx.(*DestinationContext))

	return p
}

func (s *DestAllotmentContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DestAllotmentContext) DestinationAllotment() IDestinationAllotmentContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDestinationAllotmentContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDestinationAllotmentContext)
}

func (s *DestAllotmentContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterDestAllotment(s)
	}
}

func (s *DestAllotmentContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitDestAllotment(s)
	}
}

type DestInOrderContext struct {
	*DestinationContext
}

func NewDestInOrderContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *DestInOrderContext {
	var p = new(DestInOrderContext)

	p.DestinationContext = NewEmptyDestinationContext()
	p.parser = parser
	p.CopyFrom(ctx.(*DestinationContext))

	return p
}

func (s *DestInOrderContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DestInOrderContext) DestinationInOrder() IDestinationInOrderContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDestinationInOrderContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDestinationInOrderContext)
}

func (s *DestInOrderContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterDestInOrder(s)
	}
}

func (s *DestInOrderContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitDestInOrder(s)
	}
}

func (p *NumScriptParser) Destination() (localctx IDestinationContext) {
	this := p
	_ = this

	localctx = NewDestinationContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 18, NumScriptParserRULE_destination)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(141)
	p.GetErrorHandler().Sync(p)
	switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 8, p.GetParserRuleContext()) {
	case 1:
		localctx = NewDestAccountContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(138)
			p.expression(0)
		}

	case 2:
		localctx = NewDestInOrderContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(139)
			p.DestinationInOrder()
		}

	case 3:
		localctx = NewDestAllotmentContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(140)
			p.DestinationAllotment()
		}

	}

	return localctx
}

// ISourceAccountOverdraftContext is an interface to support dynamic dispatch.
type ISourceAccountOverdraftContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsSourceAccountOverdraftContext differentiates from other interfaces.
	IsSourceAccountOverdraftContext()
}

type SourceAccountOverdraftContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySourceAccountOverdraftContext() *SourceAccountOverdraftContext {
	var p = new(SourceAccountOverdraftContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_sourceAccountOverdraft
	return p
}

func (*SourceAccountOverdraftContext) IsSourceAccountOverdraftContext() {}

func NewSourceAccountOverdraftContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SourceAccountOverdraftContext {
	var p = new(SourceAccountOverdraftContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_sourceAccountOverdraft

	return p
}

func (s *SourceAccountOverdraftContext) GetParser() antlr.Parser { return s.parser }

func (s *SourceAccountOverdraftContext) CopyFrom(ctx *SourceAccountOverdraftContext) {
	s.BaseParserRuleContext.CopyFrom(ctx.BaseParserRuleContext)
}

func (s *SourceAccountOverdraftContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SourceAccountOverdraftContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type SrcAccountOverdraftSpecificContext struct {
	*SourceAccountOverdraftContext
	specific IExpressionContext
}

func NewSrcAccountOverdraftSpecificContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SrcAccountOverdraftSpecificContext {
	var p = new(SrcAccountOverdraftSpecificContext)

	p.SourceAccountOverdraftContext = NewEmptySourceAccountOverdraftContext()
	p.parser = parser
	p.CopyFrom(ctx.(*SourceAccountOverdraftContext))

	return p
}

func (s *SrcAccountOverdraftSpecificContext) GetSpecific() IExpressionContext { return s.specific }

func (s *SrcAccountOverdraftSpecificContext) SetSpecific(v IExpressionContext) { s.specific = v }

func (s *SrcAccountOverdraftSpecificContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SrcAccountOverdraftSpecificContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *SrcAccountOverdraftSpecificContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSrcAccountOverdraftSpecific(s)
	}
}

func (s *SrcAccountOverdraftSpecificContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSrcAccountOverdraftSpecific(s)
	}
}

type SrcAccountOverdraftUnboundedContext struct {
	*SourceAccountOverdraftContext
}

func NewSrcAccountOverdraftUnboundedContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SrcAccountOverdraftUnboundedContext {
	var p = new(SrcAccountOverdraftUnboundedContext)

	p.SourceAccountOverdraftContext = NewEmptySourceAccountOverdraftContext()
	p.parser = parser
	p.CopyFrom(ctx.(*SourceAccountOverdraftContext))

	return p
}

func (s *SrcAccountOverdraftUnboundedContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SrcAccountOverdraftUnboundedContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSrcAccountOverdraftUnbounded(s)
	}
}

func (s *SrcAccountOverdraftUnboundedContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSrcAccountOverdraftUnbounded(s)
	}
}

func (p *NumScriptParser) SourceAccountOverdraft() (localctx ISourceAccountOverdraftContext) {
	this := p
	_ = this

	localctx = NewSourceAccountOverdraftContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 20, NumScriptParserRULE_sourceAccountOverdraft)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(146)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case NumScriptParserT__3:
		localctx = NewSrcAccountOverdraftSpecificContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(143)
			p.Match(NumScriptParserT__3)
		}
		{
			p.SetState(144)

			var _x = p.expression(0)

			localctx.(*SrcAccountOverdraftSpecificContext).specific = _x
		}

	case NumScriptParserT__4:
		localctx = NewSrcAccountOverdraftUnboundedContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(145)
			p.Match(NumScriptParserT__4)
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// ISourceAccountContext is an interface to support dynamic dispatch.
type ISourceAccountContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// GetAccount returns the account rule contexts.
	GetAccount() IExpressionContext

	// GetOverdraft returns the overdraft rule contexts.
	GetOverdraft() ISourceAccountOverdraftContext

	// SetAccount sets the account rule contexts.
	SetAccount(IExpressionContext)

	// SetOverdraft sets the overdraft rule contexts.
	SetOverdraft(ISourceAccountOverdraftContext)

	// IsSourceAccountContext differentiates from other interfaces.
	IsSourceAccountContext()
}

type SourceAccountContext struct {
	*antlr.BaseParserRuleContext
	parser    antlr.Parser
	account   IExpressionContext
	overdraft ISourceAccountOverdraftContext
}

func NewEmptySourceAccountContext() *SourceAccountContext {
	var p = new(SourceAccountContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_sourceAccount
	return p
}

func (*SourceAccountContext) IsSourceAccountContext() {}

func NewSourceAccountContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SourceAccountContext {
	var p = new(SourceAccountContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_sourceAccount

	return p
}

func (s *SourceAccountContext) GetParser() antlr.Parser { return s.parser }

func (s *SourceAccountContext) GetAccount() IExpressionContext { return s.account }

func (s *SourceAccountContext) GetOverdraft() ISourceAccountOverdraftContext { return s.overdraft }

func (s *SourceAccountContext) SetAccount(v IExpressionContext) { s.account = v }

func (s *SourceAccountContext) SetOverdraft(v ISourceAccountOverdraftContext) { s.overdraft = v }

func (s *SourceAccountContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *SourceAccountContext) SourceAccountOverdraft() ISourceAccountOverdraftContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISourceAccountOverdraftContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISourceAccountOverdraftContext)
}

func (s *SourceAccountContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SourceAccountContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SourceAccountContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSourceAccount(s)
	}
}

func (s *SourceAccountContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSourceAccount(s)
	}
}

func (p *NumScriptParser) SourceAccount() (localctx ISourceAccountContext) {
	this := p
	_ = this

	localctx = NewSourceAccountContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 22, NumScriptParserRULE_sourceAccount)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(148)

		var _x = p.expression(0)

		localctx.(*SourceAccountContext).account = _x
	}
	p.SetState(150)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	if _la == NumScriptParserT__3 || _la == NumScriptParserT__4 {
		{
			p.SetState(149)

			var _x = p.SourceAccountOverdraft()

			localctx.(*SourceAccountContext).overdraft = _x
		}

	}

	return localctx
}

// ISourceInOrderContext is an interface to support dynamic dispatch.
type ISourceInOrderContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Get_source returns the _source rule contexts.
	Get_source() ISourceContext

	// Set_source sets the _source rule contexts.
	Set_source(ISourceContext)

	// GetSources returns the sources rule context list.
	GetSources() []ISourceContext

	// SetSources sets the sources rule context list.
	SetSources([]ISourceContext)

	// IsSourceInOrderContext differentiates from other interfaces.
	IsSourceInOrderContext()
}

type SourceInOrderContext struct {
	*antlr.BaseParserRuleContext
	parser  antlr.Parser
	_source ISourceContext
	sources []ISourceContext
}

func NewEmptySourceInOrderContext() *SourceInOrderContext {
	var p = new(SourceInOrderContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_sourceInOrder
	return p
}

func (*SourceInOrderContext) IsSourceInOrderContext() {}

func NewSourceInOrderContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SourceInOrderContext {
	var p = new(SourceInOrderContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_sourceInOrder

	return p
}

func (s *SourceInOrderContext) GetParser() antlr.Parser { return s.parser }

func (s *SourceInOrderContext) Get_source() ISourceContext { return s._source }

func (s *SourceInOrderContext) Set_source(v ISourceContext) { s._source = v }

func (s *SourceInOrderContext) GetSources() []ISourceContext { return s.sources }

func (s *SourceInOrderContext) SetSources(v []ISourceContext) { s.sources = v }

func (s *SourceInOrderContext) LBRACE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLBRACE, 0)
}

func (s *SourceInOrderContext) AllNEWLINE() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserNEWLINE)
}

func (s *SourceInOrderContext) NEWLINE(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserNEWLINE, i)
}

func (s *SourceInOrderContext) RBRACE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRBRACE, 0)
}

func (s *SourceInOrderContext) AllSource() []ISourceContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ISourceContext); ok {
			len++
		}
	}

	tst := make([]ISourceContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ISourceContext); ok {
			tst[i] = t.(ISourceContext)
			i++
		}
	}

	return tst
}

func (s *SourceInOrderContext) Source(i int) ISourceContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISourceContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISourceContext)
}

func (s *SourceInOrderContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SourceInOrderContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SourceInOrderContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSourceInOrder(s)
	}
}

func (s *SourceInOrderContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSourceInOrder(s)
	}
}

func (p *NumScriptParser) SourceInOrder() (localctx ISourceInOrderContext) {
	this := p
	_ = this

	localctx = NewSourceInOrderContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 24, NumScriptParserRULE_sourceInOrder)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(152)
		p.Match(NumScriptParserLBRACE)
	}
	{
		p.SetState(153)
		p.Match(NumScriptParserNEWLINE)
	}
	p.SetState(157)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for ok := true; ok; ok = _la == NumScriptParserMAX || (((_la-32)&-(0x1f+1)) == 0 && ((1<<uint((_la-32)))&((1<<(NumScriptParserOP_NOT-32))|(1<<(NumScriptParserLPAREN-32))|(1<<(NumScriptParserLBRACK-32))|(1<<(NumScriptParserLBRACE-32))|(1<<(NumScriptParserSTRING-32))|(1<<(NumScriptParserPORTION-32))|(1<<(NumScriptParserNUMBER-32))|(1<<(NumScriptParserVARIABLE_NAME-32))|(1<<(NumScriptParserACCOUNT-32))|(1<<(NumScriptParserASSET-32)))) != 0) {
		{
			p.SetState(154)

			var _x = p.Source()

			localctx.(*SourceInOrderContext)._source = _x
		}
		localctx.(*SourceInOrderContext).sources = append(localctx.(*SourceInOrderContext).sources, localctx.(*SourceInOrderContext)._source)
		{
			p.SetState(155)
			p.Match(NumScriptParserNEWLINE)
		}

		p.SetState(159)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(161)
		p.Match(NumScriptParserRBRACE)
	}

	return localctx
}

// ISourceMaxedContext is an interface to support dynamic dispatch.
type ISourceMaxedContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// GetMax returns the max rule contexts.
	GetMax() IExpressionContext

	// GetSrc returns the src rule contexts.
	GetSrc() ISourceContext

	// SetMax sets the max rule contexts.
	SetMax(IExpressionContext)

	// SetSrc sets the src rule contexts.
	SetSrc(ISourceContext)

	// IsSourceMaxedContext differentiates from other interfaces.
	IsSourceMaxedContext()
}

type SourceMaxedContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
	max    IExpressionContext
	src    ISourceContext
}

func NewEmptySourceMaxedContext() *SourceMaxedContext {
	var p = new(SourceMaxedContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_sourceMaxed
	return p
}

func (*SourceMaxedContext) IsSourceMaxedContext() {}

func NewSourceMaxedContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SourceMaxedContext {
	var p = new(SourceMaxedContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_sourceMaxed

	return p
}

func (s *SourceMaxedContext) GetParser() antlr.Parser { return s.parser }

func (s *SourceMaxedContext) GetMax() IExpressionContext { return s.max }

func (s *SourceMaxedContext) GetSrc() ISourceContext { return s.src }

func (s *SourceMaxedContext) SetMax(v IExpressionContext) { s.max = v }

func (s *SourceMaxedContext) SetSrc(v ISourceContext) { s.src = v }

func (s *SourceMaxedContext) MAX() antlr.TerminalNode {
	return s.GetToken(NumScriptParserMAX, 0)
}

func (s *SourceMaxedContext) FROM() antlr.TerminalNode {
	return s.GetToken(NumScriptParserFROM, 0)
}

func (s *SourceMaxedContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *SourceMaxedContext) Source() ISourceContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISourceContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISourceContext)
}

func (s *SourceMaxedContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SourceMaxedContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SourceMaxedContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSourceMaxed(s)
	}
}

func (s *SourceMaxedContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSourceMaxed(s)
	}
}

func (p *NumScriptParser) SourceMaxed() (localctx ISourceMaxedContext) {
	this := p
	_ = this

	localctx = NewSourceMaxedContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 26, NumScriptParserRULE_sourceMaxed)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(163)
		p.Match(NumScriptParserMAX)
	}
	{
		p.SetState(164)

		var _x = p.expression(0)

		localctx.(*SourceMaxedContext).max = _x
	}
	{
		p.SetState(165)
		p.Match(NumScriptParserFROM)
	}
	{
		p.SetState(166)

		var _x = p.Source()

		localctx.(*SourceMaxedContext).src = _x
	}

	return localctx
}

// ISourceContext is an interface to support dynamic dispatch.
type ISourceContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsSourceContext differentiates from other interfaces.
	IsSourceContext()
}

type SourceContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptySourceContext() *SourceContext {
	var p = new(SourceContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_source
	return p
}

func (*SourceContext) IsSourceContext() {}

func NewSourceContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SourceContext {
	var p = new(SourceContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_source

	return p
}

func (s *SourceContext) GetParser() antlr.Parser { return s.parser }

func (s *SourceContext) CopyFrom(ctx *SourceContext) {
	s.BaseParserRuleContext.CopyFrom(ctx.BaseParserRuleContext)
}

func (s *SourceContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SourceContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type SrcAccountContext struct {
	*SourceContext
}

func NewSrcAccountContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SrcAccountContext {
	var p = new(SrcAccountContext)

	p.SourceContext = NewEmptySourceContext()
	p.parser = parser
	p.CopyFrom(ctx.(*SourceContext))

	return p
}

func (s *SrcAccountContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SrcAccountContext) SourceAccount() ISourceAccountContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISourceAccountContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISourceAccountContext)
}

func (s *SrcAccountContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSrcAccount(s)
	}
}

func (s *SrcAccountContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSrcAccount(s)
	}
}

type SrcMaxedContext struct {
	*SourceContext
}

func NewSrcMaxedContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SrcMaxedContext {
	var p = new(SrcMaxedContext)

	p.SourceContext = NewEmptySourceContext()
	p.parser = parser
	p.CopyFrom(ctx.(*SourceContext))

	return p
}

func (s *SrcMaxedContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SrcMaxedContext) SourceMaxed() ISourceMaxedContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISourceMaxedContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISourceMaxedContext)
}

func (s *SrcMaxedContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSrcMaxed(s)
	}
}

func (s *SrcMaxedContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSrcMaxed(s)
	}
}

type SrcInOrderContext struct {
	*SourceContext
}

func NewSrcInOrderContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SrcInOrderContext {
	var p = new(SrcInOrderContext)

	p.SourceContext = NewEmptySourceContext()
	p.parser = parser
	p.CopyFrom(ctx.(*SourceContext))

	return p
}

func (s *SrcInOrderContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SrcInOrderContext) SourceInOrder() ISourceInOrderContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISourceInOrderContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISourceInOrderContext)
}

func (s *SrcInOrderContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSrcInOrder(s)
	}
}

func (s *SrcInOrderContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSrcInOrder(s)
	}
}

func (p *NumScriptParser) Source() (localctx ISourceContext) {
	this := p
	_ = this

	localctx = NewSourceContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 28, NumScriptParserRULE_source)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(171)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case NumScriptParserOP_NOT, NumScriptParserLPAREN, NumScriptParserLBRACK, NumScriptParserSTRING, NumScriptParserPORTION, NumScriptParserNUMBER, NumScriptParserVARIABLE_NAME, NumScriptParserACCOUNT, NumScriptParserASSET:
		localctx = NewSrcAccountContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(168)
			p.SourceAccount()
		}

	case NumScriptParserMAX:
		localctx = NewSrcMaxedContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(169)
			p.SourceMaxed()
		}

	case NumScriptParserLBRACE:
		localctx = NewSrcInOrderContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(170)
			p.SourceInOrder()
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// ISourceAllotmentContext is an interface to support dynamic dispatch.
type ISourceAllotmentContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Get_allotmentPortion returns the _allotmentPortion rule contexts.
	Get_allotmentPortion() IAllotmentPortionContext

	// Get_source returns the _source rule contexts.
	Get_source() ISourceContext

	// Set_allotmentPortion sets the _allotmentPortion rule contexts.
	Set_allotmentPortion(IAllotmentPortionContext)

	// Set_source sets the _source rule contexts.
	Set_source(ISourceContext)

	// GetPortions returns the portions rule context list.
	GetPortions() []IAllotmentPortionContext

	// GetSources returns the sources rule context list.
	GetSources() []ISourceContext

	// SetPortions sets the portions rule context list.
	SetPortions([]IAllotmentPortionContext)

	// SetSources sets the sources rule context list.
	SetSources([]ISourceContext)

	// IsSourceAllotmentContext differentiates from other interfaces.
	IsSourceAllotmentContext()
}

type SourceAllotmentContext struct {
	*antlr.BaseParserRuleContext
	parser            antlr.Parser
	_allotmentPortion IAllotmentPortionContext
	portions          []IAllotmentPortionContext
	_source           ISourceContext
	sources           []ISourceContext
}

func NewEmptySourceAllotmentContext() *SourceAllotmentContext {
	var p = new(SourceAllotmentContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_sourceAllotment
	return p
}

func (*SourceAllotmentContext) IsSourceAllotmentContext() {}

func NewSourceAllotmentContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *SourceAllotmentContext {
	var p = new(SourceAllotmentContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_sourceAllotment

	return p
}

func (s *SourceAllotmentContext) GetParser() antlr.Parser { return s.parser }

func (s *SourceAllotmentContext) Get_allotmentPortion() IAllotmentPortionContext {
	return s._allotmentPortion
}

func (s *SourceAllotmentContext) Get_source() ISourceContext { return s._source }

func (s *SourceAllotmentContext) Set_allotmentPortion(v IAllotmentPortionContext) {
	s._allotmentPortion = v
}

func (s *SourceAllotmentContext) Set_source(v ISourceContext) { s._source = v }

func (s *SourceAllotmentContext) GetPortions() []IAllotmentPortionContext { return s.portions }

func (s *SourceAllotmentContext) GetSources() []ISourceContext { return s.sources }

func (s *SourceAllotmentContext) SetPortions(v []IAllotmentPortionContext) { s.portions = v }

func (s *SourceAllotmentContext) SetSources(v []ISourceContext) { s.sources = v }

func (s *SourceAllotmentContext) LBRACE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLBRACE, 0)
}

func (s *SourceAllotmentContext) AllNEWLINE() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserNEWLINE)
}

func (s *SourceAllotmentContext) NEWLINE(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserNEWLINE, i)
}

func (s *SourceAllotmentContext) RBRACE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRBRACE, 0)
}

func (s *SourceAllotmentContext) AllFROM() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserFROM)
}

func (s *SourceAllotmentContext) FROM(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserFROM, i)
}

func (s *SourceAllotmentContext) AllAllotmentPortion() []IAllotmentPortionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IAllotmentPortionContext); ok {
			len++
		}
	}

	tst := make([]IAllotmentPortionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IAllotmentPortionContext); ok {
			tst[i] = t.(IAllotmentPortionContext)
			i++
		}
	}

	return tst
}

func (s *SourceAllotmentContext) AllotmentPortion(i int) IAllotmentPortionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IAllotmentPortionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IAllotmentPortionContext)
}

func (s *SourceAllotmentContext) AllSource() []ISourceContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(ISourceContext); ok {
			len++
		}
	}

	tst := make([]ISourceContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(ISourceContext); ok {
			tst[i] = t.(ISourceContext)
			i++
		}
	}

	return tst
}

func (s *SourceAllotmentContext) Source(i int) ISourceContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISourceContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISourceContext)
}

func (s *SourceAllotmentContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SourceAllotmentContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *SourceAllotmentContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSourceAllotment(s)
	}
}

func (s *SourceAllotmentContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSourceAllotment(s)
	}
}

func (p *NumScriptParser) SourceAllotment() (localctx ISourceAllotmentContext) {
	this := p
	_ = this

	localctx = NewSourceAllotmentContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 30, NumScriptParserRULE_sourceAllotment)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(173)
		p.Match(NumScriptParserLBRACE)
	}
	{
		p.SetState(174)
		p.Match(NumScriptParserNEWLINE)
	}
	p.SetState(180)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for ok := true; ok; ok = (((_la-50)&-(0x1f+1)) == 0 && ((1<<uint((_la-50)))&((1<<(NumScriptParserPORTION-50))|(1<<(NumScriptParserREMAINING-50))|(1<<(NumScriptParserVARIABLE_NAME-50)))) != 0) {
		{
			p.SetState(175)

			var _x = p.AllotmentPortion()

			localctx.(*SourceAllotmentContext)._allotmentPortion = _x
		}
		localctx.(*SourceAllotmentContext).portions = append(localctx.(*SourceAllotmentContext).portions, localctx.(*SourceAllotmentContext)._allotmentPortion)
		{
			p.SetState(176)
			p.Match(NumScriptParserFROM)
		}
		{
			p.SetState(177)

			var _x = p.Source()

			localctx.(*SourceAllotmentContext)._source = _x
		}
		localctx.(*SourceAllotmentContext).sources = append(localctx.(*SourceAllotmentContext).sources, localctx.(*SourceAllotmentContext)._source)
		{
			p.SetState(178)
			p.Match(NumScriptParserNEWLINE)
		}

		p.SetState(182)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(184)
		p.Match(NumScriptParserRBRACE)
	}

	return localctx
}

// IValueAwareSourceContext is an interface to support dynamic dispatch.
type IValueAwareSourceContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsValueAwareSourceContext differentiates from other interfaces.
	IsValueAwareSourceContext()
}

type ValueAwareSourceContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyValueAwareSourceContext() *ValueAwareSourceContext {
	var p = new(ValueAwareSourceContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_valueAwareSource
	return p
}

func (*ValueAwareSourceContext) IsValueAwareSourceContext() {}

func NewValueAwareSourceContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ValueAwareSourceContext {
	var p = new(ValueAwareSourceContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_valueAwareSource

	return p
}

func (s *ValueAwareSourceContext) GetParser() antlr.Parser { return s.parser }

func (s *ValueAwareSourceContext) CopyFrom(ctx *ValueAwareSourceContext) {
	s.BaseParserRuleContext.CopyFrom(ctx.BaseParserRuleContext)
}

func (s *ValueAwareSourceContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ValueAwareSourceContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type SrcContext struct {
	*ValueAwareSourceContext
}

func NewSrcContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SrcContext {
	var p = new(SrcContext)

	p.ValueAwareSourceContext = NewEmptyValueAwareSourceContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ValueAwareSourceContext))

	return p
}

func (s *SrcContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SrcContext) Source() ISourceContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISourceContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISourceContext)
}

func (s *SrcContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSrc(s)
	}
}

func (s *SrcContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSrc(s)
	}
}

type SrcAllotmentContext struct {
	*ValueAwareSourceContext
}

func NewSrcAllotmentContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SrcAllotmentContext {
	var p = new(SrcAllotmentContext)

	p.ValueAwareSourceContext = NewEmptyValueAwareSourceContext()
	p.parser = parser
	p.CopyFrom(ctx.(*ValueAwareSourceContext))

	return p
}

func (s *SrcAllotmentContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SrcAllotmentContext) SourceAllotment() ISourceAllotmentContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISourceAllotmentContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISourceAllotmentContext)
}

func (s *SrcAllotmentContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSrcAllotment(s)
	}
}

func (s *SrcAllotmentContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSrcAllotment(s)
	}
}

func (p *NumScriptParser) ValueAwareSource() (localctx IValueAwareSourceContext) {
	this := p
	_ = this

	localctx = NewValueAwareSourceContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 32, NumScriptParserRULE_valueAwareSource)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(188)
	p.GetErrorHandler().Sync(p)
	switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 14, p.GetParserRuleContext()) {
	case 1:
		localctx = NewSrcContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(186)
			p.Source()
		}

	case 2:
		localctx = NewSrcAllotmentContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(187)
			p.SourceAllotment()
		}

	}

	return localctx
}

// IStatementContext is an interface to support dynamic dispatch.
type IStatementContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsStatementContext differentiates from other interfaces.
	IsStatementContext()
}

type StatementContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyStatementContext() *StatementContext {
	var p = new(StatementContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_statement
	return p
}

func (*StatementContext) IsStatementContext() {}

func NewStatementContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *StatementContext {
	var p = new(StatementContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_statement

	return p
}

func (s *StatementContext) GetParser() antlr.Parser { return s.parser }

func (s *StatementContext) CopyFrom(ctx *StatementContext) {
	s.BaseParserRuleContext.CopyFrom(ctx.BaseParserRuleContext)
}

func (s *StatementContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StatementContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type PrintContext struct {
	*StatementContext
	expr IExpressionContext
}

func NewPrintContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *PrintContext {
	var p = new(PrintContext)

	p.StatementContext = NewEmptyStatementContext()
	p.parser = parser
	p.CopyFrom(ctx.(*StatementContext))

	return p
}

func (s *PrintContext) GetExpr() IExpressionContext { return s.expr }

func (s *PrintContext) SetExpr(v IExpressionContext) { s.expr = v }

func (s *PrintContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *PrintContext) PRINT() antlr.TerminalNode {
	return s.GetToken(NumScriptParserPRINT, 0)
}

func (s *PrintContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *PrintContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterPrint(s)
	}
}

func (s *PrintContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitPrint(s)
	}
}

type SendAllContext struct {
	*StatementContext
	monAll IMonetaryAllContext
	src    ISourceContext
	dest   IDestinationContext
}

func NewSendAllContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SendAllContext {
	var p = new(SendAllContext)

	p.StatementContext = NewEmptyStatementContext()
	p.parser = parser
	p.CopyFrom(ctx.(*StatementContext))

	return p
}

func (s *SendAllContext) GetMonAll() IMonetaryAllContext { return s.monAll }

func (s *SendAllContext) GetSrc() ISourceContext { return s.src }

func (s *SendAllContext) GetDest() IDestinationContext { return s.dest }

func (s *SendAllContext) SetMonAll(v IMonetaryAllContext) { s.monAll = v }

func (s *SendAllContext) SetSrc(v ISourceContext) { s.src = v }

func (s *SendAllContext) SetDest(v IDestinationContext) { s.dest = v }

func (s *SendAllContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SendAllContext) SEND() antlr.TerminalNode {
	return s.GetToken(NumScriptParserSEND, 0)
}

func (s *SendAllContext) LPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLPAREN, 0)
}

func (s *SendAllContext) AllNEWLINE() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserNEWLINE)
}

func (s *SendAllContext) NEWLINE(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserNEWLINE, i)
}

func (s *SendAllContext) RPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRPAREN, 0)
}

func (s *SendAllContext) MonetaryAll() IMonetaryAllContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IMonetaryAllContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IMonetaryAllContext)
}

func (s *SendAllContext) SOURCE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserSOURCE, 0)
}

func (s *SendAllContext) AllEQ() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserEQ)
}

func (s *SendAllContext) EQ(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserEQ, i)
}

func (s *SendAllContext) DESTINATION() antlr.TerminalNode {
	return s.GetToken(NumScriptParserDESTINATION, 0)
}

func (s *SendAllContext) Source() ISourceContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(ISourceContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(ISourceContext)
}

func (s *SendAllContext) Destination() IDestinationContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDestinationContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDestinationContext)
}

func (s *SendAllContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSendAll(s)
	}
}

func (s *SendAllContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSendAll(s)
	}
}

type SaveFromAccountContext struct {
	*StatementContext
	mon    IExpressionContext
	monAll IMonetaryAllContext
	acc    IExpressionContext
}

func NewSaveFromAccountContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SaveFromAccountContext {
	var p = new(SaveFromAccountContext)

	p.StatementContext = NewEmptyStatementContext()
	p.parser = parser
	p.CopyFrom(ctx.(*StatementContext))

	return p
}

func (s *SaveFromAccountContext) GetMon() IExpressionContext { return s.mon }

func (s *SaveFromAccountContext) GetMonAll() IMonetaryAllContext { return s.monAll }

func (s *SaveFromAccountContext) GetAcc() IExpressionContext { return s.acc }

func (s *SaveFromAccountContext) SetMon(v IExpressionContext) { s.mon = v }

func (s *SaveFromAccountContext) SetMonAll(v IMonetaryAllContext) { s.monAll = v }

func (s *SaveFromAccountContext) SetAcc(v IExpressionContext) { s.acc = v }

func (s *SaveFromAccountContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SaveFromAccountContext) SAVE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserSAVE, 0)
}

func (s *SaveFromAccountContext) FROM() antlr.TerminalNode {
	return s.GetToken(NumScriptParserFROM, 0)
}

func (s *SaveFromAccountContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *SaveFromAccountContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *SaveFromAccountContext) MonetaryAll() IMonetaryAllContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IMonetaryAllContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IMonetaryAllContext)
}

func (s *SaveFromAccountContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSaveFromAccount(s)
	}
}

func (s *SaveFromAccountContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSaveFromAccount(s)
	}
}

type SetTxMetaContext struct {
	*StatementContext
	key   antlr.Token
	value IExpressionContext
}

func NewSetTxMetaContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SetTxMetaContext {
	var p = new(SetTxMetaContext)

	p.StatementContext = NewEmptyStatementContext()
	p.parser = parser
	p.CopyFrom(ctx.(*StatementContext))

	return p
}

func (s *SetTxMetaContext) GetKey() antlr.Token { return s.key }

func (s *SetTxMetaContext) SetKey(v antlr.Token) { s.key = v }

func (s *SetTxMetaContext) GetValue() IExpressionContext { return s.value }

func (s *SetTxMetaContext) SetValue(v IExpressionContext) { s.value = v }

func (s *SetTxMetaContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SetTxMetaContext) SET_TX_META() antlr.TerminalNode {
	return s.GetToken(NumScriptParserSET_TX_META, 0)
}

func (s *SetTxMetaContext) LPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLPAREN, 0)
}

func (s *SetTxMetaContext) RPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRPAREN, 0)
}

func (s *SetTxMetaContext) STRING() antlr.TerminalNode {
	return s.GetToken(NumScriptParserSTRING, 0)
}

func (s *SetTxMetaContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *SetTxMetaContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSetTxMeta(s)
	}
}

func (s *SetTxMetaContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSetTxMeta(s)
	}
}

type SetAccountMetaContext struct {
	*StatementContext
	acc   IExpressionContext
	key   antlr.Token
	value IExpressionContext
}

func NewSetAccountMetaContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SetAccountMetaContext {
	var p = new(SetAccountMetaContext)

	p.StatementContext = NewEmptyStatementContext()
	p.parser = parser
	p.CopyFrom(ctx.(*StatementContext))

	return p
}

func (s *SetAccountMetaContext) GetKey() antlr.Token { return s.key }

func (s *SetAccountMetaContext) SetKey(v antlr.Token) { s.key = v }

func (s *SetAccountMetaContext) GetAcc() IExpressionContext { return s.acc }

func (s *SetAccountMetaContext) GetValue() IExpressionContext { return s.value }

func (s *SetAccountMetaContext) SetAcc(v IExpressionContext) { s.acc = v }

func (s *SetAccountMetaContext) SetValue(v IExpressionContext) { s.value = v }

func (s *SetAccountMetaContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SetAccountMetaContext) SET_ACCOUNT_META() antlr.TerminalNode {
	return s.GetToken(NumScriptParserSET_ACCOUNT_META, 0)
}

func (s *SetAccountMetaContext) LPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLPAREN, 0)
}

func (s *SetAccountMetaContext) RPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRPAREN, 0)
}

func (s *SetAccountMetaContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *SetAccountMetaContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *SetAccountMetaContext) STRING() antlr.TerminalNode {
	return s.GetToken(NumScriptParserSTRING, 0)
}

func (s *SetAccountMetaContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSetAccountMeta(s)
	}
}

func (s *SetAccountMetaContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSetAccountMeta(s)
	}
}

type FailContext struct {
	*StatementContext
}

func NewFailContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *FailContext {
	var p = new(FailContext)

	p.StatementContext = NewEmptyStatementContext()
	p.parser = parser
	p.CopyFrom(ctx.(*StatementContext))

	return p
}

func (s *FailContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FailContext) FAIL() antlr.TerminalNode {
	return s.GetToken(NumScriptParserFAIL, 0)
}

func (s *FailContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterFail(s)
	}
}

func (s *FailContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitFail(s)
	}
}

type SendContext struct {
	*StatementContext
	mon  IExpressionContext
	src  IValueAwareSourceContext
	dest IDestinationContext
}

func NewSendContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *SendContext {
	var p = new(SendContext)

	p.StatementContext = NewEmptyStatementContext()
	p.parser = parser
	p.CopyFrom(ctx.(*StatementContext))

	return p
}

func (s *SendContext) GetMon() IExpressionContext { return s.mon }

func (s *SendContext) GetSrc() IValueAwareSourceContext { return s.src }

func (s *SendContext) GetDest() IDestinationContext { return s.dest }

func (s *SendContext) SetMon(v IExpressionContext) { s.mon = v }

func (s *SendContext) SetSrc(v IValueAwareSourceContext) { s.src = v }

func (s *SendContext) SetDest(v IDestinationContext) { s.dest = v }

func (s *SendContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *SendContext) SEND() antlr.TerminalNode {
	return s.GetToken(NumScriptParserSEND, 0)
}

func (s *SendContext) LPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLPAREN, 0)
}

func (s *SendContext) AllNEWLINE() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserNEWLINE)
}

func (s *SendContext) NEWLINE(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserNEWLINE, i)
}

func (s *SendContext) RPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRPAREN, 0)
}

func (s *SendContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *SendContext) SOURCE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserSOURCE, 0)
}

func (s *SendContext) AllEQ() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserEQ)
}

func (s *SendContext) EQ(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserEQ, i)
}

func (s *SendContext) DESTINATION() antlr.TerminalNode {
	return s.GetToken(NumScriptParserDESTINATION, 0)
}

func (s *SendContext) ValueAwareSource() IValueAwareSourceContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IValueAwareSourceContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IValueAwareSourceContext)
}

func (s *SendContext) Destination() IDestinationContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IDestinationContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IDestinationContext)
}

func (s *SendContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterSend(s)
	}
}

func (s *SendContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitSend(s)
	}
}

func (p *NumScriptParser) Statement() (localctx IStatementContext) {
	this := p
	_ = this

	localctx = NewStatementContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 34, NumScriptParserRULE_statement)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(267)
	p.GetErrorHandler().Sync(p)
	switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 18, p.GetParserRuleContext()) {
	case 1:
		localctx = NewPrintContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(190)
			p.Match(NumScriptParserPRINT)
		}
		{
			p.SetState(191)

			var _x = p.expression(0)

			localctx.(*PrintContext).expr = _x
		}

	case 2:
		localctx = NewSaveFromAccountContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(192)
			p.Match(NumScriptParserSAVE)
		}
		p.SetState(195)
		p.GetErrorHandler().Sync(p)
		switch p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 15, p.GetParserRuleContext()) {
		case 1:
			{
				p.SetState(193)

				var _x = p.expression(0)

				localctx.(*SaveFromAccountContext).mon = _x
			}

		case 2:
			{
				p.SetState(194)

				var _x = p.MonetaryAll()

				localctx.(*SaveFromAccountContext).monAll = _x
			}

		}
		{
			p.SetState(197)
			p.Match(NumScriptParserFROM)
		}
		{
			p.SetState(198)

			var _x = p.expression(0)

			localctx.(*SaveFromAccountContext).acc = _x
		}

	case 3:
		localctx = NewSetTxMetaContext(p, localctx)
		p.EnterOuterAlt(localctx, 3)
		{
			p.SetState(200)
			p.Match(NumScriptParserSET_TX_META)
		}
		{
			p.SetState(201)
			p.Match(NumScriptParserLPAREN)
		}
		{
			p.SetState(202)

			var _m = p.Match(NumScriptParserSTRING)

			localctx.(*SetTxMetaContext).key = _m
		}
		{
			p.SetState(203)
			p.Match(NumScriptParserT__5)
		}
		{
			p.SetState(204)

			var _x = p.expression(0)

			localctx.(*SetTxMetaContext).value = _x
		}
		{
			p.SetState(205)
			p.Match(NumScriptParserRPAREN)
		}

	case 4:
		localctx = NewSetAccountMetaContext(p, localctx)
		p.EnterOuterAlt(localctx, 4)
		{
			p.SetState(207)
			p.Match(NumScriptParserSET_ACCOUNT_META)
		}
		{
			p.SetState(208)
			p.Match(NumScriptParserLPAREN)
		}
		{
			p.SetState(209)

			var _x = p.expression(0)

			localctx.(*SetAccountMetaContext).acc = _x
		}
		{
			p.SetState(210)
			p.Match(NumScriptParserT__5)
		}
		{
			p.SetState(211)

			var _m = p.Match(NumScriptParserSTRING)

			localctx.(*SetAccountMetaContext).key = _m
		}
		{
			p.SetState(212)
			p.Match(NumScriptParserT__5)
		}
		{
			p.SetState(213)

			var _x = p.expression(0)

			localctx.(*SetAccountMetaContext).value = _x
		}
		{
			p.SetState(214)
			p.Match(NumScriptParserRPAREN)
		}

	case 5:
		localctx = NewFailContext(p, localctx)
		p.EnterOuterAlt(localctx, 5)
		{
			p.SetState(216)
			p.Match(NumScriptParserFAIL)
		}

	case 6:
		localctx = NewSendContext(p, localctx)
		p.EnterOuterAlt(localctx, 6)
		{
			p.SetState(217)
			p.Match(NumScriptParserSEND)
		}
		{
			p.SetState(218)

			var _x = p.expression(0)

			localctx.(*SendContext).mon = _x
		}
		{
			p.SetState(219)
			p.Match(NumScriptParserLPAREN)
		}
		{
			p.SetState(220)
			p.Match(NumScriptParserNEWLINE)
		}
		p.SetState(237)
		p.GetErrorHandler().Sync(p)

		switch p.GetTokenStream().LA(1) {
		case NumScriptParserSOURCE:
			{
				p.SetState(221)
				p.Match(NumScriptParserSOURCE)
			}
			{
				p.SetState(222)
				p.Match(NumScriptParserEQ)
			}
			{
				p.SetState(223)

				var _x = p.ValueAwareSource()

				localctx.(*SendContext).src = _x
			}
			{
				p.SetState(224)
				p.Match(NumScriptParserNEWLINE)
			}
			{
				p.SetState(225)
				p.Match(NumScriptParserDESTINATION)
			}
			{
				p.SetState(226)
				p.Match(NumScriptParserEQ)
			}
			{
				p.SetState(227)

				var _x = p.Destination()

				localctx.(*SendContext).dest = _x
			}

		case NumScriptParserDESTINATION:
			{
				p.SetState(229)
				p.Match(NumScriptParserDESTINATION)
			}
			{
				p.SetState(230)
				p.Match(NumScriptParserEQ)
			}
			{
				p.SetState(231)

				var _x = p.Destination()

				localctx.(*SendContext).dest = _x
			}
			{
				p.SetState(232)
				p.Match(NumScriptParserNEWLINE)
			}
			{
				p.SetState(233)
				p.Match(NumScriptParserSOURCE)
			}
			{
				p.SetState(234)
				p.Match(NumScriptParserEQ)
			}
			{
				p.SetState(235)

				var _x = p.ValueAwareSource()

				localctx.(*SendContext).src = _x
			}

		default:
			panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		}
		{
			p.SetState(239)
			p.Match(NumScriptParserNEWLINE)
		}
		{
			p.SetState(240)
			p.Match(NumScriptParserRPAREN)
		}

	case 7:
		localctx = NewSendAllContext(p, localctx)
		p.EnterOuterAlt(localctx, 7)
		{
			p.SetState(242)
			p.Match(NumScriptParserSEND)
		}
		{
			p.SetState(243)

			var _x = p.MonetaryAll()

			localctx.(*SendAllContext).monAll = _x
		}
		{
			p.SetState(244)
			p.Match(NumScriptParserLPAREN)
		}
		{
			p.SetState(245)
			p.Match(NumScriptParserNEWLINE)
		}
		p.SetState(262)
		p.GetErrorHandler().Sync(p)

		switch p.GetTokenStream().LA(1) {
		case NumScriptParserSOURCE:
			{
				p.SetState(246)
				p.Match(NumScriptParserSOURCE)
			}
			{
				p.SetState(247)
				p.Match(NumScriptParserEQ)
			}
			{
				p.SetState(248)

				var _x = p.Source()

				localctx.(*SendAllContext).src = _x
			}
			{
				p.SetState(249)
				p.Match(NumScriptParserNEWLINE)
			}
			{
				p.SetState(250)
				p.Match(NumScriptParserDESTINATION)
			}
			{
				p.SetState(251)
				p.Match(NumScriptParserEQ)
			}
			{
				p.SetState(252)

				var _x = p.Destination()

				localctx.(*SendAllContext).dest = _x
			}

		case NumScriptParserDESTINATION:
			{
				p.SetState(254)
				p.Match(NumScriptParserDESTINATION)
			}
			{
				p.SetState(255)
				p.Match(NumScriptParserEQ)
			}
			{
				p.SetState(256)

				var _x = p.Destination()

				localctx.(*SendAllContext).dest = _x
			}
			{
				p.SetState(257)
				p.Match(NumScriptParserNEWLINE)
			}
			{
				p.SetState(258)
				p.Match(NumScriptParserSOURCE)
			}
			{
				p.SetState(259)
				p.Match(NumScriptParserEQ)
			}
			{
				p.SetState(260)

				var _x = p.Source()

				localctx.(*SendAllContext).src = _x
			}

		default:
			panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		}
		{
			p.SetState(264)
			p.Match(NumScriptParserNEWLINE)
		}
		{
			p.SetState(265)
			p.Match(NumScriptParserRPAREN)
		}

	}

	return localctx
}

// IType_Context is an interface to support dynamic dispatch.
type IType_Context interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsType_Context differentiates from other interfaces.
	IsType_Context()
}

type Type_Context struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyType_Context() *Type_Context {
	var p = new(Type_Context)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_type_
	return p
}

func (*Type_Context) IsType_Context() {}

func NewType_Context(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Type_Context {
	var p = new(Type_Context)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_type_

	return p
}

func (s *Type_Context) GetParser() antlr.Parser { return s.parser }

func (s *Type_Context) TY_ACCOUNT() antlr.TerminalNode {
	return s.GetToken(NumScriptParserTY_ACCOUNT, 0)
}

func (s *Type_Context) TY_ASSET() antlr.TerminalNode {
	return s.GetToken(NumScriptParserTY_ASSET, 0)
}

func (s *Type_Context) TY_NUMBER() antlr.TerminalNode {
	return s.GetToken(NumScriptParserTY_NUMBER, 0)
}

func (s *Type_Context) TY_STRING() antlr.TerminalNode {
	return s.GetToken(NumScriptParserTY_STRING, 0)
}

func (s *Type_Context) TY_MONETARY() antlr.TerminalNode {
	return s.GetToken(NumScriptParserTY_MONETARY, 0)
}

func (s *Type_Context) TY_PORTION() antlr.TerminalNode {
	return s.GetToken(NumScriptParserTY_PORTION, 0)
}

func (s *Type_Context) TY_BOOL() antlr.TerminalNode {
	return s.GetToken(NumScriptParserTY_BOOL, 0)
}

func (s *Type_Context) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Type_Context) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Type_Context) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterType_(s)
	}
}

func (s *Type_Context) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitType_(s)
	}
}

func (p *NumScriptParser) Type_() (localctx IType_Context) {
	this := p
	_ = this

	localctx = NewType_Context(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 36, NumScriptParserRULE_type_)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(269)
		_la = p.GetTokenStream().LA(1)

		if !(((_la-42)&-(0x1f+1)) == 0 && ((1<<uint((_la-42)))&((1<<(NumScriptParserTY_ACCOUNT-42))|(1<<(NumScriptParserTY_ASSET-42))|(1<<(NumScriptParserTY_NUMBER-42))|(1<<(NumScriptParserTY_MONETARY-42))|(1<<(NumScriptParserTY_PORTION-42))|(1<<(NumScriptParserTY_STRING-42))|(1<<(NumScriptParserTY_BOOL-42)))) != 0) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}

	return localctx
}

// IOriginContext is an interface to support dynamic dispatch.
type IOriginContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// IsOriginContext differentiates from other interfaces.
	IsOriginContext()
}

type OriginContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyOriginContext() *OriginContext {
	var p = new(OriginContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_origin
	return p
}

func (*OriginContext) IsOriginContext() {}

func NewOriginContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *OriginContext {
	var p = new(OriginContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_origin

	return p
}

func (s *OriginContext) GetParser() antlr.Parser { return s.parser }

func (s *OriginContext) CopyFrom(ctx *OriginContext) {
	s.BaseParserRuleContext.CopyFrom(ctx.BaseParserRuleContext)
}

func (s *OriginContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *OriginContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type OriginAccountBalanceContext struct {
	*OriginContext
	account IExpressionContext
	asset   IExpressionContext
}

func NewOriginAccountBalanceContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *OriginAccountBalanceContext {
	var p = new(OriginAccountBalanceContext)

	p.OriginContext = NewEmptyOriginContext()
	p.parser = parser
	p.CopyFrom(ctx.(*OriginContext))

	return p
}

func (s *OriginAccountBalanceContext) GetAccount() IExpressionContext { return s.account }

func (s *OriginAccountBalanceContext) GetAsset() IExpressionContext { return s.asset }

func (s *OriginAccountBalanceContext) SetAccount(v IExpressionContext) { s.account = v }

func (s *OriginAccountBalanceContext) SetAsset(v IExpressionContext) { s.asset = v }

func (s *OriginAccountBalanceContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *OriginAccountBalanceContext) BALANCE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserBALANCE, 0)
}

func (s *OriginAccountBalanceContext) LPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLPAREN, 0)
}

func (s *OriginAccountBalanceContext) RPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRPAREN, 0)
}

func (s *OriginAccountBalanceContext) AllExpression() []IExpressionContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExpressionContext); ok {
			len++
		}
	}

	tst := make([]IExpressionContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExpressionContext); ok {
			tst[i] = t.(IExpressionContext)
			i++
		}
	}

	return tst
}

func (s *OriginAccountBalanceContext) Expression(i int) IExpressionContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *OriginAccountBalanceContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterOriginAccountBalance(s)
	}
}

func (s *OriginAccountBalanceContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitOriginAccountBalance(s)
	}
}

type OriginAccountMetaContext struct {
	*OriginContext
	account IExpressionContext
	key     antlr.Token
}

func NewOriginAccountMetaContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *OriginAccountMetaContext {
	var p = new(OriginAccountMetaContext)

	p.OriginContext = NewEmptyOriginContext()
	p.parser = parser
	p.CopyFrom(ctx.(*OriginContext))

	return p
}

func (s *OriginAccountMetaContext) GetKey() antlr.Token { return s.key }

func (s *OriginAccountMetaContext) SetKey(v antlr.Token) { s.key = v }

func (s *OriginAccountMetaContext) GetAccount() IExpressionContext { return s.account }

func (s *OriginAccountMetaContext) SetAccount(v IExpressionContext) { s.account = v }

func (s *OriginAccountMetaContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *OriginAccountMetaContext) META() antlr.TerminalNode {
	return s.GetToken(NumScriptParserMETA, 0)
}

func (s *OriginAccountMetaContext) LPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLPAREN, 0)
}

func (s *OriginAccountMetaContext) RPAREN() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRPAREN, 0)
}

func (s *OriginAccountMetaContext) Expression() IExpressionContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExpressionContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExpressionContext)
}

func (s *OriginAccountMetaContext) STRING() antlr.TerminalNode {
	return s.GetToken(NumScriptParserSTRING, 0)
}

func (s *OriginAccountMetaContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterOriginAccountMeta(s)
	}
}

func (s *OriginAccountMetaContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitOriginAccountMeta(s)
	}
}

func (p *NumScriptParser) Origin() (localctx IOriginContext) {
	this := p
	_ = this

	localctx = NewOriginContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 38, NumScriptParserRULE_origin)

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.SetState(285)
	p.GetErrorHandler().Sync(p)

	switch p.GetTokenStream().LA(1) {
	case NumScriptParserMETA:
		localctx = NewOriginAccountMetaContext(p, localctx)
		p.EnterOuterAlt(localctx, 1)
		{
			p.SetState(271)
			p.Match(NumScriptParserMETA)
		}
		{
			p.SetState(272)
			p.Match(NumScriptParserLPAREN)
		}
		{
			p.SetState(273)

			var _x = p.expression(0)

			localctx.(*OriginAccountMetaContext).account = _x
		}
		{
			p.SetState(274)
			p.Match(NumScriptParserT__5)
		}
		{
			p.SetState(275)

			var _m = p.Match(NumScriptParserSTRING)

			localctx.(*OriginAccountMetaContext).key = _m
		}
		{
			p.SetState(276)
			p.Match(NumScriptParserRPAREN)
		}

	case NumScriptParserBALANCE:
		localctx = NewOriginAccountBalanceContext(p, localctx)
		p.EnterOuterAlt(localctx, 2)
		{
			p.SetState(278)
			p.Match(NumScriptParserBALANCE)
		}
		{
			p.SetState(279)
			p.Match(NumScriptParserLPAREN)
		}
		{
			p.SetState(280)

			var _x = p.expression(0)

			localctx.(*OriginAccountBalanceContext).account = _x
		}
		{
			p.SetState(281)
			p.Match(NumScriptParserT__5)
		}
		{
			p.SetState(282)

			var _x = p.expression(0)

			localctx.(*OriginAccountBalanceContext).asset = _x
		}
		{
			p.SetState(283)
			p.Match(NumScriptParserRPAREN)
		}

	default:
		panic(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
	}

	return localctx
}

// IVarDeclContext is an interface to support dynamic dispatch.
type IVarDeclContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// GetTy returns the ty rule contexts.
	GetTy() IType_Context

	// GetName returns the name rule contexts.
	GetName() IVariableContext

	// GetOrig returns the orig rule contexts.
	GetOrig() IOriginContext

	// SetTy sets the ty rule contexts.
	SetTy(IType_Context)

	// SetName sets the name rule contexts.
	SetName(IVariableContext)

	// SetOrig sets the orig rule contexts.
	SetOrig(IOriginContext)

	// IsVarDeclContext differentiates from other interfaces.
	IsVarDeclContext()
}

type VarDeclContext struct {
	*antlr.BaseParserRuleContext
	parser antlr.Parser
	ty     IType_Context
	name   IVariableContext
	orig   IOriginContext
}

func NewEmptyVarDeclContext() *VarDeclContext {
	var p = new(VarDeclContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_varDecl
	return p
}

func (*VarDeclContext) IsVarDeclContext() {}

func NewVarDeclContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *VarDeclContext {
	var p = new(VarDeclContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_varDecl

	return p
}

func (s *VarDeclContext) GetParser() antlr.Parser { return s.parser }

func (s *VarDeclContext) GetTy() IType_Context { return s.ty }

func (s *VarDeclContext) GetName() IVariableContext { return s.name }

func (s *VarDeclContext) GetOrig() IOriginContext { return s.orig }

func (s *VarDeclContext) SetTy(v IType_Context) { s.ty = v }

func (s *VarDeclContext) SetName(v IVariableContext) { s.name = v }

func (s *VarDeclContext) SetOrig(v IOriginContext) { s.orig = v }

func (s *VarDeclContext) Type_() IType_Context {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IType_Context); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IType_Context)
}

func (s *VarDeclContext) Variable() IVariableContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IVariableContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IVariableContext)
}

func (s *VarDeclContext) EQ() antlr.TerminalNode {
	return s.GetToken(NumScriptParserEQ, 0)
}

func (s *VarDeclContext) Origin() IOriginContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IOriginContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IOriginContext)
}

func (s *VarDeclContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *VarDeclContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *VarDeclContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterVarDecl(s)
	}
}

func (s *VarDeclContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitVarDecl(s)
	}
}

func (p *NumScriptParser) VarDecl() (localctx IVarDeclContext) {
	this := p
	_ = this

	localctx = NewVarDeclContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 40, NumScriptParserRULE_varDecl)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(287)

		var _x = p.Type_()

		localctx.(*VarDeclContext).ty = _x
	}
	{
		p.SetState(288)

		var _x = p.Variable()

		localctx.(*VarDeclContext).name = _x
	}
	p.SetState(291)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	if _la == NumScriptParserEQ {
		{
			p.SetState(289)
			p.Match(NumScriptParserEQ)
		}
		{
			p.SetState(290)

			var _x = p.Origin()

			localctx.(*VarDeclContext).orig = _x
		}

	}

	return localctx
}

// IVarListDeclContext is an interface to support dynamic dispatch.
type IVarListDeclContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Get_varDecl returns the _varDecl rule contexts.
	Get_varDecl() IVarDeclContext

	// Set_varDecl sets the _varDecl rule contexts.
	Set_varDecl(IVarDeclContext)

	// GetV returns the v rule context list.
	GetV() []IVarDeclContext

	// SetV sets the v rule context list.
	SetV([]IVarDeclContext)

	// IsVarListDeclContext differentiates from other interfaces.
	IsVarListDeclContext()
}

type VarListDeclContext struct {
	*antlr.BaseParserRuleContext
	parser   antlr.Parser
	_varDecl IVarDeclContext
	v        []IVarDeclContext
}

func NewEmptyVarListDeclContext() *VarListDeclContext {
	var p = new(VarListDeclContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_varListDecl
	return p
}

func (*VarListDeclContext) IsVarListDeclContext() {}

func NewVarListDeclContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *VarListDeclContext {
	var p = new(VarListDeclContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_varListDecl

	return p
}

func (s *VarListDeclContext) GetParser() antlr.Parser { return s.parser }

func (s *VarListDeclContext) Get_varDecl() IVarDeclContext { return s._varDecl }

func (s *VarListDeclContext) Set_varDecl(v IVarDeclContext) { s._varDecl = v }

func (s *VarListDeclContext) GetV() []IVarDeclContext { return s.v }

func (s *VarListDeclContext) SetV(v []IVarDeclContext) { s.v = v }

func (s *VarListDeclContext) VARS() antlr.TerminalNode {
	return s.GetToken(NumScriptParserVARS, 0)
}

func (s *VarListDeclContext) LBRACE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserLBRACE, 0)
}

func (s *VarListDeclContext) AllNEWLINE() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserNEWLINE)
}

func (s *VarListDeclContext) NEWLINE(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserNEWLINE, i)
}

func (s *VarListDeclContext) RBRACE() antlr.TerminalNode {
	return s.GetToken(NumScriptParserRBRACE, 0)
}

func (s *VarListDeclContext) AllVarDecl() []IVarDeclContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IVarDeclContext); ok {
			len++
		}
	}

	tst := make([]IVarDeclContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IVarDeclContext); ok {
			tst[i] = t.(IVarDeclContext)
			i++
		}
	}

	return tst
}

func (s *VarListDeclContext) VarDecl(i int) IVarDeclContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IVarDeclContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IVarDeclContext)
}

func (s *VarListDeclContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *VarListDeclContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *VarListDeclContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterVarListDecl(s)
	}
}

func (s *VarListDeclContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitVarListDecl(s)
	}
}

func (p *NumScriptParser) VarListDecl() (localctx IVarListDeclContext) {
	this := p
	_ = this

	localctx = NewVarListDeclContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 42, NumScriptParserRULE_varListDecl)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(293)
		p.Match(NumScriptParserVARS)
	}
	{
		p.SetState(294)
		p.Match(NumScriptParserLBRACE)
	}
	{
		p.SetState(295)
		p.Match(NumScriptParserNEWLINE)
	}
	p.SetState(302)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for ok := true; ok; ok = (((_la-42)&-(0x1f+1)) == 0 && ((1<<uint((_la-42)))&((1<<(NumScriptParserTY_ACCOUNT-42))|(1<<(NumScriptParserTY_ASSET-42))|(1<<(NumScriptParserTY_NUMBER-42))|(1<<(NumScriptParserTY_MONETARY-42))|(1<<(NumScriptParserTY_PORTION-42))|(1<<(NumScriptParserTY_STRING-42))|(1<<(NumScriptParserTY_BOOL-42)))) != 0) {
		{
			p.SetState(296)

			var _x = p.VarDecl()

			localctx.(*VarListDeclContext)._varDecl = _x
		}
		localctx.(*VarListDeclContext).v = append(localctx.(*VarListDeclContext).v, localctx.(*VarListDeclContext)._varDecl)
		p.SetState(298)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)

		for ok := true; ok; ok = _la == NumScriptParserNEWLINE {
			{
				p.SetState(297)
				p.Match(NumScriptParserNEWLINE)
			}

			p.SetState(300)
			p.GetErrorHandler().Sync(p)
			_la = p.GetTokenStream().LA(1)
		}

		p.SetState(304)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(306)
		p.Match(NumScriptParserRBRACE)
	}
	{
		p.SetState(307)
		p.Match(NumScriptParserNEWLINE)
	}

	return localctx
}

// IScriptContext is an interface to support dynamic dispatch.
type IScriptContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// GetVars returns the vars rule contexts.
	GetVars() IVarListDeclContext

	// Get_statement returns the _statement rule contexts.
	Get_statement() IStatementContext

	// SetVars sets the vars rule contexts.
	SetVars(IVarListDeclContext)

	// Set_statement sets the _statement rule contexts.
	Set_statement(IStatementContext)

	// GetStmts returns the stmts rule context list.
	GetStmts() []IStatementContext

	// SetStmts sets the stmts rule context list.
	SetStmts([]IStatementContext)

	// IsScriptContext differentiates from other interfaces.
	IsScriptContext()
}

type ScriptContext struct {
	*antlr.BaseParserRuleContext
	parser     antlr.Parser
	vars       IVarListDeclContext
	_statement IStatementContext
	stmts      []IStatementContext
}

func NewEmptyScriptContext() *ScriptContext {
	var p = new(ScriptContext)
	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(nil, -1)
	p.RuleIndex = NumScriptParserRULE_script
	return p
}

func (*ScriptContext) IsScriptContext() {}

func NewScriptContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ScriptContext {
	var p = new(ScriptContext)

	p.BaseParserRuleContext = antlr.NewBaseParserRuleContext(parent, invokingState)

	p.parser = parser
	p.RuleIndex = NumScriptParserRULE_script

	return p
}

func (s *ScriptContext) GetParser() antlr.Parser { return s.parser }

func (s *ScriptContext) GetVars() IVarListDeclContext { return s.vars }

func (s *ScriptContext) Get_statement() IStatementContext { return s._statement }

func (s *ScriptContext) SetVars(v IVarListDeclContext) { s.vars = v }

func (s *ScriptContext) Set_statement(v IStatementContext) { s._statement = v }

func (s *ScriptContext) GetStmts() []IStatementContext { return s.stmts }

func (s *ScriptContext) SetStmts(v []IStatementContext) { s.stmts = v }

func (s *ScriptContext) EOF() antlr.TerminalNode {
	return s.GetToken(NumScriptParserEOF, 0)
}

func (s *ScriptContext) AllStatement() []IStatementContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IStatementContext); ok {
			len++
		}
	}

	tst := make([]IStatementContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IStatementContext); ok {
			tst[i] = t.(IStatementContext)
			i++
		}
	}

	return tst
}

func (s *ScriptContext) Statement(i int) IStatementContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IStatementContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IStatementContext)
}

func (s *ScriptContext) AllNEWLINE() []antlr.TerminalNode {
	return s.GetTokens(NumScriptParserNEWLINE)
}

func (s *ScriptContext) NEWLINE(i int) antlr.TerminalNode {
	return s.GetToken(NumScriptParserNEWLINE, i)
}

func (s *ScriptContext) VarListDecl() IVarListDeclContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IVarListDeclContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IVarListDeclContext)
}

func (s *ScriptContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ScriptContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *ScriptContext) EnterRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.EnterScript(s)
	}
}

func (s *ScriptContext) ExitRule(listener antlr.ParseTreeListener) {
	if listenerT, ok := listener.(NumScriptListener); ok {
		listenerT.ExitScript(s)
	}
}

func (p *NumScriptParser) Script() (localctx IScriptContext) {
	this := p
	_ = this

	localctx = NewScriptContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 44, NumScriptParserRULE_script)
	var _la int

	defer func() {
		p.ExitRule()
	}()

	defer func() {
		if err := recover(); err != nil {
			if v, ok := err.(antlr.RecognitionException); ok {
				localctx.SetException(v)
				p.GetErrorHandler().ReportError(p, v)
				p.GetErrorHandler().Recover(p, v)
			} else {
				panic(err)
			}
		}
	}()

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(312)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for _la == NumScriptParserNEWLINE {
		{
			p.SetState(309)
			p.Match(NumScriptParserNEWLINE)
		}

		p.SetState(314)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}
	p.SetState(316)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	if _la == NumScriptParserVARS {
		{
			p.SetState(315)

			var _x = p.VarListDecl()

			localctx.(*ScriptContext).vars = _x
		}

	}
	{
		p.SetState(318)

		var _x = p.Statement()

		localctx.(*ScriptContext)._statement = _x
	}
	localctx.(*ScriptContext).stmts = append(localctx.(*ScriptContext).stmts, localctx.(*ScriptContext)._statement)
	p.SetState(323)
	p.GetErrorHandler().Sync(p)
	_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 25, p.GetParserRuleContext())

	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			{
				p.SetState(319)
				p.Match(NumScriptParserNEWLINE)
			}
			{
				p.SetState(320)

				var _x = p.Statement()

				localctx.(*ScriptContext)._statement = _x
			}
			localctx.(*ScriptContext).stmts = append(localctx.(*ScriptContext).stmts, localctx.(*ScriptContext)._statement)

		}
		p.SetState(325)
		p.GetErrorHandler().Sync(p)
		_alt = p.GetInterpreter().AdaptivePredict(p.GetTokenStream(), 25, p.GetParserRuleContext())
	}
	p.SetState(329)
	p.GetErrorHandler().Sync(p)
	_la = p.GetTokenStream().LA(1)

	for _la == NumScriptParserNEWLINE {
		{
			p.SetState(326)
			p.Match(NumScriptParserNEWLINE)
		}

		p.SetState(331)
		p.GetErrorHandler().Sync(p)
		_la = p.GetTokenStream().LA(1)
	}
	{
		p.SetState(332)
		p.Match(NumScriptParserEOF)
	}

	return localctx
}

func (p *NumScriptParser) Sempred(localctx antlr.RuleContext, ruleIndex, predIndex int) bool {
	switch ruleIndex {
	case 4:
		var t *ExpressionContext = nil
		if localctx != nil {
			t = localctx.(*ExpressionContext)
		}
		return p.Expression_Sempred(t, predIndex)

	default:
		panic("No predicate with index: " + fmt.Sprint(ruleIndex))
	}
}

func (p *NumScriptParser) Expression_Sempred(localctx antlr.RuleContext, predIndex int) bool {
	this := p
	_ = this

	switch predIndex {
	case 0:
		return p.Precpred(p.GetParserRuleContext(), 10)

	case 1:
		return p.Precpred(p.GetParserRuleContext(), 9)

	case 2:
		return p.Precpred(p.GetParserRuleContext(), 7)

	case 3:
		return p.Precpred(p.GetParserRuleContext(), 6)

	case 4:
		return p.Precpred(p.GetParserRuleContext(), 2)

	default:
		panic("No predicate with index: " + fmt.Sprint(predIndex))
	}
}
