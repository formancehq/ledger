package compiler

import (
	"fmt"
	"math"
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/logrusorgru/aurora"
)

type CompileError struct {
	StartL, StartC int
	EndL, EndC     int
	Msg            string
}

type CompileErrorList struct {
	Errors []CompileError
	Source string
}

func (c *CompileErrorList) Is(err error) bool {
	_, ok := err.(*CompileErrorList)
	return ok
}

func (c *CompileErrorList) Error() string {
	source := strings.ReplaceAll(c.Source, "\t", " ")
	lines := strings.SplitAfter(strings.ReplaceAll(source, "\r\n", "\n"), "\n")
	lines[len(lines)-1] += "\n"

	txtBarGood := aurora.Blue("|")

	s := ""
	for _, e := range c.Errors {
		lnPad := int(math.Log10(float64(e.EndL))) + 1 // line number padding
		// error indicator
		s += fmt.Sprintf("%v error:%v:%v\n", aurora.Red("-->"), e.StartL, e.StartC)
		// initial empty line
		s += fmt.Sprintf("%v %v\n", strings.Repeat(" ", lnPad), txtBarGood)
		// offending lines
		for l := e.StartL; l <= e.EndL; l++ { // "print fail"
			line := lines[l-1]
			before := ""
			after := ""
			start := 0
			if l == e.StartL {
				before = line[:e.StartC]
				line = line[e.StartC:]
				start = e.StartC
			}
			if l == e.EndL {
				idx := e.EndC - start + 1
				if idx >= len(line) { // because newline was erased
					idx = len(line) - 1
				}
				after = line[idx:]
				line = line[:idx]
			}
			s += aurora.Red(fmt.Sprintf("%0*d | ", lnPad, l)).String()
			s += fmt.Sprintf("%v%v%v",
				aurora.BrightBlack(before), line, aurora.BrightBlack(after))
		}
		// message
		start := strings.IndexFunc(lines[e.EndL-1], func(r rune) bool {
			return r != ' '
		})
		span := e.EndC - start + 1
		if e.StartL == e.EndL {
			start = e.StartC
			span = e.EndC - e.StartC
		}
		if span == 0 {
			span = 1
		}
		s += fmt.Sprintf("%v %v %v%v %v\n",
			strings.Repeat(" ", lnPad),
			txtBarGood,
			strings.Repeat(" ", start),
			aurora.Red(strings.Repeat("^", span)),
			e.Msg)
	}
	return s
}

type ErrorListener struct {
	*antlr.DefaultErrorListener
	Errors []CompileError
}

func (l *ErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, startL, startC int, msg string, e antlr.RecognitionException) {
	length := 1
	if token, ok := offendingSymbol.(antlr.Token); ok {
		length = len(token.GetText())
	}
	endL := startL
	endC := startC + length - 1 // -1 so that end character is inside the offending token
	l.Errors = append(l.Errors, CompileError{
		StartL: startL,
		StartC: startC,
		EndL:   endL,
		EndC:   endC,
		Msg:    msg,
	})
}

func LogicError(c antlr.ParserRuleContext, err error) *CompileError {
	endC := c.GetStop().GetColumn() + len(c.GetStop().GetText())
	return &CompileError{
		StartL: c.GetStart().GetLine(),
		StartC: c.GetStart().GetColumn(),
		EndL:   c.GetStop().GetLine(),
		EndC:   endC,
		Msg:    err.Error(),
	}
}

const InternalErrorMsg = "internal compiler error, please report to the issue tracker"

func InternalError(c antlr.ParserRuleContext) *CompileError {
	return &CompileError{
		StartL: c.GetStart().GetLine(),
		StartC: c.GetStart().GetColumn(),
		EndL:   c.GetStop().GetLine(),
		EndC:   c.GetStop().GetColumn(),
		Msg:    InternalErrorMsg,
	}
}
