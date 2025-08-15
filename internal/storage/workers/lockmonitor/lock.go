package lockmonitor

import (
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
)

type lock struct {
	BlockedPID                           uint8  `bun:"blocked_pid"`
	BlockedUser                          string `bun:"blocked_user"`
	BlockingPID                          uint8  `bun:"blocking_pid"`
	BlockingUser                         string `bun:"blocking_user"`
	RawBlockedStatement                  string `bun:"blocked_statement"`
	RawCurrentStatementInBlockingProcess string `bun:"current_statement_in_blocking_process"`
	BlockedApplication                   string `bun:"blocked_application"`
	BlockingApplication                  string `bun:"blocking_application"`
}

type Lock struct {
	lock
	BlockedStatement                  Statement
	CurrentStatementInBlockingProcess Statement
}

type Statement struct {
	raw         string
	parseResult parser.Statements
	parseError  error
}

func (s *Statement) GetParsedResult() (parser.Statements, error) {
	if s.parseError != nil {
		return nil, s.parseError
	}
	if len(s.parseResult) > 0 {
		return s.parseResult, nil
	}

	s.parseResult, s.parseError = parser.Parse(s.raw)

	return s.parseResult, s.parseError
}

func (s *Statement) MustParseResult() parser.Statements {
	res, err := s.GetParsedResult()
	if err != nil {
		panic(err)
	}
	return res
}

func newStatement(raw string) Statement {
	return Statement{
		raw: raw,
	}
}
