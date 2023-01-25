package searchengine

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/aquasecurity/esquery"
)

type operatorMapper func(key string, value interface{}) esquery.Mappable

type operator struct {
	symbol string
	mapper operatorMapper
}

const (
	TermPolicyAND = "AND"
	TermPolicyOR  = "OR"
)

var (
	operatorGt = &operator{
		symbol: ">",
		mapper: func(key string, value interface{}) esquery.Mappable {
			return esquery.Range(key).Gt(value)
		},
	}
	operatorLt = &operator{
		symbol: "<",
		mapper: func(key string, value interface{}) esquery.Mappable {
			return esquery.Range(key).Lt(value)
		},
	}
	operatorGte = &operator{
		symbol: ">=",
		mapper: func(key string, value interface{}) esquery.Mappable {
			return esquery.Range(key).Gte(value)
		},
	}
	operatorLte = &operator{
		symbol: "<=",
		mapper: func(key string, value interface{}) esquery.Mappable {
			return esquery.Range(key).Lte(value)
		},
	}
	operatorEquals = &operator{
		symbol: "=",
		mapper: func(key string, value interface{}) esquery.Mappable {
			return esquery.Match(key, value)
		},
	}
	operators = map[string]*operator{
		operatorGt.symbol:     operatorGt,
		operatorLt.symbol:     operatorLt,
		operatorGte.symbol:    operatorGte,
		operatorLte.symbol:    operatorLte,
		operatorEquals.symbol: operatorEquals,
	}

	matchRegexp = regexp.MustCompile("([^<=>:]+)([<=>]+)(.+)")

	fieldMap = func(v string) string {
		return "indexed." + v
	}

	defaultFieldComputer = func(key, value string, mapper operatorMapper) (esquery.Mappable, error) {
		return mapper(fieldMap(key), value), nil
	}
)

func ParseTerm(v string) (esquery.Mappable, error) {
	if !matchRegexp.MatchString(v) {
		return esquery.CustomQuery(map[string]interface{}{
			"query_string": map[string]interface{}{
				"query": fmt.Sprintf("*%s*", strings.Replace(v, ":", "\\:", 1)),
			},
		}), nil
	}
	matches := matchRegexp.FindStringSubmatch(v)
	var (
		key      = matches[1]
		value    = matches[3]
		operator = operators[matches[2]]
	)
	return defaultFieldComputer(key, value, operator.mapper)
}

func ParseTerms(terms ...string) ([]esquery.Mappable, error) {
	ret := make([]esquery.Mappable, 0)
	for _, termStr := range terms {
		term, err := ParseTerm(termStr)
		if err != nil {
			return nil, err
		}
		ret = append(ret, term)
	}
	return ret, nil
}
