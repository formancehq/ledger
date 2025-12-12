package ledger

import (
	"encoding/json"
)

type QueryMode string

const (
	Sync QueryMode = "sync"
)

type QueryTemplates map[string]QueryTemplate

type QueryTemplate struct {
	Name        string            `json:"name"`
	OperationId string            `json:"operation_id"`
	Mode        QueryMode         `json:"mode"`
	Params      map[string]string `json:"params"`
	Body        json.RawMessage   `json:"body"`
}


// func resolveFilter(m map[string]any, ) (Builder, error) {
// 	operator, value, err := singleKey(m)
// 	if err != nil {
// 		return nil, err
// 	}
// 	switch operator {
// 	case "$and", "$or":
// 		and, err := parseSet(operator, value)
// 		if err != nil {
// 			return nil, errors.Wrap(err, "parsing $and")
// 		}
// 		return and, nil
// 	case "$match", "$gte", "$lte", "$gt", "$lt", "$exists", "$like", "$in":
// 		match, err := parseKeyValue(operator, value)
// 		if err != nil {
// 			return nil, errors.Wrapf(err, "parsing %s", operator)
// 		}
// 		return match, nil
// 	case "$not":
// 		match, err := parseNot(value)
// 		if err != nil {
// 			return nil, errors.Wrapf(err, "parsing %s", operator)
// 		}
// 		return match, nil
// 	default:
// 		return nil, fmt.Errorf("unexpected operator %s", operator)
// 	}
// }
