package ledgerpb

import (
	"encoding/json/v2"
	"encoding/json/jsontext"
	"fmt"
	"strconv"
	"strings"
)

// UnmarshalJSON implements json.Unmarshaler for DeletedMetadata
// Handles the special case where TargetID can be either a string (for ACCOUNT) or uint64 (for TRANSACTION)
func (dm *DeletedMetadata) UnmarshalJSON(data []byte) error {
	type X struct {
		TargetType string        `json:"targetType"`
		TargetID   jsontext.Value `json:"targetId"`
		Key        string        `json:"key"`
	}
	x := X{}
	err := json.Unmarshal(data, &x)
	if err != nil {
		return err
	}

	dm.TargetType = x.TargetType
	dm.Key = x.Key

	switch strings.ToUpper(x.TargetType) {
	case strings.ToUpper(MetaTargetTypeAccount):
		var accountID string
		targetIDBytes, err := json.Marshal(x.TargetID)
		if err != nil {
			return err
		}
		err = json.Unmarshal(targetIDBytes, &accountID)
		if err == nil {
			dm.TargetId = &DeletedMetadata_AccountId{AccountId: accountID}
		}
	case strings.ToUpper(MetaTargetTypeTransaction):
		var txID uint64
		targetIDBytes, err := json.Marshal(x.TargetID)
		if err != nil {
			return err
		}
		txID, err = strconv.ParseUint(string(targetIDBytes), 10, 64)
		if err == nil {
			dm.TargetId = &DeletedMetadata_TransactionId{TransactionId: txID}
		}
	default:
		return fmt.Errorf("unknown type '%s'", x.TargetType)
	}
	return err
}

