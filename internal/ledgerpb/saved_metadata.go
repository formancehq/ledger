package ledgerpb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/formancehq/go-libs/v3/metadata"
)

// UnmarshalJSON implements json.Unmarshaler for SavedMetadata
// Handles the special case where TargetID can be either a string (for ACCOUNT) or uint64 (for TRANSACTION)
func (sm *SavedMetadata) UnmarshalJSON(data []byte) error {
	type X struct {
		TargetType string            `json:"targetType"`
		TargetID   json.RawMessage   `json:"targetId"`
		Metadata   metadata.Metadata `json:"metadata"`
	}
	x := X{}
	err := json.Unmarshal(data, &x)
	if err != nil {
		return err
	}

	sm.TargetType = x.TargetType
	sm.Metadata = x.Metadata

	switch strings.ToUpper(x.TargetType) {
	case strings.ToUpper(MetaTargetTypeAccount):
		var accountID string
		err = json.Unmarshal(x.TargetID, &accountID)
		if err == nil {
			sm.TargetId = &SavedMetadata_AccountId{AccountId: accountID}
		}
	case strings.ToUpper(MetaTargetTypeTransaction):
		var txID uint64
		txID, err = strconv.ParseUint(string(x.TargetID), 10, 64)
		if err == nil {
			sm.TargetId = &SavedMetadata_TransactionId{TransactionId: txID}
		}
	default:
		return fmt.Errorf("unknown type '%s'", x.TargetType)
	}
	return err
}
