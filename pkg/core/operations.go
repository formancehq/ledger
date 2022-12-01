package core

type AdditionalOperations struct {
	SetAccountMeta AccountsMeta `json:"set_account_meta,omitempty"`
}

type AccountsMeta map[string]Metadata
