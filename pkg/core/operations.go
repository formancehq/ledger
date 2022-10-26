package core

type AdditionalOperations struct {
	AccountMeta AccountsMeta `json:"set_account_meta" swaggertype:"object"`
}

type AccountsMeta map[string]Metadata
