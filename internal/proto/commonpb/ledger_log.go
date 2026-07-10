package commonpb

// ToLedgerInfo reconstructs a LedgerInfo from the creation-time fields
// captured in the log. Useful for HTTP/CLI responses that need a full
// LedgerInfo after a CreateLedger operation.
func (x *CreatedLedgerLog) ToLedgerInfo() *LedgerInfo {
	if x == nil {
		return nil
	}

	return &LedgerInfo{
		Name:                   x.GetName(),
		Id:                     x.GetId(),
		CreatedAt:              x.GetCreatedAt(),
		MetadataSchema:         x.GetMetadataSchema(),
		Mode:                   x.GetMode(),
		MirrorSource:           x.GetMirrorSource(),
		AccountTypes:           x.GetAccountTypes(),
		DefaultEnforcementMode: x.GetDefaultEnforcementMode(),
	}
}
