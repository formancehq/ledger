package commonpb

// ToLedgerInfo reconstructs the persisted LedgerInfo projection from creation
// fields. Initial metadata belongs in its canonical keyspace; response callers
// that need metadata must attach GetMetadata() separately.
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
