package metadata

type Owner interface {
	GetMetadata() map[string]string
}
