package commonpb

import (
	"sort"

	"github.com/formancehq/go-libs/v3/metadata"
)

// Target type constants
const (
	MetaTargetTypeAccount     = "ACCOUNT"
	MetaTargetTypeTransaction = "TRANSACTION"
)

// MetadataFromMap converts a metadata.Metadata (map[string]string) to a []*Metadata slice
func MetadataFromMap(m metadata.Metadata) []*Metadata {
	if m == nil {
		return nil
	}
	result := make([]*Metadata, 0, len(m))
	for k, v := range m {
		result = append(result, &Metadata{Key: k, Value: v})
	}
	// Sort by key for deterministic output
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})
	return result
}

// MetadataSetFromMap converts a metadata.Metadata (map[string]string) to a *MetadataSet
func MetadataSetFromMap(m metadata.Metadata) *MetadataSet {
	if m == nil {
		return nil
	}
	return &MetadataSet{
		Metadata: MetadataFromMap(m),
	}
}

// MetadataToMap converts a []*Metadata slice to metadata.Metadata (map[string]string)
func MetadataToMap(m []*Metadata) metadata.Metadata {
	if m == nil {
		return nil
	}
	result := make(metadata.Metadata, len(m))
	for _, md := range m {
		if md != nil {
			result[md.Key] = md.Value
		}
	}
	return result
}

// MetadataSetToMap converts a *MetadataSet to metadata.Metadata (map[string]string)
func MetadataSetToMap(ms *MetadataSet) metadata.Metadata {
	if ms == nil {
		return nil
	}
	return MetadataToMap(ms.Metadata)
}

// NewMetadataSet creates a new MetadataSet from a metadata.Metadata map
func NewMetadataSet(m metadata.Metadata) *MetadataSet {
	return MetadataSetFromMap(m)
}

// ToMap converts a MetadataSet to metadata.Metadata (map[string]string)
func (ms *MetadataSet) ToMap() metadata.Metadata {
	return MetadataSetToMap(ms)
}

// AccountMetadataFromMap converts a map[string]metadata.Metadata to map[string]*MetadataSet
func AccountMetadataFromMap(m map[string]metadata.Metadata) map[string]*MetadataSet {
	if m == nil {
		return nil
	}
	result := make(map[string]*MetadataSet, len(m))
	for k, v := range m {
		result[k] = MetadataSetFromMap(v)
	}
	return result
}

// AccountMetadataToMap converts a map[string]*MetadataSet to map[string]metadata.Metadata
func AccountMetadataToMap(m map[string]*MetadataSet) map[string]metadata.Metadata {
	if m == nil {
		return nil
	}
	result := make(map[string]metadata.Metadata, len(m))
	for k, v := range m {
		result[k] = MetadataSetToMap(v)
	}
	return result
}
