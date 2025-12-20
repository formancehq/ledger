package service

import (
	"github.com/formancehq/go-libs/v3/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

func StructToMetadata(s *structpb.Struct) metadata.Metadata {
	if s == nil {
		return metadata.Metadata{}
	}
	md := make(metadata.Metadata)
	for k, v := range s.Fields {
		md[k] = v.GetStringValue()
	}
	return md
}

func MetadataToStruct(md metadata.Metadata) (*structpb.Struct, error) {
	if len(md) == 0 {
		return nil, nil
	}
	fields := make(map[string]*structpb.Value)
	for k, v := range md {
		val, err := structpb.NewValue(v)
		if err != nil {
			return nil, err
		}
		fields[k] = val
	}
	return &structpb.Struct{Fields: fields}, nil
}
