package v1beta2

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func AddPrefixToFieldError(prefix string) func(t1 *field.Error) *field.Error {
	return func(t1 *field.Error) *field.Error {
		t1.Field = fmt.Sprintf("%s.%s", prefix, t1.Field)
		return t1
	}
}

func ValidateRequiredConfigValueOrReferenceOnly[T comparable, SRC interface {
	*ConfigSource | *corev1.EnvVarSource
}](key string, v T, source SRC) field.ErrorList {
	var zeroValue T
	ret := field.ErrorList{}
	if !(v == zeroValue || source == nil) {
		ret = append(ret, &field.Error{
			Type:     field.ErrorTypeDuplicate,
			Field:    key,
			BadValue: v,
			Detail:   fmt.Sprintf("Only '%s' OR '%sFrom' can be specified", key, key),
		})
	}
	return ret
}

func ValidateRequiredConfigValueOrReference[T comparable, SRC interface {
	*ConfigSource | *corev1.EnvVarSource
}](key string, v T, source SRC) field.ErrorList {
	var zeroValue T
	ret := field.ErrorList{}
	if v == zeroValue && source == nil {
		ret = append(ret, field.Invalid(
			field.NewPath(key),
			nil,
			fmt.Sprintf("Either '%s' or '%sFrom' must be specified", key, key),
		))
	}
	return append(ret, ValidateRequiredConfigValueOrReferenceOnly(key, v, source)...)
}

func SelectRequiredConfigValueOrReference[VALUE interface {
	string |
		int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64
}, SRC interface {
	*ConfigSource | *corev1.EnvVarSource
}](key, prefix string, v VALUE, src SRC) corev1.EnvVar {
	var (
		ret         corev1.EnvVar
		stringValue *string
	)
	switch v := any(v).(type) {
	case string:
		if v != "" {
			stringValue = &v
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		if v != 0 {
			value := fmt.Sprintf("%d", v)
			stringValue = &value
		}
	}
	if stringValue != nil {
		ret = EnvWithPrefix(prefix, key, *stringValue)
	} else {
		switch src := any(src).(type) {
		case *ConfigSource:
			ret = EnvFromWithPrefix(prefix, key, src.Env())
		case *corev1.EnvVarSource:
			ret = corev1.EnvVar{
				Name:      prefix + key,
				ValueFrom: src,
			}
		}
	}
	return ret
}
