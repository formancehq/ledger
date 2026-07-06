package explain

import (
	"reflect"
	"strings"
)

// fieldsFromType builds a []Field tree by reflecting on a Go struct type.
// It extracts field names from json tags and maps Go types to schema type strings.
func fieldsFromType(t reflect.Type) []Field {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	var fields []Field

	for sf := range t.Fields() {
		if sf.Anonymous || !sf.IsExported() {
			continue
		}

		tag := sf.Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		jsonName := strings.Split(tag, ",")[0]
		if jsonName == "" {
			continue
		}

		ft := sf.Type
		isPtr := false
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
			isPtr = true
		}

		typeName, children := resolveType(ft)
		_ = isPtr

		immutable := sf.Tag.Get("ledger") == "immutable"

		fields = append(fields, Field{
			Name:      jsonName,
			Type:      typeName,
			Immutable: immutable,
			Children:  children,
		})
	}

	return fields
}

// resolveType maps a Go reflect.Type to an explain type string and optional children.
func resolveType(t reflect.Type) (string, []Field) {
	// Handle slices/arrays.
	if t.Kind() == reflect.Slice {
		elem := t.Elem()
		if elem.Kind() == reflect.Pointer {
			elem = elem.Elem()
		}

		switch {
		case elem.Kind() == reflect.String:
			return "[]string", nil
		case elem == reflect.TypeFor[int32]():
			return "[]int32", nil
		case elem.Kind() == reflect.Struct:
			children := fieldsFromType(elem)

			return "[]object", children
		default:
			return "[]" + typeName(elem), nil
		}
	}

	// Handle maps.
	if t.Kind() == reflect.Map {
		keyType := typeName(t.Key())
		valType := t.Elem()
		if valType.Kind() == reflect.Slice && valType.Elem().Kind() == reflect.String {
			return "map[" + keyType + "][]string", nil
		}

		return "map[" + keyType + "]" + typeName(valType), nil
	}

	// Handle structs: recurse if it's a local type, otherwise use the type name.
	if t.Kind() == reflect.Struct {
		pkg := t.PkgPath()
		switch {
		case isK8sType(pkg):
			return typeName(t), nil
		default:
			children := fieldsFromType(t)
			if len(children) > 0 {
				return "object", children
			}

			return typeName(t), nil
		}
	}

	// Primitives.
	switch t.Kind() {
	case reflect.Bool:
		return "bool", nil
	case reflect.Int32:
		return "int32", nil
	case reflect.Int64:
		return "int64", nil
	case reflect.String:
		return "string", nil
	default:
		return t.Kind().String(), nil
	}
}

// isK8sType returns true for types from k8s.io packages that should not be recursed into.
func isK8sType(pkg string) bool {
	return strings.Contains(pkg, "k8s.io/api/") ||
		strings.Contains(pkg, "k8s.io/apimachinery/pkg/api/resource") ||
		strings.Contains(pkg, "k8s.io/apimachinery/pkg/runtime")
}

// typeName returns a short display name for a Go type.
func typeName(t reflect.Type) string {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	name := t.Name()
	if name != "" {
		return name
	}

	return t.Kind().String()
}
