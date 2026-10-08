package filterexpr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestFormatAccountHasAsset(t *testing.T) {
	t.Parallel()

	hasAsset := func(base string, precision uint32) *ledgerpb.QueryFilter {
		return &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_AccountHasAsset{
				AccountHasAsset: &ledgerpb.AccountHasAssetCondition{AssetBase: base, Precision: precision},
			},
		}
	}

	t.Run("bare base renders without precision", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "has asset USD", Format(hasAsset("USD", 0)))
	})

	t.Run("base with precision", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "has asset USD/4", Format(hasAsset("USD", 4)))
	})

	t.Run("round-trips through Parse", func(t *testing.T) {
		t.Parallel()
		for _, in := range []string{"has asset USD", "has asset USD/2"} {
			f, err := Parse(in, tx)
			require.NoError(t, err)
			assert.Equal(t, in, Format(f), "round-trip for %q", in)
		}
	})
}

func TestFormat(t *testing.T) {
	t.Parallel()

	t.Run("nil filter", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", Format(nil))
	})

	t.Run("metadata string equality", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "category"},
					Condition: &ledgerpb.FieldCondition_StringCond{
						StringCond: &ledgerpb.StringCondition{
							Value: &ledgerpb.StringCondition_Hardcoded{Hardcoded: "premium"},
						},
					},
				},
			},
		}
		assert.Equal(t, "metadata[category] == premium", Format(f))
	})

	t.Run("metadata string with spaces is quoted", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "name"},
					Condition: &ledgerpb.FieldCondition_StringCond{
						StringCond: &ledgerpb.StringCondition{
							Value: &ledgerpb.StringCondition_Hardcoded{Hardcoded: "hello world"},
						},
					},
				},
			},
		}
		assert.Equal(t, `metadata[name] == "hello world"`, Format(f))
	})

	t.Run("metadata param", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "category"},
					Condition: &ledgerpb.FieldCondition_StringCond{
						StringCond: &ledgerpb.StringCondition{
							Value: &ledgerpb.StringCondition_Param{Param: "val"},
						},
					},
				},
			},
		}
		assert.Equal(t, "metadata[category] == $val", Format(f))
	})

	t.Run("metadata boolean true", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "active"},
					Condition: &ledgerpb.FieldCondition_BoolCond{
						BoolCond: &ledgerpb.BoolCondition{
							Value: &ledgerpb.BoolCondition_Hardcoded{Hardcoded: true},
						},
					},
				},
			},
		}
		assert.Equal(t, "metadata[active] == true", Format(f))
	})

	t.Run("metadata boolean false", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "active"},
					Condition: &ledgerpb.FieldCondition_BoolCond{
						BoolCond: &ledgerpb.BoolCondition{
							Value: &ledgerpb.BoolCondition_Hardcoded{Hardcoded: false},
						},
					},
				},
			},
		}
		assert.Equal(t, "metadata[active] == false", Format(f))
	})

	t.Run("metadata int equality", func(t *testing.T) {
		t.Parallel()
		v := int64(42)
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "age"},
					Condition: &ledgerpb.FieldCondition_IntCond{
						IntCond: &ledgerpb.IntCondition{Min: &v, Max: &v},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] == 42", Format(f))
	})

	t.Run("metadata int greater than", func(t *testing.T) {
		t.Parallel()
		v := int64(18)
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "age"},
					Condition: &ledgerpb.FieldCondition_IntCond{
						IntCond: &ledgerpb.IntCondition{Min: &v, MinExclusive: true},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] > 18", Format(f))
	})

	t.Run("metadata int greater than or equal", func(t *testing.T) {
		t.Parallel()
		v := int64(18)
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "age"},
					Condition: &ledgerpb.FieldCondition_IntCond{
						IntCond: &ledgerpb.IntCondition{Min: &v},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] >= 18", Format(f))
	})

	t.Run("metadata int less than", func(t *testing.T) {
		t.Parallel()
		v := int64(65)
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "age"},
					Condition: &ledgerpb.FieldCondition_IntCond{
						IntCond: &ledgerpb.IntCondition{Max: &v, MaxExclusive: true},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] < 65", Format(f))
	})

	t.Run("metadata int less than or equal", func(t *testing.T) {
		t.Parallel()
		v := int64(65)
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "age"},
					Condition: &ledgerpb.FieldCondition_IntCond{
						IntCond: &ledgerpb.IntCondition{Max: &v},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] <= 65", Format(f))
	})

	t.Run("metadata int param min", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "age"},
					Condition: &ledgerpb.FieldCondition_IntCond{
						IntCond: &ledgerpb.IntCondition{ParamMin: "min", MinExclusive: true},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] > $min", Format(f))
	})

	t.Run("metadata int param max", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field: &ledgerpb.FieldRef{Metadata: "age"},
					Condition: &ledgerpb.FieldCondition_IntCond{
						IntCond: &ledgerpb.IntCondition{ParamMax: "max", MaxExclusive: true},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] < $max", Format(f))
	})

	t.Run("metadata exists", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field:     &ledgerpb.FieldRef{Metadata: "category"},
					Condition: &ledgerpb.FieldCondition_ExistsCond{ExistsCond: &ledgerpb.ExistsCondition{}},
				},
			},
		}
		assert.Equal(t, "metadata[category] exists", Format(f))
	})

	t.Run("address exact", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Address{
				Address: &ledgerpb.AddressMatch{
					Match: &ledgerpb.AddressMatch_HardcodedExact{HardcodedExact: "users:alice"},
					Role:  ledgerpb.AddressRole_ADDRESS_ROLE_ANY,
				},
			},
		}
		assert.Equal(t, `address == "users:alice"`, Format(f))
	})

	t.Run("address prefix", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Address{
				Address: &ledgerpb.AddressMatch{
					Match: &ledgerpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "users:"},
					Role:  ledgerpb.AddressRole_ADDRESS_ROLE_ANY,
				},
			},
		}
		assert.Equal(t, `address ^= "users:"`, Format(f))
	})

	t.Run("source exact", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Address{
				Address: &ledgerpb.AddressMatch{
					Match: &ledgerpb.AddressMatch_HardcodedExact{HardcodedExact: "merchants:alice"},
					Role:  ledgerpb.AddressRole_ADDRESS_ROLE_SOURCE,
				},
			},
		}
		assert.Equal(t, `source == "merchants:alice"`, Format(f))
	})

	t.Run("destination prefix", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Address{
				Address: &ledgerpb.AddressMatch{
					Match: &ledgerpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "users:"},
					Role:  ledgerpb.AddressRole_ADDRESS_ROLE_DESTINATION,
				},
			},
		}
		assert.Equal(t, `destination ^= "users:"`, Format(f))
	})

	t.Run("address param exact", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Address{
				Address: &ledgerpb.AddressMatch{
					Match: &ledgerpb.AddressMatch_ParamExact{ParamExact: "addr"},
					Role:  ledgerpb.AddressRole_ADDRESS_ROLE_ANY,
				},
			},
		}
		assert.Equal(t, "address == $addr", Format(f))
	})

	t.Run("address param prefix", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Address{
				Address: &ledgerpb.AddressMatch{
					Match: &ledgerpb.AddressMatch_ParamPrefix{ParamPrefix: "prefix"},
					Role:  ledgerpb.AddressRole_ADDRESS_ROLE_ANY,
				},
			},
		}
		assert.Equal(t, "address ^= $prefix", Format(f))
	})

	t.Run("AND", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_And{
				And: &ledgerpb.AndFilter{
					Filters: []*ledgerpb.QueryFilter{
						fieldStringFilter("a", "x"),
						fieldStringFilter("b", "y"),
					},
				},
			},
		}
		assert.Equal(t, "metadata[a] == x and metadata[b] == y", Format(f))
	})

	t.Run("OR", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Or{
				Or: &ledgerpb.OrFilter{
					Filters: []*ledgerpb.QueryFilter{
						fieldStringFilter("a", "x"),
						fieldStringFilter("b", "y"),
					},
				},
			},
		}
		assert.Equal(t, "metadata[a] == x or metadata[b] == y", Format(f))
	})

	t.Run("NOT simple", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Not{
				Not: &ledgerpb.NotFilter{
					Filter: &ledgerpb.QueryFilter{
						Filter: &ledgerpb.QueryFilter_Address{
							Address: &ledgerpb.AddressMatch{
								Match: &ledgerpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "users:"},
								Role:  ledgerpb.AddressRole_ADDRESS_ROLE_ANY,
							},
						},
					},
				},
			},
		}
		assert.Equal(t, `not address ^= "users:"`, Format(f))
	})

	t.Run("NOT equality sugars to !=", func(t *testing.T) {
		t.Parallel()
		// not(metadata[category] == premium) → metadata[category] != premium
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Not{
				Not: &ledgerpb.NotFilter{
					Filter: fieldStringFilter("category", "premium"),
				},
			},
		}
		assert.Equal(t, "metadata[category] != premium", Format(f))
	})

	t.Run("NOT with grouping", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Not{
				Not: &ledgerpb.NotFilter{
					Filter: &ledgerpb.QueryFilter{
						Filter: &ledgerpb.QueryFilter_Or{
							Or: &ledgerpb.OrFilter{
								Filters: []*ledgerpb.QueryFilter{
									fieldStringFilter("a", "x"),
									fieldStringFilter("b", "y"),
								},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, "not (metadata[a] == x or metadata[b] == y)", Format(f))
	})

	t.Run("OR inside AND needs parens", func(t *testing.T) {
		t.Parallel()
		// (a or b) and c
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_And{
				And: &ledgerpb.AndFilter{
					Filters: []*ledgerpb.QueryFilter{
						{
							Filter: &ledgerpb.QueryFilter_Or{
								Or: &ledgerpb.OrFilter{
									Filters: []*ledgerpb.QueryFilter{
										fieldStringFilter("a", "x"),
										fieldStringFilter("b", "y"),
									},
								},
							},
						},
						fieldStringFilter("c", "z"),
					},
				},
			},
		}
		assert.Equal(t, "(metadata[a] == x or metadata[b] == y) and metadata[c] == z", Format(f))
	})

	t.Run("three-way AND", func(t *testing.T) {
		t.Parallel()
		f := &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_And{
				And: &ledgerpb.AndFilter{
					Filters: []*ledgerpb.QueryFilter{
						fieldStringFilter("a", "x"),
						fieldStringFilter("b", "y"),
						fieldStringFilter("c", "z"),
					},
				},
			},
		}
		assert.Equal(t, "metadata[a] == x and metadata[b] == y and metadata[c] == z", Format(f))
	})
}

func TestFormatRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string // expected output; empty means same as input
	}{
		{"simple metadata", "metadata[category] == premium", ""},
		{"metadata exists", "metadata[key] exists", ""},
		{"metadata int equality", "metadata[age] == 42", ""},
		{"metadata greater than", "metadata[age] > 18", ""},
		{"metadata less than or equal", "metadata[age] <= 65", ""},
		{"address prefix", `address ^= "users:"`, ""},
		{"source exact", `source == "merchants:alice"`, ""},
		{"destination prefix", `destination ^= "users:"`, ""},
		{"AND", "metadata[a] == x and metadata[b] == y", ""},
		{"OR", "metadata[a] == x or metadata[b] == y", ""},
		{"NOT address", `not address ^= "users:"`, ""},
		{"NOT equality sugars to !=", "metadata[category] != premium", ""},
		{"param string", "metadata[category] == $val", ""},
		{"param address", "address == $addr", ""},
		{"param int min", "metadata[age] > $min", ""},
		{"param int max", "metadata[age] <= $max", ""},
		{"complex: AND with range", "metadata[age] >= 18 and metadata[age] < 65", ""},
		{"between inclusive", "metadata[age] between 18 and 65", ""},
		{"between collapses to equality", "metadata[age] between 42 and 42", "metadata[age] == 42"},
		{"between with parameters", "metadata[age] between $low and $high", ""},
		{"between mixed param/literal", "metadata[age] between 18 and $max", ""},
		{"AND of ranges round-trips as between", "metadata[age] >= 18 and metadata[age] < 65", "metadata[age] >= 18 and metadata[age] < 65"},
		{"complex: source and destination", `source ^= "a:" and destination ^= "b:"`, ""},
		// EN-1549: a bare `ledger` condition on a non-audit target renders back to
		// the same textual form (previously formatted as "<unknown filter>", which
		// broke the prepared-query config export/apply round-trip).
		{"ledger exact", "ledger == main", ""},
		{"ledger quoted value", `ledger == "my-ledger"`, ""},
		{"ledger param", "ledger == $name", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parsed, err := Parse(tt.input, tx)
			require.NoError(t, err)

			got := Format(parsed)
			want := tt.want
			if want == "" {
				want = tt.input
			}
			assert.Equal(t, want, got)
		})
	}
}

// fieldStringFilter is a test helper that creates a simple metadata string equality filter.
func fieldStringFilter(key, value string) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field: &ledgerpb.FieldRef{Metadata: key},
				Condition: &ledgerpb.FieldCondition_StringCond{
					StringCond: &ledgerpb.StringCondition{
						Value: &ledgerpb.StringCondition_Hardcoded{Hardcoded: value},
					},
				},
			},
		},
	}
}

// uintField wraps a UintCondition into a metadata FieldCondition for the
// Format tests below — UintCondition has no surface syntax in the parser
// (range operators always emit IntCondition), so we build it directly.
func uintField(key string, uc *ledgerpb.UintCondition) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field:     &ledgerpb.FieldRef{Metadata: key},
				Condition: &ledgerpb.FieldCondition_UintCond{UintCond: uc},
			},
		},
	}
}

func TestFormatUintCondition(t *testing.T) {
	t.Parallel()

	t.Run("uint equality", func(t *testing.T) {
		t.Parallel()
		f := uintField("age", &ledgerpb.UintCondition{Min: new(uint64(42)), Max: new(uint64(42))})
		assert.Equal(t, "metadata[age] == 42", Format(f))
	})

	t.Run("uint greater than", func(t *testing.T) {
		t.Parallel()
		f := uintField("age", &ledgerpb.UintCondition{Min: new(uint64(18)), MinExclusive: true})
		assert.Equal(t, "metadata[age] > 18", Format(f))
	})

	t.Run("uint greater than or equal", func(t *testing.T) {
		t.Parallel()
		f := uintField("age", &ledgerpb.UintCondition{Min: new(uint64(18))})
		assert.Equal(t, "metadata[age] >= 18", Format(f))
	})

	t.Run("uint less than", func(t *testing.T) {
		t.Parallel()
		f := uintField("age", &ledgerpb.UintCondition{Max: new(uint64(65)), MaxExclusive: true})
		assert.Equal(t, "metadata[age] < 65", Format(f))
	})

	t.Run("uint less than or equal", func(t *testing.T) {
		t.Parallel()
		f := uintField("age", &ledgerpb.UintCondition{Max: new(uint64(65))})
		assert.Equal(t, "metadata[age] <= 65", Format(f))
	})

	t.Run("uint param min", func(t *testing.T) {
		t.Parallel()
		f := uintField("age", &ledgerpb.UintCondition{ParamMin: "min", MinExclusive: true})
		assert.Equal(t, "metadata[age] > $min", Format(f))
	})

	t.Run("uint param max", func(t *testing.T) {
		t.Parallel()
		f := uintField("age", &ledgerpb.UintCondition{ParamMax: "max"})
		assert.Equal(t, "metadata[age] <= $max", Format(f))
	})

	t.Run("uint between inclusive", func(t *testing.T) {
		t.Parallel()
		f := uintField("age", &ledgerpb.UintCondition{Min: new(uint64(18)), Max: new(uint64(65))})
		assert.Equal(t, "metadata[age] between 18 and 65", Format(f))
	})

	t.Run("uint between with exclusive bounds normalized", func(t *testing.T) {
		t.Parallel()
		// Lower exclusive becomes Min+1; upper exclusive becomes Max-1.
		f := uintField("age", &ledgerpb.UintCondition{
			Min: new(uint64(18)), MinExclusive: true,
			Max: new(uint64(65)), MaxExclusive: true,
		})
		assert.Equal(t, "metadata[age] between 19 and 64", Format(f))
	})

	t.Run("uint between with param bounds", func(t *testing.T) {
		t.Parallel()
		f := uintField("age", &ledgerpb.UintCondition{ParamMin: "lo", ParamMax: "hi"})
		assert.Equal(t, "metadata[age] between $lo and $hi", Format(f))
	})

	t.Run("uint with no bounds set falls back to placeholder", func(t *testing.T) {
		t.Parallel()
		f := uintField("x", &ledgerpb.UintCondition{})
		assert.Equal(t, "metadata[x] <uint?>", Format(f))
	})
}

func TestFormatIntConditionEdgeCases(t *testing.T) {
	t.Parallel()

	intField := func(key string, ic *ledgerpb.IntCondition) *ledgerpb.QueryFilter {
		return &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_Field{
				Field: &ledgerpb.FieldCondition{
					Field:     &ledgerpb.FieldRef{Metadata: key},
					Condition: &ledgerpb.FieldCondition_IntCond{IntCond: ic},
				},
			},
		}
	}

	t.Run("int between mixes hardcoded lower with param upper", func(t *testing.T) {
		t.Parallel()
		f := intField("age", &ledgerpb.IntCondition{Min: new(int64(18)), ParamMax: "hi"})
		assert.Equal(t, "metadata[age] between 18 and $hi", Format(f))
	})

	t.Run("int between mixes param lower with hardcoded upper", func(t *testing.T) {
		t.Parallel()
		f := intField("age", &ledgerpb.IntCondition{ParamMin: "lo", Max: new(int64(65))})
		assert.Equal(t, "metadata[age] between $lo and 65", Format(f))
	})

	t.Run("int between with exclusive bounds normalized", func(t *testing.T) {
		t.Parallel()
		f := intField("age", &ledgerpb.IntCondition{
			Min: new(int64(18)), MinExclusive: true,
			Max: new(int64(65)), MaxExclusive: true,
		})
		assert.Equal(t, "metadata[age] between 19 and 64", Format(f))
	})

	t.Run("int with no bounds set falls back to placeholder", func(t *testing.T) {
		t.Parallel()
		f := intField("x", &ledgerpb.IntCondition{})
		assert.Equal(t, "metadata[x] <int?>", Format(f))
	})
}
