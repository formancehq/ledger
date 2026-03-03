package filterexpr

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormat(t *testing.T) {
	t.Parallel()

	t.Run("nil filter", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", Format(nil))
	})

	t.Run("metadata string equality", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "category"},
					Condition: &commonpb.FieldCondition_StringCond{
						StringCond: &commonpb.StringCondition{
							Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "premium"},
						},
					},
				},
			},
		}
		assert.Equal(t, "metadata[category] == premium", Format(f))
	})

	t.Run("metadata string with spaces is quoted", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "name"},
					Condition: &commonpb.FieldCondition_StringCond{
						StringCond: &commonpb.StringCondition{
							Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "hello world"},
						},
					},
				},
			},
		}
		assert.Equal(t, `metadata[name] == "hello world"`, Format(f))
	})

	t.Run("metadata param", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "category"},
					Condition: &commonpb.FieldCondition_StringCond{
						StringCond: &commonpb.StringCondition{
							Value: &commonpb.StringCondition_Param{Param: "val"},
						},
					},
				},
			},
		}
		assert.Equal(t, "metadata[category] == $val", Format(f))
	})

	t.Run("metadata boolean true", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "active"},
					Condition: &commonpb.FieldCondition_BoolCond{
						BoolCond: &commonpb.BoolCondition{
							Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: true},
						},
					},
				},
			},
		}
		assert.Equal(t, "metadata[active] == true", Format(f))
	})

	t.Run("metadata boolean false", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "active"},
					Condition: &commonpb.FieldCondition_BoolCond{
						BoolCond: &commonpb.BoolCondition{
							Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: false},
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
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "age"},
					Condition: &commonpb.FieldCondition_IntCond{
						IntCond: &commonpb.IntCondition{Min: &v, Max: &v},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] == 42", Format(f))
	})

	t.Run("metadata int greater than", func(t *testing.T) {
		t.Parallel()
		v := int64(18)
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "age"},
					Condition: &commonpb.FieldCondition_IntCond{
						IntCond: &commonpb.IntCondition{Min: &v, MinExclusive: true},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] > 18", Format(f))
	})

	t.Run("metadata int greater than or equal", func(t *testing.T) {
		t.Parallel()
		v := int64(18)
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "age"},
					Condition: &commonpb.FieldCondition_IntCond{
						IntCond: &commonpb.IntCondition{Min: &v},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] >= 18", Format(f))
	})

	t.Run("metadata int less than", func(t *testing.T) {
		t.Parallel()
		v := int64(65)
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "age"},
					Condition: &commonpb.FieldCondition_IntCond{
						IntCond: &commonpb.IntCondition{Max: &v, MaxExclusive: true},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] < 65", Format(f))
	})

	t.Run("metadata int less than or equal", func(t *testing.T) {
		t.Parallel()
		v := int64(65)
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "age"},
					Condition: &commonpb.FieldCondition_IntCond{
						IntCond: &commonpb.IntCondition{Max: &v},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] <= 65", Format(f))
	})

	t.Run("metadata int param min", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "age"},
					Condition: &commonpb.FieldCondition_IntCond{
						IntCond: &commonpb.IntCondition{ParamMin: "min", MinExclusive: true},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] > $min", Format(f))
	})

	t.Run("metadata int param max", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "age"},
					Condition: &commonpb.FieldCondition_IntCond{
						IntCond: &commonpb.IntCondition{ParamMax: "max", MaxExclusive: true},
					},
				},
			},
		}
		assert.Equal(t, "metadata[age] < $max", Format(f))
	})

	t.Run("metadata exists", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field:     &commonpb.FieldRef{Metadata: "category"},
					Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
				},
			},
		}
		assert.Equal(t, "metadata[category] exists", Format(f))
	})

	t.Run("address exact", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "users:alice"},
					Role:  commonpb.AddressRole_ADDRESS_ROLE_ANY,
				},
			},
		}
		assert.Equal(t, "address == users:alice", Format(f))
	})

	t.Run("address prefix", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "users:"},
					Role:  commonpb.AddressRole_ADDRESS_ROLE_ANY,
				},
			},
		}
		assert.Equal(t, "address ^= users:", Format(f))
	})

	t.Run("source exact", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "merchants:alice"},
					Role:  commonpb.AddressRole_ADDRESS_ROLE_SOURCE,
				},
			},
		}
		assert.Equal(t, "source == merchants:alice", Format(f))
	})

	t.Run("destination prefix", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "users:"},
					Role:  commonpb.AddressRole_ADDRESS_ROLE_DESTINATION,
				},
			},
		}
		assert.Equal(t, "destination ^= users:", Format(f))
	})

	t.Run("address param exact", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_ParamExact{ParamExact: "addr"},
					Role:  commonpb.AddressRole_ADDRESS_ROLE_ANY,
				},
			},
		}
		assert.Equal(t, "address == $addr", Format(f))
	})

	t.Run("address param prefix", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_ParamPrefix{ParamPrefix: "prefix"},
					Role:  commonpb.AddressRole_ADDRESS_ROLE_ANY,
				},
			},
		}
		assert.Equal(t, "address ^= $prefix", Format(f))
	})

	t.Run("AND", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{
					Filters: []*commonpb.QueryFilter{
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
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Or{
				Or: &commonpb.OrFilter{
					Filters: []*commonpb.QueryFilter{
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
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Not{
				Not: &commonpb.NotFilter{
					Filter: &commonpb.QueryFilter{
						Filter: &commonpb.QueryFilter_Address{
							Address: &commonpb.AddressMatch{
								Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "users:"},
								Role:  commonpb.AddressRole_ADDRESS_ROLE_ANY,
							},
						},
					},
				},
			},
		}
		assert.Equal(t, "not address ^= users:", Format(f))
	})

	t.Run("NOT equality sugars to !=", func(t *testing.T) {
		t.Parallel()
		// not(metadata[category] == premium) → metadata[category] != premium
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Not{
				Not: &commonpb.NotFilter{
					Filter: fieldStringFilter("category", "premium"),
				},
			},
		}
		assert.Equal(t, "metadata[category] != premium", Format(f))
	})

	t.Run("NOT with grouping", func(t *testing.T) {
		t.Parallel()
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Not{
				Not: &commonpb.NotFilter{
					Filter: &commonpb.QueryFilter{
						Filter: &commonpb.QueryFilter_Or{
							Or: &commonpb.OrFilter{
								Filters: []*commonpb.QueryFilter{
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
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{
					Filters: []*commonpb.QueryFilter{
						{
							Filter: &commonpb.QueryFilter_Or{
								Or: &commonpb.OrFilter{
									Filters: []*commonpb.QueryFilter{
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
		f := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{
					Filters: []*commonpb.QueryFilter{
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
		{"address prefix", "address ^= users:", ""},
		{"source exact", `source == "merchants:alice"`, "source == merchants:alice"},
		{"destination prefix", `destination ^= "users:"`, "destination ^= users:"},
		{"AND", "metadata[a] == x and metadata[b] == y", ""},
		{"OR", "metadata[a] == x or metadata[b] == y", ""},
		{"NOT address", "not address ^= users:", ""},
		{"NOT equality sugars to !=", "metadata[category] != premium", ""},
		{"param string", "metadata[category] == $val", ""},
		{"param address", "address == $addr", ""},
		{"param int min", "metadata[age] > $min", ""},
		{"param int max", "metadata[age] <= $max", ""},
		{"complex: AND with range", "metadata[age] >= 18 and metadata[age] < 65", ""},
		{"complex: source and destination", `source ^= "a:" and destination ^= "b:"`, "source ^= a: and destination ^= b:"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parsed, err := Parse(tt.input)
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
func fieldStringFilter(key, value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: key},
				Condition: &commonpb.FieldCondition_StringCond{
					StringCond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value},
					},
				},
			},
		},
	}
}
