package compiler

import (
	"bytes"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"github.com/formancehq/ledger/internal/machine"

	program2 "github.com/formancehq/ledger/internal/machine/vm/program"
	"github.com/stretchr/testify/require"
)

type TestCase struct {
	Case     string
	Expected CaseResult
}

type CaseResult struct {
	Instructions []byte
	Resources    []program2.Resource
	Variables    []string
	Error        string
}

func test(t *testing.T, c TestCase) {
	p, err := Compile(c.Case)
	if c.Expected.Error != "" {
		require.Error(t, err)
		require.NotEmpty(t, err.Error())
		require.ErrorContains(t, err, c.Expected.Error)
		return
	}
	require.NoError(t, err)
	require.NotNil(t, p)

	if len(c.Expected.Instructions) > 0 && !bytes.Equal(p.Instructions, c.Expected.Instructions) {
		t.Error(fmt.Errorf(
			"unexpected instructions:\n%v\nhas: %+v\nwant:%+v",
			*p, p.Instructions, c.Expected.Instructions))
		return
	} else if len(p.Resources) != len(c.Expected.Resources) {
		t.Error(fmt.Errorf(
			"unexpected resources\n%v\nhas: \n%+v\nwant:\n%+v",
			*p, p.Resources, c.Expected.Resources))
		return
	}

	for i, expected := range c.Expected.Resources {
		if !checkResourcesEqual(p.Resources[i], c.Expected.Resources[i]) {
			t.Error(fmt.Errorf("%v: %v is not %v: %v",
				p.Resources[i], reflect.TypeOf(p.Resources[i]).Name(),
				expected, reflect.TypeOf(expected).Name(),
			))
			t.Error(fmt.Errorf(
				"unexpected resources\n%v\nhas: \n%+v\nwant:\n%+v",
				*p, p.Resources, c.Expected.Resources))
			return
		}
	}
}

func checkResourcesEqual(actual, expected program2.Resource) bool {
	if reflect.TypeOf(actual) != reflect.TypeOf(expected) {
		return false
	}
	switch res := actual.(type) {
	case program2.Constant:
		return machine.ValueEquals(res.Inner, expected.(program2.Constant).Inner)
	case program2.Variable:
		e := expected.(program2.Variable)
		return res.Typ == e.Typ && res.Name == e.Name
	case program2.VariableAccountMetadata:
		e := expected.(program2.VariableAccountMetadata)
		return res.Account == e.Account &&
			res.Key == e.Key &&
			res.Typ == e.Typ
	case program2.VariableAccountBalance:
		e := expected.(program2.VariableAccountBalance)
		return res.Account == e.Account &&
			res.Asset == e.Asset
	case program2.Monetary:
		e := expected.(program2.Monetary)
		return res.Amount.Equal(e.Amount) && res.Asset == e.Asset
	default:
		panic(fmt.Errorf("invalid resource of type '%T'", res))
	}
}

func TestSimplePrint(t *testing.T) {
	test(t, TestCase{
		Case: "print 1",
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 00, 00,
				program2.OP_PRINT,
			},
			Resources: []program2.Resource{
				program2.Constant{Inner: machine.NewMonetaryInt(1)},
			},
		},
	})
}

func TestCompositeExpr(t *testing.T) {
	test(t, TestCase{
		Case: "print 29 + 15 - 2",
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 00, 00,
				program2.OP_APUSH, 01, 00,
				program2.OP_IADD,
				program2.OP_APUSH, 02, 00,
				program2.OP_ISUB,
				program2.OP_PRINT,
			},
			Resources: []program2.Resource{
				program2.Constant{Inner: machine.NewMonetaryInt(29)},
				program2.Constant{Inner: machine.NewMonetaryInt(15)},
				program2.Constant{Inner: machine.NewMonetaryInt(2)},
			},
		},
	})
}

func TestFail(t *testing.T) {
	test(t, TestCase{
		Case: "fail",
		Expected: CaseResult{
			Instructions: []byte{program2.OP_FAIL},
			Resources:    []program2.Resource{},
		},
	})
}

func TestCRLF(t *testing.T) {
	test(t, TestCase{
		Case: "print @a\r\nprint @b",
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 00, 00,
				program2.OP_PRINT,
				program2.OP_APUSH, 01, 00,
				program2.OP_PRINT,
			},
			Resources: []program2.Resource{
				program2.Constant{Inner: machine.AccountAddress("a")},
				program2.Constant{Inner: machine.AccountAddress("b")},
			},
		},
	})
}

func TestConstant(t *testing.T) {
	user := machine.AccountAddress("user:U001")
	test(t, TestCase{
		Case: "print @user:U001",
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 00, 00,
				program2.OP_PRINT,
			},
			Resources: []program2.Resource{program2.Constant{Inner: user}},
		},
	})
}

func TestSetTxMeta(t *testing.T) {
	test(t, TestCase{
		Case: `
		set_tx_meta("aaa", @platform)
		set_tx_meta("bbb", GEM)
		set_tx_meta("ccc", 42)
		set_tx_meta("ddd", "test")
		set_tx_meta("eee", [COIN 30])
		set_tx_meta("fff", 15%)
		`,
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 00, 00,
				program2.OP_APUSH, 01, 00,
				program2.OP_TX_META,
				program2.OP_APUSH, 02, 00,
				program2.OP_APUSH, 03, 00,
				program2.OP_TX_META,
				program2.OP_APUSH, 04, 00,
				program2.OP_APUSH, 05, 00,
				program2.OP_TX_META,
				program2.OP_APUSH, 06, 00,
				program2.OP_APUSH, 07, 00,
				program2.OP_TX_META,
				program2.OP_APUSH, 9, 00,
				program2.OP_APUSH, 10, 00,
				program2.OP_TX_META,
				program2.OP_APUSH, 11, 00,
				program2.OP_APUSH, 12, 00,
				program2.OP_TX_META,
			},
			Resources: []program2.Resource{
				program2.Constant{Inner: machine.AccountAddress("platform")},
				program2.Constant{Inner: machine.String("aaa")},
				program2.Constant{Inner: machine.Asset("GEM")},
				program2.Constant{Inner: machine.String("bbb")},
				program2.Constant{Inner: machine.NewNumber(42)},
				program2.Constant{Inner: machine.String("ccc")},
				program2.Constant{Inner: machine.String("test")},
				program2.Constant{Inner: machine.String("ddd")},
				program2.Constant{Inner: machine.Asset("COIN")},
				program2.Monetary{Asset: 8, Amount: machine.NewMonetaryInt(30)},
				program2.Constant{Inner: machine.String("eee")},
				program2.Constant{Inner: machine.Portion{
					Remaining: false,
					Specific:  big.NewRat(15, 100),
				}},
				program2.Constant{Inner: machine.String("fff")},
			},
		},
	})
}

func TestSetTxMetaVars(t *testing.T) {
	test(t, TestCase{
		Case: `
		vars {
			portion $commission
		}
		set_tx_meta("fee", $commission)
		`,
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 00, 00,
				program2.OP_APUSH, 01, 00,
				program2.OP_TX_META,
			},
			Resources: []program2.Resource{
				program2.Variable{Typ: machine.TypePortion, Name: "commission"},
				program2.Constant{Inner: machine.String("fee")},
			},
		},
	})
}

func TestComments(t *testing.T) {
	test(t, TestCase{
		Case: `
		/* This is a multi-line comment, it spans multiple lines
		and /* doesn't choke on nested comments */ ! */
		vars {
			account $a
		}
		// this is a single-line comment
		print $a
		`,
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 00, 00,
				program2.OP_PRINT,
			},
			Resources: []program2.Resource{
				program2.Variable{Typ: machine.TypeAccount, Name: "a"},
			},
		},
	})
}

func TestUndeclaredVariable(t *testing.T) {
	test(t, TestCase{
		Case: "print $nope",
		Expected: CaseResult{
			Error: "declared",
		},
	})
}

func TestInvalidTypeInSendValue(t *testing.T) {
	test(t, TestCase{
		Case: `
		send @a (
			source = {
				@a
				[GEM 2]
			}
			destination = @b
		)`,
		Expected: CaseResult{
			Error: "send monetary: the expression should be of type 'monetary' instead of 'account'",
		},
	})
}

func TestInvalidTypeInSource(t *testing.T) {
	test(t, TestCase{
		Case: `
		send [USD/2 99] (
			source = {
				@a
				[GEM 2]
			}
			destination = @b
		)`,
		Expected: CaseResult{
			Error: "wrong type",
		},
	})
}

func TestDestinationAllotment(t *testing.T) {
	test(t, TestCase{
		Case: `send [EUR/2 43] (
			source = @foo
			destination = {
				1/8 to @bar
				7/8 to @baz
			}
		)`,
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 02, 00, // @foo
				program2.OP_APUSH, 01, 00, // @foo, [EUR/2 43]
				program2.OP_ASSET,         // @foo, EUR/2
				program2.OP_APUSH, 03, 00, // @foo, EUR/2, 0
				program2.OP_MONETARY_NEW,  // @foo, [EUR/2 0]
				program2.OP_TAKE_ALL,      // [EUR/2 @foo <?>]
				program2.OP_APUSH, 01, 00, // [EUR/2 @foo <?>], [EUR/2 43]
				program2.OP_TAKE,          // [EUR/2 @foo <?>], [EUR/2 @foo 43]
				program2.OP_APUSH, 04, 00, // [EUR/2 @foo <?>], [EUR/2 @foo 43] 1
				program2.OP_BUMP,          // [EUR/2 @foo 43], [EUR/2 @foo <?>]
				program2.OP_REPAY,         // [EUR/2 @foo 43]
				program2.OP_FUNDING_SUM,   // [EUR/2 @foo 43], [EUR/2 43]
				program2.OP_APUSH, 05, 00, // [EUR/2 @foo 43], [EUR/2 43], 7/8
				program2.OP_APUSH, 06, 00, // [EUR/2 @foo 43], [EUR/2 43], 7/8, 1/8
				program2.OP_APUSH, 07, 00, // [EUR/2 @foo 43], [EUR/2 43], 7/8, 1/8, 2
				program2.OP_MAKE_ALLOTMENT, // [EUR/2 @foo 43], [EUR/2 43], {1/8 : 7/8}
				program2.OP_ALLOC,          // [EUR/2 @foo 43], [EUR/2 37], [EUR/2 6]
				program2.OP_APUSH, 07, 00,  // [EUR/2 @foo 43], [EUR/2 37] [EUR/2 6], 2
				program2.OP_BUMP,          // [EUR/2 37], [EUR/2 6], [EUR/2 @foo 43]
				program2.OP_APUSH, 04, 00, // [EUR/2 37], [EUR/2 6], [EUR/2 @foo 43] 1
				program2.OP_BUMP,         // [EUR/2 37], [EUR/2 @foo 43], [EUR/2 6]
				program2.OP_TAKE,         // [EUR/2 37], [EUR/2 @foo 37], [EUR/2 @foo 6]
				program2.OP_FUNDING_SUM,  // [EUR/2 37], [EUR/2 @foo 37], [EUR/2 @foo 6] [EUR/2 6]
				program2.OP_TAKE,         // [EUR/2 37], [EUR/2 @foo 37], [EUR/2] [EUR/2 @foo 6]
				program2.OP_APUSH, 8, 00, // [EUR/2 37], [EUR/2 @foo 37], [EUR/2] [EUR/2 @foo 6], @bar
				program2.OP_SEND,          // [EUR/2 37], [EUR/2 @foo 37], [EUR/2]
				program2.OP_APUSH, 04, 00, // [EUR/2 37], [EUR/2 @foo 37], [EUR/2] 1
				program2.OP_BUMP,          // [EUR/2 37], [EUR/2], [EUR/2 @foo 37]
				program2.OP_APUSH, 07, 00, // [EUR/2 37], [EUR/2], [EUR/2 @foo 37] 2
				program2.OP_FUNDING_ASSEMBLE, // [EUR/2 37], [EUR/2 @foo 37]
				program2.OP_APUSH, 04, 00,    // [EUR/2 37], [EUR/2 @foo 37], 1
				program2.OP_BUMP,         // [EUR/2 @foo 37], [EUR/2 37]
				program2.OP_TAKE,         // [EUR/2], [EUR/2 @foo 37]
				program2.OP_FUNDING_SUM,  // [EUR/2], [EUR/2 @foo 37], [EUR/2 37]
				program2.OP_TAKE,         // [EUR/2], [EUR/2], [EUR/2 @foo 37]
				program2.OP_APUSH, 9, 00, // [EUR/2], [EUR/2], [EUR/2 @foo 37], @baz
				program2.OP_SEND,          // [EUR/2], [EUR/2]
				program2.OP_APUSH, 04, 00, // [EUR/2], [EUR/2], 1
				program2.OP_BUMP,          // [EUR/2], [EUR/2]
				program2.OP_APUSH, 07, 00, // [EUR/2], [EUR/2], 2
				program2.OP_FUNDING_ASSEMBLE, // [EUR/2]
				program2.OP_REPAY,            //
			},
			Resources: []program2.Resource{
				program2.Constant{Inner: machine.Asset("EUR/2")},
				program2.Monetary{
					Asset:  0,
					Amount: machine.NewMonetaryInt(43),
				},
				program2.Constant{Inner: machine.AccountAddress("foo")},
				program2.Constant{Inner: machine.NewMonetaryInt(0)},
				program2.Constant{Inner: machine.NewMonetaryInt(1)},
				program2.Constant{Inner: machine.Portion{Specific: big.NewRat(7, 8)}},
				program2.Constant{Inner: machine.Portion{Specific: big.NewRat(1, 8)}},
				program2.Constant{Inner: machine.NewMonetaryInt(2)},
				program2.Constant{Inner: machine.AccountAddress("bar")},
				program2.Constant{Inner: machine.AccountAddress("baz")},
			},
		},
	})
}

func TestDestinationInOrder(t *testing.T) {
	test(t, TestCase{
		Case: `send [COIN 50] (
			source = @a
			destination = {
				max [COIN 10] to @b
				remaining to @c
			}
		)`,
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 02, 00, // @a
				program2.OP_APUSH, 01, 00, // @a, [COIN 50]
				program2.OP_ASSET,         // @a, COIN
				program2.OP_APUSH, 03, 00, // @a, COIN, 0
				program2.OP_MONETARY_NEW,  // @a, [COIN 0]
				program2.OP_TAKE_ALL,      // [COIN @a <?>]
				program2.OP_APUSH, 01, 00, // [COIN @a <?>], [COIN 50]
				program2.OP_TAKE,          // [COIN @a <?>], [COIN @a 50]
				program2.OP_APUSH, 04, 00, // [COIN @a <?>], [COIN @a 50], 1
				program2.OP_BUMP,          // [COIN @a 50], [COIN @a <?>]
				program2.OP_REPAY,         // [COIN @a 50]
				program2.OP_FUNDING_SUM,   // [COIN @a 50], [COIN 50] <- start of DestinationInOrder
				program2.OP_ASSET,         // [COIN @a 50], COIN
				program2.OP_APUSH, 03, 00, // [COIN @a 50], COIN, 0
				program2.OP_MONETARY_NEW,  // [COIN @a 50], [COIN 0]
				program2.OP_APUSH, 04, 00, // [COIN @a 50], [COIN 0], 1
				program2.OP_BUMP,          // [COIN 0], [COIN @a 50]
				program2.OP_APUSH, 05, 00, // [COIN 0], [COIN @a 50], [COIN 10] <- start processing max subdestinations
				program2.OP_TAKE_MAX,      // [COIN 0], [COIN 0], [COIN @a 40], [COIN @a 10]
				program2.OP_APUSH, 06, 00, // [COIN 0], [COIN 0], [COIN @a 40], [COIN @a 10], 2
				program2.OP_BUMP,          // [COIN 0], [COIN @a 40], [COIN @a 10], [COIN 0]
				program2.OP_DELETE,        // [COIN 0], [COIN @a 40], [COIN @a 10]
				program2.OP_FUNDING_SUM,   // [COIN 0], [COIN @a 40], [COIN @a 10], [COIN 10]
				program2.OP_TAKE,          // [COIN 0], [COIN @a 40], [COIN], [COIN @a 10]
				program2.OP_APUSH, 07, 00, // [COIN 0], [COIN @a 40], [COIN], [COIN @a 10], @b
				program2.OP_SEND,         // [COIN 0], [COIN @a 40], [COIN]
				program2.OP_FUNDING_SUM,  // [COIN 0], [COIN @a 40], [COIN], [COIN 0]
				program2.OP_APUSH, 8, 00, // [COIN 0], [COIN @a 40], [COIN], [COIN 0], 3
				program2.OP_BUMP,          // [COIN @a 40], [COIN], [COIN 0], [COIN 0]
				program2.OP_MONETARY_ADD,  // [COIN @a 40], [COIN], [COIN 0]
				program2.OP_APUSH, 04, 00, // [COIN @a 40], [COIN], [COIN 0], 1
				program2.OP_BUMP,          // [COIN @a 40], [COIN 0], [COIN]
				program2.OP_APUSH, 06, 00, // [COIN @a 40], [COIN 0], [COIN] 2
				program2.OP_BUMP,          // [COIN 0], [COIN], [COIN @a 40]
				program2.OP_APUSH, 06, 00, // [COIN 0], [COIN], [COIN @a 40], 2
				program2.OP_FUNDING_ASSEMBLE, // [COIN 0], [COIN @a 40]
				program2.OP_FUNDING_REVERSE,  // [COIN 0], [COIN @a 40] <- start processing remaining subdestination
				program2.OP_APUSH, 04, 00,    // [COIN 0], [COIN @a 40], 1
				program2.OP_BUMP,            // [COIN @a 40], [COIN 0]
				program2.OP_TAKE,            // [COIN @a 40], [COIN]
				program2.OP_FUNDING_REVERSE, // [COIN @a 40], [COIN]
				program2.OP_APUSH, 04, 00,   // [COIN @a 40], [COIN], 1
				program2.OP_BUMP,            // [COIN], [COIN @a 40]
				program2.OP_FUNDING_REVERSE, // [COIN], [COIN @a 40]
				program2.OP_FUNDING_SUM,     // [COIN], [COIN @a 40], [COIN 40]
				program2.OP_TAKE,            // [COIN], [COIN], [COIN @a 40]
				program2.OP_APUSH, 9, 00,    // [COIN], [COIN], [COIN @a 40], @c
				program2.OP_SEND,          // [COIN], [COIN]
				program2.OP_APUSH, 04, 00, // [COIN], [COIN], 1
				program2.OP_BUMP,          // [COIN], [COIN]
				program2.OP_APUSH, 06, 00, // [COIN], [COIN], 2
				program2.OP_FUNDING_ASSEMBLE, // [COIN]
				program2.OP_REPAY,            //
			},
			Resources: []program2.Resource{
				program2.Constant{Inner: machine.Asset("COIN")},
				program2.Monetary{
					Asset:  0,
					Amount: machine.NewMonetaryInt(50),
				},
				program2.Constant{Inner: machine.AccountAddress("a")},
				program2.Constant{Inner: machine.NewMonetaryInt(0)},
				program2.Constant{Inner: machine.NewMonetaryInt(1)},
				program2.Monetary{
					Asset:  0,
					Amount: machine.NewMonetaryInt(10),
				},
				program2.Constant{Inner: machine.NewMonetaryInt(2)},
				program2.Constant{Inner: machine.AccountAddress("b")},
				program2.Constant{Inner: machine.NewMonetaryInt(3)},
				program2.Constant{Inner: machine.AccountAddress("c")},
			},
		},
	})
}

func TestAllocationPercentages(t *testing.T) {
	test(t, TestCase{
		Case: `send [EUR/2 43] (
			source = @foo
			destination = {
				12.5% to @bar
				37.5% to @baz
				50% to @qux
			}
		)`,
		Expected: CaseResult{
			Resources: []program2.Resource{
				program2.Constant{Inner: machine.Asset("EUR/2")},
				program2.Monetary{
					Asset:  0,
					Amount: machine.NewMonetaryInt(43),
				},
				program2.Constant{Inner: machine.AccountAddress("foo")},
				program2.Constant{Inner: machine.NewMonetaryInt(0)},
				program2.Constant{Inner: machine.NewMonetaryInt(1)},
				program2.Constant{Inner: machine.Portion{Specific: big.NewRat(1, 2)}},
				program2.Constant{Inner: machine.Portion{Specific: big.NewRat(3, 8)}},
				program2.Constant{Inner: machine.Portion{Specific: big.NewRat(1, 8)}},
				program2.Constant{Inner: machine.NewMonetaryInt(3)},
				program2.Constant{Inner: machine.AccountAddress("bar")},
				program2.Constant{Inner: machine.NewMonetaryInt(2)},
				program2.Constant{Inner: machine.AccountAddress("baz")},
				program2.Constant{Inner: machine.AccountAddress("qux")},
			},
		},
	})
}

func TestSend(t *testing.T) {
	script := `
		send [EUR/2 99] (
			source = @alice
			destination = @bob
		)`
	alice := machine.AccountAddress("alice")
	bob := machine.AccountAddress("bob")
	test(t, TestCase{
		Case: script,
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 02, 00, // @alice
				program2.OP_APUSH, 01, 00, // @alice, [EUR/2 99]
				program2.OP_ASSET,         // @alice, EUR/2
				program2.OP_APUSH, 03, 00, // @alice, EUR/2, 0
				program2.OP_MONETARY_NEW,  // @alice, [EUR/2 0]
				program2.OP_TAKE_ALL,      // [EUR/2 @alice <?>]
				program2.OP_APUSH, 01, 00, // [EUR/2 @alice <?>], [EUR/2 99]
				program2.OP_TAKE,          // [EUR/2 @alice <?>], [EUR/2 @alice 99]
				program2.OP_APUSH, 04, 00, // [EUR/2 @alice <?>], [EUR/2 @alice 99], 1
				program2.OP_BUMP,          // [EUR/2 @alice 99], [EUR/2 @alice <?>]
				program2.OP_REPAY,         // [EUR/2 @alice 99]
				program2.OP_FUNDING_SUM,   // [EUR/2 @alice 99], [EUR/2 99]
				program2.OP_TAKE,          // [EUR/2], [EUR/2 @alice 99]
				program2.OP_APUSH, 05, 00, // [EUR/2], [EUR/2 @alice 99], @bob
				program2.OP_SEND,  // [EUR/2]
				program2.OP_REPAY, //
			}, Resources: []program2.Resource{
				program2.Constant{Inner: machine.Asset("EUR/2")},
				program2.Monetary{
					Asset:  0,
					Amount: machine.NewMonetaryInt(99),
				},
				program2.Constant{Inner: alice},
				program2.Constant{Inner: machine.NewMonetaryInt(0)},
				program2.Constant{Inner: machine.NewMonetaryInt(1)},
				program2.Constant{Inner: bob}},
		},
	})
}

func TestSendAll(t *testing.T) {
	test(t, TestCase{
		Case: `send [EUR/2 *] (
			source = @alice
			destination = @bob
		)`,
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 01, 00, // @alice
				program2.OP_APUSH, 00, 00, // @alice, EUR/2
				program2.OP_APUSH, 02, 00, // @alice, EUR/2, 0
				program2.OP_MONETARY_NEW,  // @alice, [EUR/2 0]
				program2.OP_TAKE_ALL,      // [EUR/2 @alice <?>]
				program2.OP_FUNDING_SUM,   // [EUR/2 @alice <?>], [EUR/2 <?>]
				program2.OP_TAKE,          // [EUR/2], [EUR/2 @alice <?>]
				program2.OP_APUSH, 03, 00, // [EUR/2], [EUR/2 @alice <?>], @b
				program2.OP_SEND,  // [EUR/2]
				program2.OP_REPAY, //
			}, Resources: []program2.Resource{
				program2.Constant{Inner: machine.Asset("EUR/2")},
				program2.Constant{Inner: machine.AccountAddress("alice")},
				program2.Constant{Inner: machine.NewMonetaryInt(0)},
				program2.Constant{Inner: machine.AccountAddress("bob")}},
		},
	})
}

func TestMetadata(t *testing.T) {
	test(t, TestCase{
		Case: `
		vars {
			account $sale
			account $seller = meta($sale, "seller")
			portion $commission = meta($seller, "commission")
		}
		send [EUR/2 53] (
			source = $sale
			destination = {
				$commission to @platform
				remaining to $seller
			}
		)`,
		Expected: CaseResult{
			Resources: []program2.Resource{
				program2.Variable{Typ: machine.TypeAccount, Name: "sale"},
				program2.VariableAccountMetadata{
					Typ:     machine.TypeAccount,
					Account: machine.NewAddress(0),
					Key:     "seller",
				},
				program2.VariableAccountMetadata{
					Typ:     machine.TypePortion,
					Account: machine.NewAddress(1),
					Key:     "commission",
				},
				program2.Constant{Inner: machine.Asset("EUR/2")},
				program2.Monetary{
					Asset:  3,
					Amount: machine.NewMonetaryInt(53),
				},
				program2.Constant{Inner: machine.NewMonetaryInt(0)},
				program2.Constant{Inner: machine.NewMonetaryInt(1)},
				program2.Constant{Inner: machine.NewPortionRemaining()},
				program2.Constant{Inner: machine.NewMonetaryInt(2)},
				program2.Constant{Inner: machine.AccountAddress("platform")},
			},
		},
	})
}

func TestSyntaxError(t *testing.T) {
	test(t, TestCase{
		Case: "print fail",
		Expected: CaseResult{
			Error: "mismatched input",
		},
	})
}

func TestLogicError(t *testing.T) {
	test(t, TestCase{
		Case: `send [EUR/2 200] (
			source = 200
			destination = @bob
		)`,
		Expected: CaseResult{
			Error: "expected",
		},
	})
}

func TestPreventTakeAllFromWorld(t *testing.T) {
	test(t, TestCase{
		Case: `send [GEM *] (
			source = @world
			destination = @foo
		)`,
		Expected: CaseResult{
			Error: "cannot",
		},
	})
}

func TestPreventAddToBottomlessSource(t *testing.T) {
	test(t, TestCase{
		Case: `send [GEM 1000] (
			source = {
				@a
				@world
				@c
			}
			destination = @out
		)`,
		Expected: CaseResult{
			Error: "world",
		},
	})
}

func TestPreventAddToBottomlessSource2(t *testing.T) {
	test(t, TestCase{
		Case: `send [GEM 1000] (
			source = {
				{
					@a
					@world
				}
				{
					@b
					@world
				}
			}
			destination = @out
		)`,
		Expected: CaseResult{
			Error: "world",
		},
	})
}

func TestPreventSourceAlreadyEmptied(t *testing.T) {
	test(t, TestCase{
		Case: `send [GEM 1000] (
			source = {
				{
					@a
					@b
				}
				@a
			}
			destination = @out
		)`,
		Expected: CaseResult{
			Error: "empt",
		},
	})
}

func TestPreventTakeAllFromAllocation(t *testing.T) {
	test(t, TestCase{
		Case: `send [GEM *] (
			source = {
				50% from @a
				50% from @b
			}
			destination = @out
		)`,
		Expected: CaseResult{
			Error: "all",
		},
	})
}

func TestWrongTypeSourceMax(t *testing.T) {
	test(t, TestCase{
		Case: `send [GEM 15] (
			source = {
				max @foo from @bar
				@world
			}
			destination = @baz
		)`,
		Expected: CaseResult{
			Error: "type",
		},
	})
}

func TestOverflowingSources(t *testing.T) {
	t.Run(">100%", func(t *testing.T) {
		test(t, TestCase{
			Case: `send [COIN 100] (
				source = {
					1/2 from @alice
					3/4 from @bob
				}
				destination = @dest
			)`,
			Expected: CaseResult{
				Error: "greater than 100%",
			},
		})
	})

	t.Run("<100%", func(t *testing.T) {
		test(t, TestCase{
			Case: `send [COIN 100] (
				source = {
					1/2 from @world
				}
				destination = @dest
			)`,
			Expected: CaseResult{
				Error: "less than 100%",
			},
		})
	})

}

func TestOverflowingAllocation(t *testing.T) {
	t.Run(">100%", func(t *testing.T) {
		test(t, TestCase{
			Case: `send [GEM 15] (
				source = @world
				destination = {
					2/3 to @a
					2/3 to @b
				}
			)`,
			Expected: CaseResult{
				Error: "100%",
			},
		})
	})

	t.Run("=100% + remaining", func(t *testing.T) {
		test(t, TestCase{
			Case: `send [GEM 15] (
				source = @world
				destination = {
					1/2 to @a
					1/2 to @b
					remaining to @c
				}
			)`,
			Expected: CaseResult{
				Error: "100%",
			},
		})
	})

	t.Run(">100% + remaining", func(t *testing.T) {
		test(t, TestCase{
			Case: `send [GEM 15] (
				source = @world
				destination = {
					2/3 to @a
					1/2 to @b
					remaining to @c
				}
			)`,
			Expected: CaseResult{
				Error: "100%",
			},
		})
	})

	t.Run("const remaining + remaining", func(t *testing.T) {
		test(t, TestCase{
			Case: `send [GEM 15] (
				source = @world
				destination = {
					2/3 to @a
					remaining to @b
					remaining to @c
				}
			)`,
			Expected: CaseResult{
				Error: "`remaining` in the same",
			},
		})
	})

	t.Run("dyn remaining + remaining", func(t *testing.T) {
		test(t, TestCase{
			Case: `vars {
				portion $p
			}
			send [GEM 15] (
				source = @world
				destination = {
					$p to @a
					remaining to @b
					remaining to @c
				}
			)`,
			Expected: CaseResult{
				Error: "`remaining` in the same",
			},
		})
	})

	t.Run(">100% + remaining + variable", func(t *testing.T) {
		test(t, TestCase{
			Case: `vars {
				portion $prop
			}
			send [GEM 15] (
				source = @world
				destination = {
					1/2 to @a
					2/3 to @b
					remaining to @c
					$prop to @d
				}
			)`,
			Expected: CaseResult{
				Error: "100%",
			},
		})
	})

	t.Run("variable - remaining", func(t *testing.T) {
		test(t, TestCase{
			Case: `vars {
				portion $prop
			}
			send [GEM 15] (
				source = @world
				destination = {
					2/3 to @a
					$prop to @b
				}
			)`,
			Expected: CaseResult{
				Error: "100%",
			},
		})
	})
}

func TestAllocationWrongDestination(t *testing.T) {
	test(t, TestCase{
		Case: `send [GEM 15] (
			source = @world
			destination = [GEM 10]
		)`,
		Expected: CaseResult{
			Error: "account",
		},
	})
	test(t, TestCase{
		Case: `send [GEM 15] (
			source = @world
			destination = {
				2/3 to @a
				1/3 to [GEM 10]
			}
		)`,
		Expected: CaseResult{
			Error: "account",
		},
	})
}

func TestAllocationInvalidPortion(t *testing.T) {
	test(t, TestCase{
		Case: `vars {
			account $p
		}
		send [GEM 15] (
			source = @world
			destination = {
				10% to @a
				$p to @b
			}
		)`,
		Expected: CaseResult{
			Error: "type",
		},
	})
}

func TestOverdraftOnWorld(t *testing.T) {
	test(t, TestCase{
		Case: `send [GEM 15] (
			source = @world allowing overdraft up to [GEM 10]
			destination = @foo
		)`,
		Expected: CaseResult{
			Error: "overdraft",
		},
	})
}

func TestOverdraftWrongType(t *testing.T) {
	test(t, TestCase{
		Case: `send [GEM 15] (
			source = @foo allowing overdraft up to @baz
			destination = @bar
		)`,
		Expected: CaseResult{
			Error: "type",
		},
	})
}

func TestDestinationInOrderWrongType(t *testing.T) {
	test(t, TestCase{
		Case: `send [GEM 15] (
			source = @foo
			destination = {
				max @bar to @baz
				remaining to @qux
			}
		)`,
		Expected: CaseResult{
			Error: "type",
		},
	})
}

func TestSetAccountMeta(t *testing.T) {
	t.Run("all types", func(t *testing.T) {
		test(t, TestCase{
			Case: `
			set_account_meta(@alice, "aaa", @platform)
			set_account_meta(@alice, "bbb", GEM)
			set_account_meta(@alice, "ccc", 42)
			set_account_meta(@alice, "ddd", "test")
			set_account_meta(@alice, "eee", [COIN 30])
			set_account_meta(@alice, "fff", 15%)
			`,
			Expected: CaseResult{
				Instructions: []byte{
					program2.OP_APUSH, 00, 00,
					program2.OP_APUSH, 01, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_ACCOUNT_META,
					program2.OP_APUSH, 03, 00,
					program2.OP_APUSH, 04, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_ACCOUNT_META,
					program2.OP_APUSH, 05, 00,
					program2.OP_APUSH, 06, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_ACCOUNT_META,
					program2.OP_APUSH, 7, 00,
					program2.OP_APUSH, 8, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_ACCOUNT_META,
					program2.OP_APUSH, 10, 00,
					program2.OP_APUSH, 11, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_ACCOUNT_META,
					program2.OP_APUSH, 12, 00,
					program2.OP_APUSH, 13, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_ACCOUNT_META,
				},
				Resources: []program2.Resource{
					program2.Constant{Inner: machine.AccountAddress("platform")},
					program2.Constant{Inner: machine.String("aaa")},
					program2.Constant{Inner: machine.AccountAddress("alice")},
					program2.Constant{Inner: machine.Asset("GEM")},
					program2.Constant{Inner: machine.String("bbb")},
					program2.Constant{Inner: machine.NewNumber(42)},
					program2.Constant{Inner: machine.String("ccc")},
					program2.Constant{Inner: machine.String("test")},
					program2.Constant{Inner: machine.String("ddd")},
					program2.Constant{Inner: machine.Asset("COIN")},
					program2.Monetary{
						Asset:  9,
						Amount: machine.NewMonetaryInt(30),
					},
					program2.Constant{Inner: machine.String("eee")},
					program2.Constant{Inner: machine.Portion{
						Remaining: false,
						Specific:  big.NewRat(15, 100),
					}},
					program2.Constant{Inner: machine.String("fff")},
				},
			},
		})
	})

	t.Run("with vars", func(t *testing.T) {
		test(t, TestCase{
			Case: `vars {
				account $acc
			}
			send [EUR/2 100] (
				source = @world
				destination = $acc
			)
			set_account_meta($acc, "fees", 1%)`,
			Expected: CaseResult{
				Instructions: []byte{
					program2.OP_APUSH, 03, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_ASSET,
					program2.OP_APUSH, 04, 00,
					program2.OP_MONETARY_NEW,
					program2.OP_TAKE_ALL,
					program2.OP_APUSH, 02, 00,
					program2.OP_TAKE_MAX,
					program2.OP_APUSH, 05, 00,
					program2.OP_BUMP,
					program2.OP_REPAY,
					program2.OP_APUSH, 03, 00,
					program2.OP_APUSH, 06, 00,
					program2.OP_BUMP,
					program2.OP_TAKE_ALWAYS,
					program2.OP_APUSH, 06, 00,
					program2.OP_FUNDING_ASSEMBLE,
					program2.OP_FUNDING_SUM,
					program2.OP_TAKE,
					program2.OP_APUSH, 00, 00,
					program2.OP_SEND,
					program2.OP_REPAY,
					program2.OP_APUSH, 07, 00,
					program2.OP_APUSH, 8, 00,
					program2.OP_APUSH, 00, 00,
					program2.OP_ACCOUNT_META,
				},
				Resources: []program2.Resource{
					program2.Variable{Typ: machine.TypeAccount, Name: "acc"},
					program2.Constant{Inner: machine.Asset("EUR/2")},
					program2.Monetary{
						Asset:  1,
						Amount: machine.NewMonetaryInt(100),
					},
					program2.Constant{Inner: machine.AccountAddress("world")},
					program2.Constant{Inner: machine.NewMonetaryInt(0)},
					program2.Constant{Inner: machine.NewMonetaryInt(1)},
					program2.Constant{Inner: machine.NewMonetaryInt(2)},
					program2.Constant{Inner: machine.Portion{
						Remaining: false,
						Specific:  big.NewRat(1, 100),
					}},
					program2.Constant{Inner: machine.String("fees")},
				},
			},
		})
	})

	t.Run("errors", func(t *testing.T) {
		test(t, TestCase{
			Case: `set_account_meta(@alice, "fees")`,
			Expected: CaseResult{
				Error: "mismatched input",
			},
		})
		test(t, TestCase{
			Case: `set_account_meta("test")`,
			Expected: CaseResult{
				Error: "mismatched input",
			},
		})
		test(t, TestCase{
			Case: `set_account_meta(@alice, "t1", "t2", "t3")`,
			Expected: CaseResult{
				Error: "mismatched input",
			},
		})
		test(t, TestCase{
			Case: `vars {
				portion $p
			}
			set_account_meta($p, "fees", 1%)`,
			Expected: CaseResult{
				Error: "should be of type account",
			},
		})
	})
}

func TestVariableBalance(t *testing.T) {
	t.Run("simplest", func(t *testing.T) {
		test(t, TestCase{
			Case: `vars {
				monetary $bal = balance(@alice, COIN)
			}
			send $bal (
				source = @alice
				destination = @bob
			)`,
			Expected: CaseResult{
				Instructions: []byte{
					program2.OP_APUSH, 00, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_ASSET,
					program2.OP_APUSH, 03, 00,
					program2.OP_MONETARY_NEW,
					program2.OP_TAKE_ALL,
					program2.OP_APUSH, 02, 00,
					program2.OP_TAKE,
					program2.OP_APUSH, 04, 00,
					program2.OP_BUMP,
					program2.OP_REPAY,
					program2.OP_FUNDING_SUM,
					program2.OP_TAKE,
					program2.OP_APUSH, 05, 00,
					program2.OP_SEND,
					program2.OP_REPAY,
				},
				Resources: []program2.Resource{
					program2.Constant{Inner: machine.AccountAddress("alice")},
					program2.Constant{Inner: machine.Asset("COIN")},
					program2.VariableAccountBalance{Account: 0, Asset: 1},
					program2.Constant{Inner: machine.NewMonetaryInt(0)},
					program2.Constant{Inner: machine.NewMonetaryInt(1)},
					program2.Constant{Inner: machine.AccountAddress("bob")},
				},
			},
		})
	})

	t.Run("with account variable", func(t *testing.T) {
		test(t, TestCase{
			Case: `vars {
				account $acc
				monetary $bal = balance($acc, COIN)
			}
			send $bal (
				source = @world
				destination = @alice
			)`,
			Expected: CaseResult{
				Instructions: []byte{
					program2.OP_APUSH, 03, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_ASSET,
					program2.OP_APUSH, 04, 00,
					program2.OP_MONETARY_NEW,
					program2.OP_TAKE_ALL,
					program2.OP_APUSH, 02, 00,
					program2.OP_TAKE_MAX,
					program2.OP_APUSH, 05, 00,
					program2.OP_BUMP,
					program2.OP_REPAY,
					program2.OP_APUSH, 03, 00,
					program2.OP_APUSH, 06, 00,
					program2.OP_BUMP,
					program2.OP_TAKE_ALWAYS,
					program2.OP_APUSH, 06, 00,
					program2.OP_FUNDING_ASSEMBLE,
					program2.OP_FUNDING_SUM,
					program2.OP_TAKE,
					program2.OP_APUSH, 07, 00,
					program2.OP_SEND,
					program2.OP_REPAY,
				},
				Resources: []program2.Resource{
					program2.Variable{Typ: machine.TypeAccount, Name: "acc"},
					program2.Constant{Inner: machine.Asset("COIN")},
					program2.VariableAccountBalance{Account: 0, Asset: 1},
					program2.Constant{Inner: machine.AccountAddress("world")},
					program2.Constant{Inner: machine.NewMonetaryInt(0)},
					program2.Constant{Inner: machine.NewMonetaryInt(1)},
					program2.Constant{Inner: machine.NewMonetaryInt(2)},
					program2.Constant{Inner: machine.AccountAddress("alice")},
				},
			},
		})
	})

	t.Run("error variable type", func(t *testing.T) {
		test(t, TestCase{
			Case: `vars {
				account $bal = balance(@alice, COIN)
			}
			send $bal (
				source = @alice
				destination = @bob
			)`,
			Expected: CaseResult{
				Error: "variable $bal: type should be 'monetary' to pull account balance",
			},
		})
	})

	t.Run("error no asset", func(t *testing.T) {
		test(t, TestCase{
			Case: `vars {
				monetary $bal = balance(@alice)
			}
			send $bal (
				source = @alice
				destination = @bob
			)`,
			Expected: CaseResult{
				Error: "mismatched input",
			},
		})
	})

	t.Run("error too many arguments", func(t *testing.T) {
		test(t, TestCase{
			Case: `vars {
				monetary $bal = balance(@alice, USD, COIN)
			}
			send $bal (
				source = @alice
				destination = @bob
			)`,
			Expected: CaseResult{
				Error: "mismatched input ',' expecting ')'",
			},
		})
	})

	t.Run("error wrong type for account", func(t *testing.T) {
		test(t, TestCase{
			Case: `vars {
				monetary $bal = balance(USD, COIN)
			}
			send $bal (
				source = @alice
				destination = @bob
			)`,
			Expected: CaseResult{
				Error: "variable $bal: the first argument to pull account balance should be of type 'account'",
			},
		})
	})

	t.Run("error wrong type for asset", func(t *testing.T) {
		test(t, TestCase{
			Case: `vars {
				monetary $bal = balance(@alice, @bob)
			}
			send $bal (
				source = @alice
				destination = @bob
			)`,
			Expected: CaseResult{
				Error: "variable $bal: the second argument to pull account balance should be of type 'asset'",
			},
		})
	})

	t.Run("error not in variables", func(t *testing.T) {
		test(t, TestCase{
			Case: `send balance(@alice, COIN) (
				source = @alice
				destination = @bob
			)`,
			Expected: CaseResult{
				Error: "mismatched input 'balance'",
			},
		})
	})
}

func TestVariableAsset(t *testing.T) {
	script := `vars {
			asset $ass
			monetary $bal = balance(@alice, $ass)
		}

		send [$ass *] (
			source = @alice
			destination = @bob
		)

		send [$ass 1] (
			source = @bob
			destination = @alice
		)

		send $bal (
			source = @alice
			destination = @bob
		)`

	test(t, TestCase{
		Case: script,
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 01, 00,
				program2.OP_APUSH, 00, 00,
				program2.OP_APUSH, 03, 00,
				program2.OP_MONETARY_NEW,
				program2.OP_TAKE_ALL,
				program2.OP_FUNDING_SUM,
				program2.OP_TAKE,
				program2.OP_APUSH, 04, 00,
				program2.OP_SEND,
				program2.OP_REPAY,
				program2.OP_APUSH, 04, 00,
				program2.OP_APUSH, 05, 00,
				program2.OP_ASSET,
				program2.OP_APUSH, 03, 00,
				program2.OP_MONETARY_NEW,
				program2.OP_TAKE_ALL,
				program2.OP_APUSH, 05, 00,
				program2.OP_TAKE,
				program2.OP_APUSH, 06, 00,
				program2.OP_BUMP,
				program2.OP_REPAY,
				program2.OP_FUNDING_SUM,
				program2.OP_TAKE,
				program2.OP_APUSH, 01, 00,
				program2.OP_SEND,
				program2.OP_REPAY,
				program2.OP_APUSH, 01, 00,
				program2.OP_APUSH, 02, 00,
				program2.OP_ASSET,
				program2.OP_APUSH, 03, 00,
				program2.OP_MONETARY_NEW,
				program2.OP_TAKE_ALL,
				program2.OP_APUSH, 02, 00,
				program2.OP_TAKE,
				program2.OP_APUSH, 06, 00,
				program2.OP_BUMP,
				program2.OP_REPAY,
				program2.OP_FUNDING_SUM,
				program2.OP_TAKE,
				program2.OP_APUSH, 04, 00,
				program2.OP_SEND,
				program2.OP_REPAY,
			},
			Resources: []program2.Resource{
				program2.Variable{Typ: machine.TypeAsset, Name: "ass"},
				program2.Constant{Inner: machine.AccountAddress("alice")},
				program2.VariableAccountBalance{
					Name:    "bal",
					Account: 1,
					Asset:   0,
				},
				program2.Constant{Inner: machine.NewMonetaryInt(0)},
				program2.Constant{Inner: machine.AccountAddress("bob")},
				program2.Monetary{
					Asset:  0,
					Amount: machine.NewMonetaryInt(1),
				},
				program2.Constant{Inner: machine.NewMonetaryInt(1)},
			},
		},
	})
}

func TestPrint(t *testing.T) {
	script := `print 1 + 2 + 3`
	test(t, TestCase{
		Case: script,
		Expected: CaseResult{
			Instructions: []byte{
				program2.OP_APUSH, 00, 00,
				program2.OP_APUSH, 01, 00,
				program2.OP_IADD,
				program2.OP_APUSH, 02, 00,
				program2.OP_IADD,
				program2.OP_PRINT,
			},
			Resources: []program2.Resource{
				program2.Constant{Inner: machine.NewMonetaryInt(1)},
				program2.Constant{Inner: machine.NewMonetaryInt(2)},
				program2.Constant{Inner: machine.NewMonetaryInt(3)},
			},
		},
	})
}

func TestSendWithArithmetic(t *testing.T) {
	t.Run("nominal", func(t *testing.T) {
		script := `
			vars {
				asset $ass
				monetary $mon
			}
			send [EUR 1] + $mon + [$ass 3] - [EUR 4] (
				source = @a
				destination = @b
			)`

		test(t, TestCase{
			Case: script,
			Expected: CaseResult{
				Instructions: []byte{
					program2.OP_APUSH, 06, 00,
					program2.OP_APUSH, 03, 00,
					program2.OP_ASSET,
					program2.OP_APUSH, 07, 00,
					program2.OP_MONETARY_NEW,
					program2.OP_TAKE_ALL,
					program2.OP_APUSH, 03, 00,
					program2.OP_APUSH, 01, 00,
					program2.OP_MONETARY_ADD,
					program2.OP_APUSH, 04, 00,
					program2.OP_MONETARY_ADD,
					program2.OP_APUSH, 05, 00,
					program2.OP_MONETARY_SUB,
					program2.OP_TAKE,
					program2.OP_APUSH, 8, 00,
					program2.OP_BUMP,
					program2.OP_REPAY,
					program2.OP_FUNDING_SUM,
					program2.OP_TAKE,
					program2.OP_APUSH, 9, 00,
					program2.OP_SEND,
					program2.OP_REPAY,
				},
				Resources: []program2.Resource{
					program2.Variable{
						Typ:  machine.TypeAsset,
						Name: "ass",
					},
					program2.Variable{
						Typ:  machine.TypeMonetary,
						Name: "mon",
					},
					program2.Constant{Inner: machine.Asset("EUR")},
					program2.Monetary{
						Asset:  2,
						Amount: machine.NewMonetaryInt(1),
					},
					program2.Monetary{
						Asset:  0,
						Amount: machine.NewMonetaryInt(3),
					},
					program2.Monetary{
						Asset:  2,
						Amount: machine.NewMonetaryInt(4),
					},
					program2.Constant{Inner: machine.AccountAddress("a")},
					program2.Constant{Inner: machine.NewMonetaryInt(0)},
					program2.Constant{Inner: machine.NewMonetaryInt(1)},
					program2.Constant{Inner: machine.AccountAddress("b")},
				},
			},
		})
	})

	t.Run("error incompatible types", func(t *testing.T) {
		script := `send [EUR 1] + 2 (
				source = @world
				destination = @bob
			)`

		test(t, TestCase{
			Case: script,
			Expected: CaseResult{
				Instructions: []byte{},
				Resources:    []program2.Resource{},
				Error:        "tried to do an arithmetic operation with incompatible left and right-hand side operand types: monetary and number",
			},
		})
	})

	t.Run("error incompatible types var", func(t *testing.T) {
		script := `
			vars {
				number $nb
			}
			send [EUR 1] - $nb (
				source = @world
				destination = @bob
			)`

		test(t, TestCase{
			Case: script,
			Expected: CaseResult{
				Instructions: []byte{},
				Resources:    []program2.Resource{},
				Error:        "tried to do an arithmetic operation with incompatible left and right-hand side operand types: monetary and number",
			},
		})
	})
}

func TestSaveFromAccount(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		test(t, TestCase{
			Case: `
 			save [EUR 10] from @alice

 			send [EUR 20] (
 				source = @alice
 				destination = @bob
 			)`,
			Expected: CaseResult{
				Instructions: []byte{
					program2.OP_APUSH, 01, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_SAVE,
					program2.OP_APUSH, 02, 00,
					program2.OP_APUSH, 03, 00,
					program2.OP_ASSET,
					program2.OP_APUSH, 04, 00,
					program2.OP_MONETARY_NEW,
					program2.OP_TAKE_ALL,
					program2.OP_APUSH, 03, 00,
					program2.OP_TAKE,
					program2.OP_APUSH, 05, 00,
					program2.OP_BUMP,
					program2.OP_REPAY,
					program2.OP_FUNDING_SUM,
					program2.OP_TAKE,
					program2.OP_APUSH, 06, 00,
					program2.OP_SEND,
					program2.OP_REPAY,
				},
				Resources: []program2.Resource{
					program2.Constant{Inner: machine.Asset("EUR")},
					program2.Monetary{
						Asset:  0,
						Amount: machine.NewMonetaryInt(10),
					},
					program2.Constant{Inner: machine.AccountAddress("alice")},
					program2.Monetary{
						Asset:  0,
						Amount: machine.NewMonetaryInt(20),
					},
					program2.Constant{Inner: machine.NewMonetaryInt(0)},
					program2.Constant{Inner: machine.NewMonetaryInt(1)},
					program2.Constant{Inner: machine.AccountAddress("bob")},
				},
			},
		})
	})

	t.Run("save all", func(t *testing.T) {
		test(t, TestCase{
			Case: `
 			save [EUR *] from @alice

 			send [EUR 20] (
 				source = @alice
 				destination = @bob
 			)`,
			Expected: CaseResult{
				Instructions: []byte{
					program2.OP_APUSH, 00, 00,
					program2.OP_APUSH, 01, 00,
					program2.OP_SAVE,
					program2.OP_APUSH, 01, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_ASSET,
					program2.OP_APUSH, 03, 00,
					program2.OP_MONETARY_NEW,
					program2.OP_TAKE_ALL,
					program2.OP_APUSH, 02, 00,
					program2.OP_TAKE,
					program2.OP_APUSH, 04, 00,
					program2.OP_BUMP,
					program2.OP_REPAY,
					program2.OP_FUNDING_SUM,
					program2.OP_TAKE,
					program2.OP_APUSH, 05, 00,
					program2.OP_SEND,
					program2.OP_REPAY,
				},
				Resources: []program2.Resource{
					program2.Constant{Inner: machine.Asset("EUR")},
					program2.Constant{Inner: machine.AccountAddress("alice")},
					program2.Monetary{
						Asset:  0,
						Amount: machine.NewMonetaryInt(20),
					},
					program2.Constant{Inner: machine.NewMonetaryInt(0)},
					program2.Constant{Inner: machine.NewMonetaryInt(1)},
					program2.Constant{Inner: machine.AccountAddress("bob")},
				},
			},
		})
	})

	t.Run("with asset var", func(t *testing.T) {
		test(t, TestCase{
			Case: `
			vars {
				asset $ass
			}

 			save [$ass 10] from @alice

 			send [$ass 20] (
 				source = @alice
 				destination = @bob
 			)`,
			Expected: CaseResult{
				Instructions: []byte{
					program2.OP_APUSH, 01, 00,
					program2.OP_APUSH, 02, 00,
					program2.OP_SAVE,
					program2.OP_APUSH, 02, 00,
					program2.OP_APUSH, 03, 00,
					program2.OP_ASSET,
					program2.OP_APUSH, 04, 00,
					program2.OP_MONETARY_NEW,
					program2.OP_TAKE_ALL,
					program2.OP_APUSH, 03, 00,
					program2.OP_TAKE,
					program2.OP_APUSH, 05, 00,
					program2.OP_BUMP,
					program2.OP_REPAY,
					program2.OP_FUNDING_SUM,
					program2.OP_TAKE,
					program2.OP_APUSH, 06, 00,
					program2.OP_SEND,
					program2.OP_REPAY,
				},
				Resources: []program2.Resource{
					program2.Variable{Typ: machine.TypeAsset, Name: "ass"},
					program2.Monetary{
						Asset:  0,
						Amount: machine.NewMonetaryInt(10),
					},
					program2.Constant{Inner: machine.AccountAddress("alice")},
					program2.Monetary{
						Asset:  0,
						Amount: machine.NewMonetaryInt(20),
					},
					program2.Constant{Inner: machine.NewMonetaryInt(0)},
					program2.Constant{Inner: machine.NewMonetaryInt(1)},
					program2.Constant{Inner: machine.AccountAddress("bob")},
				},
			},
		})
	})

	t.Run("with monetary var", func(t *testing.T) {
		test(t, TestCase{
			Case: `
			vars {
				monetary $mon
			}

 			save $mon from @alice

 			send [EUR 20] (
 				source = @alice
 				destination = @bob
 			)`,
			Expected: CaseResult{
				Instructions: []byte{
					program2.OP_APUSH, 00, 00,
					program2.OP_APUSH, 01, 00,
					program2.OP_SAVE,
					program2.OP_APUSH, 01, 00,
					program2.OP_APUSH, 03, 00,
					program2.OP_ASSET,
					program2.OP_APUSH, 04, 00,
					program2.OP_MONETARY_NEW,
					program2.OP_TAKE_ALL,
					program2.OP_APUSH, 03, 00,
					program2.OP_TAKE,
					program2.OP_APUSH, 05, 00,
					program2.OP_BUMP,
					program2.OP_REPAY,
					program2.OP_FUNDING_SUM,
					program2.OP_TAKE,
					program2.OP_APUSH, 06, 00,
					program2.OP_SEND,
					program2.OP_REPAY,
				},
				Resources: []program2.Resource{
					program2.Variable{Typ: machine.TypeMonetary, Name: "mon"},
					program2.Constant{Inner: machine.AccountAddress("alice")},
					program2.Constant{Inner: machine.Asset("EUR")},
					program2.Monetary{
						Asset:  2,
						Amount: machine.NewMonetaryInt(20),
					},
					program2.Constant{Inner: machine.NewMonetaryInt(0)},
					program2.Constant{Inner: machine.NewMonetaryInt(1)},
					program2.Constant{Inner: machine.AccountAddress("bob")},
				},
			},
		})
	})

	t.Run("error wrong type monetary", func(t *testing.T) {
		test(t, TestCase{
			Case: `
				save 30 from @alice
			`,
			Expected: CaseResult{
				Instructions: []byte{},
				Resources:    []program2.Resource{},
				Error:        "save monetary from account: the first expression should be of type 'monetary' instead of 'number'",
			},
		})
	})

	t.Run("error wrong type account", func(t *testing.T) {
		test(t, TestCase{
			Case: `
				save [EUR 30] from ALICE
			`,
			Expected: CaseResult{
				Instructions: []byte{},
				Resources:    []program2.Resource{},
				Error:        "save monetary from account: the second expression should be of type 'account' instead of 'asset'",
			},
		})
	})
}
