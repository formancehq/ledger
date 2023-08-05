package compiler

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/numary/ledger/pkg/machine/internal"
	. "github.com/numary/ledger/pkg/machine/vm/program"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestCase struct {
	Case     string
	Expected CaseResult
}

type CaseResult struct {
	Program Program
	Error   string
}

func test(t *testing.T, c TestCase) {
	p, err := Compile(c.Case)
	if c.Expected.Error != "" {
		require.Error(t, err)
		require.NotEmpty(t, err.Error())
		if !assert.ErrorContains(t, err, c.Expected.Error) {
			fmt.Println(err)
			t.FailNow()
		}
		return
	}
	require.NoError(t, err)
	require.NotNil(t, p)

	if !assert.Equal(t, c.Expected.Program, *p) {
		t.FailNow()
	}
}

func TestSimplePrint(t *testing.T) {
	test(t, TestCase{
		Case: "print 1",
		Expected: CaseResult{
			Program: Program{
				Instruction: []Instruction{
					InstructionPrint{
						Expr: ExprLiteral{
							Value: internal.NewNumber(1),
						},
					},
				},
			},
		},
	})
}

func TestCompositeExpr(t *testing.T) {
	test(t, TestCase{
		Case: "print 29 + 15 - 2",
		Expected: CaseResult{
			Program: Program{
				Instruction: []Instruction{
					InstructionPrint{
						Expr: ExprNumberOperation{
							Op: OP_SUB,
							Lhs: ExprNumberOperation{
								Op:  OP_ADD,
								Lhs: ExprLiteral{Value: internal.NewNumber(29)},
								Rhs: ExprLiteral{Value: internal.NewNumber(15)},
							},
							Rhs: ExprLiteral{Value: internal.NewNumber(2)},
						},
					},
				},
			},
		},
	})
}

func TestFail(t *testing.T) {
	test(t, TestCase{
		Case: "fail",
		Expected: CaseResult{
			Program: Program{
				Instruction: []Instruction{
					InstructionFail{},
				},
			},
		},
	})
}

func TestCRLF(t *testing.T) {
	test(t, TestCase{
		Case: "print @a\r\nprint @b",
		Expected: CaseResult{
			Program: Program{
				Instruction: []Instruction{
					InstructionPrint{
						Expr: ExprLiteral{Value: internal.AccountAddress("a")},
					},
					InstructionPrint{
						Expr: ExprLiteral{Value: internal.AccountAddress("b")},
					},
				},
			},
		},
	})
}

func TestConstant(t *testing.T) {
	user := internal.AccountAddress("user:U001")
	test(t, TestCase{
		Case: "print @user:U001",
		Expected: CaseResult{
			Program: Program{
				Instruction: []Instruction{
					InstructionPrint{
						Expr: ExprLiteral{Value: user},
					},
				},
			},
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
			Program: Program{
				Instruction: []Instruction{
					InstructionSetTxMeta{
						Key:   "aaa",
						Value: ExprLiteral{Value: internal.AccountAddress("platform")},
					},
					InstructionSetTxMeta{
						Key:   "bbb",
						Value: ExprLiteral{Value: internal.Asset("GEM")},
					},
					InstructionSetTxMeta{
						Key:   "ccc",
						Value: ExprLiteral{Value: internal.NewNumber(42)},
					},
					InstructionSetTxMeta{
						Key:   "ddd",
						Value: ExprLiteral{Value: internal.String("test")},
					},
					InstructionSetTxMeta{
						Key: "eee",
						Value: ExprMonetaryNew{
							Asset:  ExprLiteral{Value: internal.Asset("COIN")},
							Amount: ExprLiteral{Value: internal.NewNumber(30)},
						},
					},
					InstructionSetTxMeta{
						Key: "fff",
						Value: ExprLiteral{Value: internal.Portion{
							Remaining: false,
							Specific:  big.NewRat(15, 100),
						}},
					},
				},
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
			Program: Program{
				VarsDecl: []VarDecl{
					{
						Typ:  internal.TypePortion,
						Name: "commission",
					},
				},
				Instruction: []Instruction{
					InstructionSetTxMeta{
						Key:   "fee",
						Value: ExprVariable("commission"),
					},
				},
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
			Program: Program{
				VarsDecl: []VarDecl{
					{
						Typ:  internal.TypeAccount,
						Name: "a",
					},
				},
				Instruction: []Instruction{
					InstructionPrint{
						Expr: ExprVariable("a"),
					},
				},
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
			Error: "wrong type: expected monetary",
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
			Program: Program{
				Instruction: []Instruction{
					InstructionAllocate{
						Funding: ExprTake{
							Amount: ExprMonetaryNew{
								Asset:  ExprLiteral{Value: internal.Asset("EUR/2")},
								Amount: ExprLiteral{Value: internal.NewNumber(43)},
							},
							Source: ValueAwareSourceSource{
								Source: SourceAccount{Account: ExprLiteral{Value: internal.AccountAddress("foo")}},
							},
						},
						Destination: DestinationAllotment{
							{
								Portion: AllotmentPortion{
									Expr: ExprLiteral{Value: internal.Portion{
										Remaining: false,
										Specific:  big.NewRat(1, 8),
									}},
								},
								Kod: KeptOrDestination{
									Destination: DestinationAccount{
										Expr: ExprLiteral{Value: internal.AccountAddress("bar")},
									},
								},
							},
							{
								Portion: AllotmentPortion{
									Expr: ExprLiteral{Value: internal.Portion{
										Remaining: false,
										Specific:  big.NewRat(7, 8),
									}},
								},
								Kod: KeptOrDestination{
									Destination: DestinationAccount{
										Expr: ExprLiteral{Value: internal.AccountAddress("baz")},
									},
								},
							},
						},
					},
				},
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
			Program: Program{
				Instruction: []Instruction{
					InstructionAllocate{
						Funding: ExprTake{
							Amount: ExprMonetaryNew{
								Asset:  ExprLiteral{Value: internal.Asset("COIN")},
								Amount: ExprLiteral{Value: internal.NewNumber(50)},
							},
							Source: ValueAwareSourceSource{
								Source: SourceAccount{Account: ExprLiteral{Value: internal.AccountAddress("a")}},
							},
						},
						Destination: DestinationInOrder{
							Parts: []DestinationInOrderPart{
								{
									Max: ExprMonetaryNew{
										Asset:  ExprLiteral{Value: internal.Asset("COIN")},
										Amount: ExprLiteral{Value: internal.NewNumber(10)},
									},
									Kod: KeptOrDestination{
										Destination: DestinationAccount{
											Expr: ExprLiteral{Value: internal.AccountAddress("b")},
										},
									},
								},
							},
							Remaining: KeptOrDestination{
								Destination: DestinationAccount{
									Expr: ExprLiteral{Value: internal.AccountAddress("c")},
								},
							},
						},
					},
				},
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
			Program: Program{
				Instruction: []Instruction{
					InstructionAllocate{
						Funding: ExprTake{
							Amount: ExprMonetaryNew{
								Asset:  ExprLiteral{Value: internal.Asset("EUR/2")},
								Amount: ExprLiteral{Value: internal.NewNumber(43)},
							},
							Source: ValueAwareSourceSource{
								Source: SourceAccount{Account: ExprLiteral{Value: internal.AccountAddress("foo")}},
							},
						},
						Destination: DestinationAllotment{
							{
								Portion: AllotmentPortion{
									Expr: ExprLiteral{Value: internal.Portion{
										Remaining: false,
										Specific:  big.NewRat(125, 1000),
									}},
								},
								Kod: KeptOrDestination{
									Destination: DestinationAccount{
										Expr: ExprLiteral{Value: internal.AccountAddress("bar")},
									},
								},
							},
							{
								Portion: AllotmentPortion{
									Expr: ExprLiteral{Value: internal.Portion{
										Remaining: false,
										Specific:  big.NewRat(375, 1000),
									}},
								},
								Kod: KeptOrDestination{
									Destination: DestinationAccount{
										Expr: ExprLiteral{Value: internal.AccountAddress("baz")},
									},
								},
							},
							{
								Portion: AllotmentPortion{
									Expr: ExprLiteral{Value: internal.Portion{
										Remaining: false,
										Specific:  big.NewRat(500, 1000),
									}},
								},
								Kod: KeptOrDestination{
									Destination: DestinationAccount{
										Expr: ExprLiteral{Value: internal.AccountAddress("qux")},
									},
								},
							},
						},
					},
				},
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
	test(t, TestCase{
		Case: script,
		Expected: CaseResult{
			Program: Program{
				Instruction: []Instruction{
					InstructionAllocate{
						Funding: ExprTake{
							Amount: ExprMonetaryNew{
								Asset:  ExprLiteral{Value: internal.Asset("EUR/2")},
								Amount: ExprLiteral{Value: internal.NewNumber(99)},
							},
							Source: ValueAwareSourceSource{
								Source: SourceAccount{Account: ExprLiteral{Value: internal.AccountAddress("alice")}},
							},
						},
						Destination: DestinationAccount{
							Expr: ExprLiteral{Value: internal.AccountAddress("bob")},
						},
					},
				},
			},
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
			Program: Program{
				Instruction: []Instruction{
					InstructionAllocate{
						Funding: ExprTakeAll{
							Asset:  ExprLiteral{Value: internal.Asset("EUR/2")},
							Source: SourceAccount{Account: ExprLiteral{Value: internal.AccountAddress("alice")}},
						},
						Destination: DestinationAccount{
							Expr: ExprLiteral{Value: internal.AccountAddress("bob")},
						},
					},
				},
			},
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
			Program: Program{
				VarsDecl: []VarDecl{
					{
						Typ:  internal.TypeAccount,
						Name: "sale",
					},
					{
						Typ:  internal.TypeAccount,
						Name: "seller",
						Origin: VarOriginMeta{
							Account: ExprVariable("sale"),
							Key:     "seller",
						},
					},
					{
						Typ:  internal.TypePortion,
						Name: "commission",
						Origin: VarOriginMeta{
							Account: ExprVariable("seller"),
							Key:     "commission",
						},
					},
				},
				Instruction: []Instruction{
					InstructionAllocate{
						Funding: ExprTake{
							Amount: ExprMonetaryNew{
								Asset:  ExprLiteral{Value: internal.Asset("EUR/2")},
								Amount: ExprLiteral{Value: internal.NewNumber(53)},
							},
							Source: ValueAwareSourceSource{
								Source: SourceAccount{Account: ExprVariable("sale")},
							},
						},
						Destination: DestinationAllotment{
							{
								Portion: AllotmentPortion{
									Expr: ExprVariable("commission"),
								},
								Kod: KeptOrDestination{
									Destination: DestinationAccount{
										Expr: ExprLiteral{Value: internal.AccountAddress("platform")},
									},
								},
							},
							{
								Portion: AllotmentPortion{
									Remaining: true,
								},
								Kod: KeptOrDestination{
									Destination: DestinationAccount{
										Expr: ExprVariable("seller"),
									},
								},
							},
						},
					},
				},
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

// func TestPreventSourceAlreadyEmptied(t *testing.T) {
// 	test(t, TestCase{
// 		Case: `send [GEM 1000] (
// 			source = {
// 				{
// 					@a
// 					@b
// 				}
// 				@a
// 			}
// 			destination = @out
// 		)`,
// 		Expected: CaseResult{
// 			Error: "empt",
// 		},
// 	})
// }

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
			Error: "mismatched",
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
				Program: Program{
					Instruction: []Instruction{
						InstructionSetAccountMeta{
							Account: ExprLiteral{Value: internal.AccountAddress("alice")},
							Key:     "aaa",
							Value:   ExprLiteral{Value: internal.AccountAddress("platform")},
						},
						InstructionSetAccountMeta{
							Account: ExprLiteral{Value: internal.AccountAddress("alice")},
							Key:     "bbb",
							Value:   ExprLiteral{Value: internal.Asset("GEM")},
						},
						InstructionSetAccountMeta{
							Account: ExprLiteral{Value: internal.AccountAddress("alice")},
							Key:     "ccc",
							Value:   ExprLiteral{Value: internal.NewNumber(42)},
						},
						InstructionSetAccountMeta{
							Account: ExprLiteral{Value: internal.AccountAddress("alice")},
							Key:     "ddd",
							Value:   ExprLiteral{Value: internal.String("test")},
						},
						InstructionSetAccountMeta{
							Account: ExprLiteral{Value: internal.AccountAddress("alice")},
							Key:     "eee",
							Value: ExprMonetaryNew{
								Asset:  ExprLiteral{Value: internal.Asset("COIN")},
								Amount: ExprLiteral{Value: internal.NewNumber(30)},
							},
						},
						InstructionSetAccountMeta{
							Account: ExprLiteral{Value: internal.AccountAddress("alice")},
							Key:     "fff",
							Value: ExprLiteral{Value: internal.Portion{
								Remaining: false,
								Specific:  big.NewRat(15, 100),
							}},
						},
					},
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
				Program: Program{
					VarsDecl: []VarDecl{
						{
							Typ:  internal.TypeAccount,
							Name: "acc",
						},
					},
					Instruction: []Instruction{
						InstructionAllocate{
							Funding: ExprTake{
								Amount: ExprMonetaryNew{
									Asset:  ExprLiteral{Value: internal.Asset("EUR/2")},
									Amount: ExprLiteral{Value: internal.NewNumber(100)},
								},
								Source: ValueAwareSourceSource{
									Source: SourceAccount{Account: ExprLiteral{Value: internal.AccountAddress("world")}},
								},
							},
							Destination: DestinationAccount{
								Expr: ExprVariable("acc"),
							},
						},
						InstructionSetAccountMeta{
							Account: ExprVariable("acc"),
							Key:     "fees",
							Value: ExprLiteral{Value: internal.Portion{
								Remaining: false,
								Specific:  big.NewRat(1, 100),
							}},
						},
					},
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
				Error: "wrong type: expected account",
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
				Program: Program{
					VarsDecl: []VarDecl{
						{
							Typ:  internal.TypeMonetary,
							Name: "bal",
							Origin: VarOriginBalance{
								Account: ExprLiteral{Value: internal.AccountAddress("alice")},
								Asset:   ExprLiteral{Value: internal.Asset("COIN")},
							},
						},
					},
					Instruction: []Instruction{
						InstructionAllocate{
							Funding: ExprTake{
								Amount: ExprVariable("bal"),
								Source: ValueAwareSourceSource{
									Source: SourceAccount{Account: ExprLiteral{Value: internal.AccountAddress("alice")}},
								},
							},
							Destination: DestinationAccount{
								Expr: ExprLiteral{Value: internal.AccountAddress("bob")},
							},
						},
					},
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
				Program: Program{
					VarsDecl: []VarDecl{
						{
							Typ:  internal.TypeAccount,
							Name: "acc",
						},
						{
							Typ:  internal.TypeMonetary,
							Name: "bal",
							Origin: VarOriginBalance{
								Account: ExprVariable("acc"),
								Asset:   ExprLiteral{Value: internal.Asset("COIN")},
							},
						},
					},
					Instruction: []Instruction{
						InstructionAllocate{
							Funding: ExprTake{
								Amount: ExprVariable("bal"),
								Source: ValueAwareSourceSource{
									Source: SourceAccount{Account: ExprLiteral{Value: internal.AccountAddress("world")}},
								},
							},
							Destination: DestinationAccount{
								Expr: ExprLiteral{Value: internal.AccountAddress("alice")},
							},
						},
					},
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
				Error: "wrong type: expected account",
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
				Error: "wrong type: expected asset",
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
				Error: "no viable alternative",
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
			Program: Program{
				VarsDecl: []VarDecl{
					{
						Typ:  internal.TypeAsset,
						Name: "ass",
					},
					{
						Typ:  internal.TypeMonetary,
						Name: "bal",
						Origin: VarOriginBalance{
							Account: ExprLiteral{Value: internal.AccountAddress("alice")},
							Asset:   ExprVariable("ass"),
						},
					},
				},
				Instruction: []Instruction{
					InstructionAllocate{
						Funding: ExprTakeAll{
							Asset:  ExprVariable("ass"),
							Source: SourceAccount{Account: ExprLiteral{Value: internal.AccountAddress("alice")}},
						},
						Destination: DestinationAccount{
							Expr: ExprLiteral{Value: internal.AccountAddress("bob")},
						},
					},
					InstructionAllocate{
						Funding: ExprTake{
							Amount: ExprMonetaryNew{Asset: ExprVariable("ass"), Amount: ExprLiteral{Value: internal.NewNumber(1)}},
							Source: ValueAwareSourceSource{
								Source: SourceAccount{Account: ExprLiteral{Value: internal.AccountAddress("bob")}},
							},
						},
						Destination: DestinationAccount{
							Expr: ExprLiteral{Value: internal.AccountAddress("alice")},
						},
					},
					InstructionAllocate{
						Funding: ExprTake{
							Amount: ExprVariable("bal"),
							Source: ValueAwareSourceSource{
								Source: SourceAccount{Account: ExprLiteral{Value: internal.AccountAddress("alice")}},
							},
						},
						Destination: DestinationAccount{
							Expr: ExprLiteral{Value: internal.AccountAddress("bob")},
						},
					},
				},
			},
		},
	})
}

func TestPrint(t *testing.T) {
	script := `print 1 + 2 + 3`
	test(t, TestCase{
		Case: script,
		Expected: CaseResult{
			Program: Program{
				Instruction: []Instruction{
					InstructionPrint{
						Expr: ExprNumberOperation{
							Op: OP_ADD,
							Lhs: ExprNumberOperation{
								Op:  OP_ADD,
								Lhs: ExprLiteral{Value: internal.NewNumber(1)},
								Rhs: ExprLiteral{Value: internal.NewNumber(2)},
							},
							Rhs: ExprLiteral{Value: internal.NewNumber(3)},
						},
					},
				},
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
				Program: Program{
					VarsDecl: []VarDecl{
						{
							Typ:  internal.TypeAsset,
							Name: "ass",
						},
						{
							Typ:  internal.TypeMonetary,
							Name: "mon",
						},
					},
					Instruction: []Instruction{
						InstructionAllocate{
							Funding: ExprTake{
								Amount: ExprMonetaryOperation{
									Op: OP_SUB,
									Lhs: ExprMonetaryOperation{
										Op: OP_ADD,
										Lhs: ExprMonetaryOperation{
											Op: OP_ADD,
											Lhs: ExprMonetaryNew{
												Asset:  ExprLiteral{Value: internal.Asset("EUR")},
												Amount: ExprLiteral{Value: internal.NewMonetaryInt(1)},
											},
											Rhs: ExprVariable("mon"),
										},
										Rhs: ExprMonetaryNew{
											Asset:  ExprVariable("ass"),
											Amount: ExprLiteral{Value: internal.NewMonetaryInt(3)},
										},
									},
									Rhs: ExprMonetaryNew{
										Asset:  ExprLiteral{Value: internal.Asset("EUR")},
										Amount: ExprLiteral{Value: internal.NewMonetaryInt(4)},
									},
								},
								Source: ValueAwareSourceSource{
									Source: SourceAccount{
										Account: ExprLiteral{Value: internal.AccountAddress("a")},
									}},
							},
							Destination: DestinationAccount{
								Expr: ExprLiteral{Value: internal.AccountAddress("b")},
							},
						},
					},
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
				Error: "wrong type",
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
				Error: "wrong type",
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
				Program: Program{
					Instruction: []Instruction{
						InstructionSave{
							Amount: ExprMonetaryNew{
								Asset:  ExprLiteral{Value: internal.Asset("EUR")},
								Amount: ExprLiteral{Value: internal.NewNumber(10)},
							},
							Account: ExprLiteral{Value: internal.AccountAddress("alice")},
						},
						InstructionAllocate{
							Funding: ExprTake{
								Amount: ExprMonetaryNew{
									Asset:  ExprLiteral{Value: internal.Asset("EUR")},
									Amount: ExprLiteral{Value: internal.NewNumber(20)},
								},
								Source: ValueAwareSourceSource{
									Source: SourceAccount{
										Account: ExprLiteral{Value: internal.AccountAddress("alice")},
									},
								},
							},
							Destination: DestinationAccount{
								Expr: ExprLiteral{Value: internal.AccountAddress("bob")},
							},
						},
					},
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
				Program: Program{
					Instruction: []Instruction{
						InstructionSaveAll{
							Asset:   ExprLiteral{Value: internal.Asset("EUR")},
							Account: ExprLiteral{Value: internal.AccountAddress("alice")},
						},
						InstructionAllocate{
							Funding: ExprTake{
								Amount: ExprMonetaryNew{
									Asset:  ExprLiteral{Value: internal.Asset("EUR")},
									Amount: ExprLiteral{Value: internal.NewNumber(20)},
								},
								Source: ValueAwareSourceSource{
									Source: SourceAccount{
										Account: ExprLiteral{Value: internal.AccountAddress("alice")},
									},
								},
							},
							Destination: DestinationAccount{
								Expr: ExprLiteral{Value: internal.AccountAddress("bob")},
							},
						},
					},
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
				Program: Program{
					VarsDecl: []VarDecl{
						{
							Typ:  internal.TypeAsset,
							Name: "ass",
						},
					},
					Instruction: []Instruction{
						InstructionSave{
							Amount: ExprMonetaryNew{
								Asset:  ExprVariable("ass"),
								Amount: ExprLiteral{Value: internal.NewNumber(10)},
							},
							Account: ExprLiteral{Value: internal.AccountAddress("alice")},
						},
						InstructionAllocate{
							Funding: ExprTake{
								Amount: ExprMonetaryNew{
									Asset:  ExprVariable("ass"),
									Amount: ExprLiteral{Value: internal.NewNumber(20)},
								},
								Source: ValueAwareSourceSource{
									Source: SourceAccount{
										Account: ExprLiteral{Value: internal.AccountAddress("alice")},
									},
								},
							},
							Destination: DestinationAccount{
								Expr: ExprLiteral{Value: internal.AccountAddress("bob")},
							},
						},
					},
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
				Program: Program{
					VarsDecl: []VarDecl{
						{
							Typ:  internal.TypeMonetary,
							Name: "mon",
						},
					},
					Instruction: []Instruction{
						InstructionSave{
							Amount:  ExprVariable("mon"),
							Account: ExprLiteral{Value: internal.AccountAddress("alice")},
						},
						InstructionAllocate{
							Funding: ExprTake{
								Amount: ExprMonetaryNew{
									Asset:  ExprLiteral{Value: internal.Asset("EUR")},
									Amount: ExprLiteral{Value: internal.NewNumber(20)},
								},
								Source: ValueAwareSourceSource{
									Source: SourceAccount{
										Account: ExprLiteral{Value: internal.AccountAddress("alice")},
									},
								},
							},
							Destination: DestinationAccount{
								Expr: ExprLiteral{Value: internal.AccountAddress("bob")},
							},
						},
					},
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
				Error: "wrong type",
			},
		})
	})

	t.Run("error wrong type account", func(t *testing.T) {
		test(t, TestCase{
			Case: `
				save [EUR 30] from ALICE
			`,
			Expected: CaseResult{
				Error: "wrong type",
			},
		})
	})
}
