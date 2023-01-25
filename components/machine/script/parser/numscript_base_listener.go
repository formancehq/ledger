// Code generated from NumScript.g4 by ANTLR 4.10.1. DO NOT EDIT.

package parser // NumScript

import "github.com/antlr/antlr4/runtime/Go/antlr"

// BaseNumScriptListener is a complete listener for a parse tree produced by NumScriptParser.
type BaseNumScriptListener struct{}

var _ NumScriptListener = &BaseNumScriptListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseNumScriptListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseNumScriptListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseNumScriptListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseNumScriptListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterMonetary is called when production monetary is entered.
func (s *BaseNumScriptListener) EnterMonetary(ctx *MonetaryContext) {}

// ExitMonetary is called when production monetary is exited.
func (s *BaseNumScriptListener) ExitMonetary(ctx *MonetaryContext) {}

// EnterMonetaryAll is called when production monetaryAll is entered.
func (s *BaseNumScriptListener) EnterMonetaryAll(ctx *MonetaryAllContext) {}

// ExitMonetaryAll is called when production monetaryAll is exited.
func (s *BaseNumScriptListener) ExitMonetaryAll(ctx *MonetaryAllContext) {}

// EnterLitAccount is called when production LitAccount is entered.
func (s *BaseNumScriptListener) EnterLitAccount(ctx *LitAccountContext) {}

// ExitLitAccount is called when production LitAccount is exited.
func (s *BaseNumScriptListener) ExitLitAccount(ctx *LitAccountContext) {}

// EnterLitAsset is called when production LitAsset is entered.
func (s *BaseNumScriptListener) EnterLitAsset(ctx *LitAssetContext) {}

// ExitLitAsset is called when production LitAsset is exited.
func (s *BaseNumScriptListener) ExitLitAsset(ctx *LitAssetContext) {}

// EnterLitNumber is called when production LitNumber is entered.
func (s *BaseNumScriptListener) EnterLitNumber(ctx *LitNumberContext) {}

// ExitLitNumber is called when production LitNumber is exited.
func (s *BaseNumScriptListener) ExitLitNumber(ctx *LitNumberContext) {}

// EnterLitString is called when production LitString is entered.
func (s *BaseNumScriptListener) EnterLitString(ctx *LitStringContext) {}

// ExitLitString is called when production LitString is exited.
func (s *BaseNumScriptListener) ExitLitString(ctx *LitStringContext) {}

// EnterLitPortion is called when production LitPortion is entered.
func (s *BaseNumScriptListener) EnterLitPortion(ctx *LitPortionContext) {}

// ExitLitPortion is called when production LitPortion is exited.
func (s *BaseNumScriptListener) ExitLitPortion(ctx *LitPortionContext) {}

// EnterLitMonetary is called when production LitMonetary is entered.
func (s *BaseNumScriptListener) EnterLitMonetary(ctx *LitMonetaryContext) {}

// ExitLitMonetary is called when production LitMonetary is exited.
func (s *BaseNumScriptListener) ExitLitMonetary(ctx *LitMonetaryContext) {}

// EnterVariable is called when production variable is entered.
func (s *BaseNumScriptListener) EnterVariable(ctx *VariableContext) {}

// ExitVariable is called when production variable is exited.
func (s *BaseNumScriptListener) ExitVariable(ctx *VariableContext) {}

// EnterExprAddSub is called when production ExprAddSub is entered.
func (s *BaseNumScriptListener) EnterExprAddSub(ctx *ExprAddSubContext) {}

// ExitExprAddSub is called when production ExprAddSub is exited.
func (s *BaseNumScriptListener) ExitExprAddSub(ctx *ExprAddSubContext) {}

// EnterExprLiteral is called when production ExprLiteral is entered.
func (s *BaseNumScriptListener) EnterExprLiteral(ctx *ExprLiteralContext) {}

// ExitExprLiteral is called when production ExprLiteral is exited.
func (s *BaseNumScriptListener) ExitExprLiteral(ctx *ExprLiteralContext) {}

// EnterExprVariable is called when production ExprVariable is entered.
func (s *BaseNumScriptListener) EnterExprVariable(ctx *ExprVariableContext) {}

// ExitExprVariable is called when production ExprVariable is exited.
func (s *BaseNumScriptListener) ExitExprVariable(ctx *ExprVariableContext) {}

// EnterAllotmentPortionConst is called when production allotmentPortionConst is entered.
func (s *BaseNumScriptListener) EnterAllotmentPortionConst(ctx *AllotmentPortionConstContext) {}

// ExitAllotmentPortionConst is called when production allotmentPortionConst is exited.
func (s *BaseNumScriptListener) ExitAllotmentPortionConst(ctx *AllotmentPortionConstContext) {}

// EnterAllotmentPortionVar is called when production allotmentPortionVar is entered.
func (s *BaseNumScriptListener) EnterAllotmentPortionVar(ctx *AllotmentPortionVarContext) {}

// ExitAllotmentPortionVar is called when production allotmentPortionVar is exited.
func (s *BaseNumScriptListener) ExitAllotmentPortionVar(ctx *AllotmentPortionVarContext) {}

// EnterAllotmentPortionRemaining is called when production allotmentPortionRemaining is entered.
func (s *BaseNumScriptListener) EnterAllotmentPortionRemaining(ctx *AllotmentPortionRemainingContext) {
}

// ExitAllotmentPortionRemaining is called when production allotmentPortionRemaining is exited.
func (s *BaseNumScriptListener) ExitAllotmentPortionRemaining(ctx *AllotmentPortionRemainingContext) {
}

// EnterDestinationInOrder is called when production destinationInOrder is entered.
func (s *BaseNumScriptListener) EnterDestinationInOrder(ctx *DestinationInOrderContext) {}

// ExitDestinationInOrder is called when production destinationInOrder is exited.
func (s *BaseNumScriptListener) ExitDestinationInOrder(ctx *DestinationInOrderContext) {}

// EnterDestinationAllotment is called when production destinationAllotment is entered.
func (s *BaseNumScriptListener) EnterDestinationAllotment(ctx *DestinationAllotmentContext) {}

// ExitDestinationAllotment is called when production destinationAllotment is exited.
func (s *BaseNumScriptListener) ExitDestinationAllotment(ctx *DestinationAllotmentContext) {}

// EnterIsDestination is called when production isDestination is entered.
func (s *BaseNumScriptListener) EnterIsDestination(ctx *IsDestinationContext) {}

// ExitIsDestination is called when production isDestination is exited.
func (s *BaseNumScriptListener) ExitIsDestination(ctx *IsDestinationContext) {}

// EnterIsKept is called when production isKept is entered.
func (s *BaseNumScriptListener) EnterIsKept(ctx *IsKeptContext) {}

// ExitIsKept is called when production isKept is exited.
func (s *BaseNumScriptListener) ExitIsKept(ctx *IsKeptContext) {}

// EnterDestAccount is called when production DestAccount is entered.
func (s *BaseNumScriptListener) EnterDestAccount(ctx *DestAccountContext) {}

// ExitDestAccount is called when production DestAccount is exited.
func (s *BaseNumScriptListener) ExitDestAccount(ctx *DestAccountContext) {}

// EnterDestInOrder is called when production DestInOrder is entered.
func (s *BaseNumScriptListener) EnterDestInOrder(ctx *DestInOrderContext) {}

// ExitDestInOrder is called when production DestInOrder is exited.
func (s *BaseNumScriptListener) ExitDestInOrder(ctx *DestInOrderContext) {}

// EnterDestAllotment is called when production DestAllotment is entered.
func (s *BaseNumScriptListener) EnterDestAllotment(ctx *DestAllotmentContext) {}

// ExitDestAllotment is called when production DestAllotment is exited.
func (s *BaseNumScriptListener) ExitDestAllotment(ctx *DestAllotmentContext) {}

// EnterSrcAccountOverdraftSpecific is called when production SrcAccountOverdraftSpecific is entered.
func (s *BaseNumScriptListener) EnterSrcAccountOverdraftSpecific(ctx *SrcAccountOverdraftSpecificContext) {
}

// ExitSrcAccountOverdraftSpecific is called when production SrcAccountOverdraftSpecific is exited.
func (s *BaseNumScriptListener) ExitSrcAccountOverdraftSpecific(ctx *SrcAccountOverdraftSpecificContext) {
}

// EnterSrcAccountOverdraftUnbounded is called when production SrcAccountOverdraftUnbounded is entered.
func (s *BaseNumScriptListener) EnterSrcAccountOverdraftUnbounded(ctx *SrcAccountOverdraftUnboundedContext) {
}

// ExitSrcAccountOverdraftUnbounded is called when production SrcAccountOverdraftUnbounded is exited.
func (s *BaseNumScriptListener) ExitSrcAccountOverdraftUnbounded(ctx *SrcAccountOverdraftUnboundedContext) {
}

// EnterSourceAccount is called when production sourceAccount is entered.
func (s *BaseNumScriptListener) EnterSourceAccount(ctx *SourceAccountContext) {}

// ExitSourceAccount is called when production sourceAccount is exited.
func (s *BaseNumScriptListener) ExitSourceAccount(ctx *SourceAccountContext) {}

// EnterSourceInOrder is called when production sourceInOrder is entered.
func (s *BaseNumScriptListener) EnterSourceInOrder(ctx *SourceInOrderContext) {}

// ExitSourceInOrder is called when production sourceInOrder is exited.
func (s *BaseNumScriptListener) ExitSourceInOrder(ctx *SourceInOrderContext) {}

// EnterSourceMaxed is called when production sourceMaxed is entered.
func (s *BaseNumScriptListener) EnterSourceMaxed(ctx *SourceMaxedContext) {}

// ExitSourceMaxed is called when production sourceMaxed is exited.
func (s *BaseNumScriptListener) ExitSourceMaxed(ctx *SourceMaxedContext) {}

// EnterSrcAccount is called when production SrcAccount is entered.
func (s *BaseNumScriptListener) EnterSrcAccount(ctx *SrcAccountContext) {}

// ExitSrcAccount is called when production SrcAccount is exited.
func (s *BaseNumScriptListener) ExitSrcAccount(ctx *SrcAccountContext) {}

// EnterSrcMaxed is called when production SrcMaxed is entered.
func (s *BaseNumScriptListener) EnterSrcMaxed(ctx *SrcMaxedContext) {}

// ExitSrcMaxed is called when production SrcMaxed is exited.
func (s *BaseNumScriptListener) ExitSrcMaxed(ctx *SrcMaxedContext) {}

// EnterSrcInOrder is called when production SrcInOrder is entered.
func (s *BaseNumScriptListener) EnterSrcInOrder(ctx *SrcInOrderContext) {}

// ExitSrcInOrder is called when production SrcInOrder is exited.
func (s *BaseNumScriptListener) ExitSrcInOrder(ctx *SrcInOrderContext) {}

// EnterSourceAllotment is called when production sourceAllotment is entered.
func (s *BaseNumScriptListener) EnterSourceAllotment(ctx *SourceAllotmentContext) {}

// ExitSourceAllotment is called when production sourceAllotment is exited.
func (s *BaseNumScriptListener) ExitSourceAllotment(ctx *SourceAllotmentContext) {}

// EnterSrc is called when production Src is entered.
func (s *BaseNumScriptListener) EnterSrc(ctx *SrcContext) {}

// ExitSrc is called when production Src is exited.
func (s *BaseNumScriptListener) ExitSrc(ctx *SrcContext) {}

// EnterSrcAllotment is called when production SrcAllotment is entered.
func (s *BaseNumScriptListener) EnterSrcAllotment(ctx *SrcAllotmentContext) {}

// ExitSrcAllotment is called when production SrcAllotment is exited.
func (s *BaseNumScriptListener) ExitSrcAllotment(ctx *SrcAllotmentContext) {}

// EnterPrint is called when production Print is entered.
func (s *BaseNumScriptListener) EnterPrint(ctx *PrintContext) {}

// ExitPrint is called when production Print is exited.
func (s *BaseNumScriptListener) ExitPrint(ctx *PrintContext) {}

// EnterSetTxMeta is called when production SetTxMeta is entered.
func (s *BaseNumScriptListener) EnterSetTxMeta(ctx *SetTxMetaContext) {}

// ExitSetTxMeta is called when production SetTxMeta is exited.
func (s *BaseNumScriptListener) ExitSetTxMeta(ctx *SetTxMetaContext) {}

// EnterSetAccountMeta is called when production SetAccountMeta is entered.
func (s *BaseNumScriptListener) EnterSetAccountMeta(ctx *SetAccountMetaContext) {}

// ExitSetAccountMeta is called when production SetAccountMeta is exited.
func (s *BaseNumScriptListener) ExitSetAccountMeta(ctx *SetAccountMetaContext) {}

// EnterFail is called when production Fail is entered.
func (s *BaseNumScriptListener) EnterFail(ctx *FailContext) {}

// ExitFail is called when production Fail is exited.
func (s *BaseNumScriptListener) ExitFail(ctx *FailContext) {}

// EnterSend is called when production Send is entered.
func (s *BaseNumScriptListener) EnterSend(ctx *SendContext) {}

// ExitSend is called when production Send is exited.
func (s *BaseNumScriptListener) ExitSend(ctx *SendContext) {}

// EnterType_ is called when production type_ is entered.
func (s *BaseNumScriptListener) EnterType_(ctx *Type_Context) {}

// ExitType_ is called when production type_ is exited.
func (s *BaseNumScriptListener) ExitType_(ctx *Type_Context) {}

// EnterOriginAccountMeta is called when production OriginAccountMeta is entered.
func (s *BaseNumScriptListener) EnterOriginAccountMeta(ctx *OriginAccountMetaContext) {}

// ExitOriginAccountMeta is called when production OriginAccountMeta is exited.
func (s *BaseNumScriptListener) ExitOriginAccountMeta(ctx *OriginAccountMetaContext) {}

// EnterOriginAccountBalance is called when production OriginAccountBalance is entered.
func (s *BaseNumScriptListener) EnterOriginAccountBalance(ctx *OriginAccountBalanceContext) {}

// ExitOriginAccountBalance is called when production OriginAccountBalance is exited.
func (s *BaseNumScriptListener) ExitOriginAccountBalance(ctx *OriginAccountBalanceContext) {}

// EnterVarDecl is called when production varDecl is entered.
func (s *BaseNumScriptListener) EnterVarDecl(ctx *VarDeclContext) {}

// ExitVarDecl is called when production varDecl is exited.
func (s *BaseNumScriptListener) ExitVarDecl(ctx *VarDeclContext) {}

// EnterVarListDecl is called when production varListDecl is entered.
func (s *BaseNumScriptListener) EnterVarListDecl(ctx *VarListDeclContext) {}

// ExitVarListDecl is called when production varListDecl is exited.
func (s *BaseNumScriptListener) ExitVarListDecl(ctx *VarListDeclContext) {}

// EnterScript is called when production script is entered.
func (s *BaseNumScriptListener) EnterScript(ctx *ScriptContext) {}

// ExitScript is called when production script is exited.
func (s *BaseNumScriptListener) ExitScript(ctx *ScriptContext) {}
