// Code generated from NumScript.g4 by ANTLR 4.10.1. DO NOT EDIT.

package parser // NumScript

import "github.com/antlr/antlr4/runtime/Go/antlr"

// NumScriptListener is a complete listener for a parse tree produced by NumScriptParser.
type NumScriptListener interface {
	antlr.ParseTreeListener

	// EnterMonetary is called when entering the monetary production.
	EnterMonetary(c *MonetaryContext)

	// EnterMonetaryAll is called when entering the monetaryAll production.
	EnterMonetaryAll(c *MonetaryAllContext)

	// EnterLitAccount is called when entering the LitAccount production.
	EnterLitAccount(c *LitAccountContext)

	// EnterLitAsset is called when entering the LitAsset production.
	EnterLitAsset(c *LitAssetContext)

	// EnterLitNumber is called when entering the LitNumber production.
	EnterLitNumber(c *LitNumberContext)

	// EnterLitString is called when entering the LitString production.
	EnterLitString(c *LitStringContext)

	// EnterLitPortion is called when entering the LitPortion production.
	EnterLitPortion(c *LitPortionContext)

	// EnterVariable is called when entering the variable production.
	EnterVariable(c *VariableContext)

	// EnterExprAddSub is called when entering the ExprAddSub production.
	EnterExprAddSub(c *ExprAddSubContext)

	// EnterExprTernary is called when entering the ExprTernary production.
	EnterExprTernary(c *ExprTernaryContext)

	// EnterExprLogicalNot is called when entering the ExprLogicalNot production.
	EnterExprLogicalNot(c *ExprLogicalNotContext)

	// EnterExprArithmeticCondition is called when entering the ExprArithmeticCondition production.
	EnterExprArithmeticCondition(c *ExprArithmeticConditionContext)

	// EnterExprLiteral is called when entering the ExprLiteral production.
	EnterExprLiteral(c *ExprLiteralContext)

	// EnterExprLogicalOr is called when entering the ExprLogicalOr production.
	EnterExprLogicalOr(c *ExprLogicalOrContext)

	// EnterExprVariable is called when entering the ExprVariable production.
	EnterExprVariable(c *ExprVariableContext)

	// EnterExprMonetaryNew is called when entering the ExprMonetaryNew production.
	EnterExprMonetaryNew(c *ExprMonetaryNewContext)

	// EnterExprEnclosed is called when entering the ExprEnclosed production.
	EnterExprEnclosed(c *ExprEnclosedContext)

	// EnterExprLogicalAnd is called when entering the ExprLogicalAnd production.
	EnterExprLogicalAnd(c *ExprLogicalAndContext)

	// EnterAllotmentPortionConst is called when entering the AllotmentPortionConst production.
	EnterAllotmentPortionConst(c *AllotmentPortionConstContext)

	// EnterAllotmentPortionVar is called when entering the AllotmentPortionVar production.
	EnterAllotmentPortionVar(c *AllotmentPortionVarContext)

	// EnterAllotmentPortionRemaining is called when entering the AllotmentPortionRemaining production.
	EnterAllotmentPortionRemaining(c *AllotmentPortionRemainingContext)

	// EnterDestinationInOrder is called when entering the destinationInOrder production.
	EnterDestinationInOrder(c *DestinationInOrderContext)

	// EnterDestinationAllotment is called when entering the destinationAllotment production.
	EnterDestinationAllotment(c *DestinationAllotmentContext)

	// EnterIsDestination is called when entering the IsDestination production.
	EnterIsDestination(c *IsDestinationContext)

	// EnterIsKept is called when entering the IsKept production.
	EnterIsKept(c *IsKeptContext)

	// EnterDestAccount is called when entering the DestAccount production.
	EnterDestAccount(c *DestAccountContext)

	// EnterDestInOrder is called when entering the DestInOrder production.
	EnterDestInOrder(c *DestInOrderContext)

	// EnterDestAllotment is called when entering the DestAllotment production.
	EnterDestAllotment(c *DestAllotmentContext)

	// EnterSrcAccountOverdraftSpecific is called when entering the SrcAccountOverdraftSpecific production.
	EnterSrcAccountOverdraftSpecific(c *SrcAccountOverdraftSpecificContext)

	// EnterSrcAccountOverdraftUnbounded is called when entering the SrcAccountOverdraftUnbounded production.
	EnterSrcAccountOverdraftUnbounded(c *SrcAccountOverdraftUnboundedContext)

	// EnterSourceAccount is called when entering the sourceAccount production.
	EnterSourceAccount(c *SourceAccountContext)

	// EnterSourceInOrder is called when entering the sourceInOrder production.
	EnterSourceInOrder(c *SourceInOrderContext)

	// EnterSourceMaxed is called when entering the sourceMaxed production.
	EnterSourceMaxed(c *SourceMaxedContext)

	// EnterSrcAccount is called when entering the SrcAccount production.
	EnterSrcAccount(c *SrcAccountContext)

	// EnterSrcMaxed is called when entering the SrcMaxed production.
	EnterSrcMaxed(c *SrcMaxedContext)

	// EnterSrcInOrder is called when entering the SrcInOrder production.
	EnterSrcInOrder(c *SrcInOrderContext)

	// EnterSourceAllotment is called when entering the sourceAllotment production.
	EnterSourceAllotment(c *SourceAllotmentContext)

	// EnterSrc is called when entering the Src production.
	EnterSrc(c *SrcContext)

	// EnterSrcAllotment is called when entering the SrcAllotment production.
	EnterSrcAllotment(c *SrcAllotmentContext)

	// EnterPrint is called when entering the Print production.
	EnterPrint(c *PrintContext)

	// EnterSaveFromAccount is called when entering the SaveFromAccount production.
	EnterSaveFromAccount(c *SaveFromAccountContext)

	// EnterSetTxMeta is called when entering the SetTxMeta production.
	EnterSetTxMeta(c *SetTxMetaContext)

	// EnterSetAccountMeta is called when entering the SetAccountMeta production.
	EnterSetAccountMeta(c *SetAccountMetaContext)

	// EnterFail is called when entering the Fail production.
	EnterFail(c *FailContext)

	// EnterSend is called when entering the Send production.
	EnterSend(c *SendContext)

	// EnterSendAll is called when entering the SendAll production.
	EnterSendAll(c *SendAllContext)

	// EnterType_ is called when entering the type_ production.
	EnterType_(c *Type_Context)

	// EnterOriginAccountMeta is called when entering the OriginAccountMeta production.
	EnterOriginAccountMeta(c *OriginAccountMetaContext)

	// EnterOriginAccountBalance is called when entering the OriginAccountBalance production.
	EnterOriginAccountBalance(c *OriginAccountBalanceContext)

	// EnterVarDecl is called when entering the varDecl production.
	EnterVarDecl(c *VarDeclContext)

	// EnterVarListDecl is called when entering the varListDecl production.
	EnterVarListDecl(c *VarListDeclContext)

	// EnterScript is called when entering the script production.
	EnterScript(c *ScriptContext)

	// ExitMonetary is called when exiting the monetary production.
	ExitMonetary(c *MonetaryContext)

	// ExitMonetaryAll is called when exiting the monetaryAll production.
	ExitMonetaryAll(c *MonetaryAllContext)

	// ExitLitAccount is called when exiting the LitAccount production.
	ExitLitAccount(c *LitAccountContext)

	// ExitLitAsset is called when exiting the LitAsset production.
	ExitLitAsset(c *LitAssetContext)

	// ExitLitNumber is called when exiting the LitNumber production.
	ExitLitNumber(c *LitNumberContext)

	// ExitLitString is called when exiting the LitString production.
	ExitLitString(c *LitStringContext)

	// ExitLitPortion is called when exiting the LitPortion production.
	ExitLitPortion(c *LitPortionContext)

	// ExitVariable is called when exiting the variable production.
	ExitVariable(c *VariableContext)

	// ExitExprAddSub is called when exiting the ExprAddSub production.
	ExitExprAddSub(c *ExprAddSubContext)

	// ExitExprTernary is called when exiting the ExprTernary production.
	ExitExprTernary(c *ExprTernaryContext)

	// ExitExprLogicalNot is called when exiting the ExprLogicalNot production.
	ExitExprLogicalNot(c *ExprLogicalNotContext)

	// ExitExprArithmeticCondition is called when exiting the ExprArithmeticCondition production.
	ExitExprArithmeticCondition(c *ExprArithmeticConditionContext)

	// ExitExprLiteral is called when exiting the ExprLiteral production.
	ExitExprLiteral(c *ExprLiteralContext)

	// ExitExprLogicalOr is called when exiting the ExprLogicalOr production.
	ExitExprLogicalOr(c *ExprLogicalOrContext)

	// ExitExprVariable is called when exiting the ExprVariable production.
	ExitExprVariable(c *ExprVariableContext)

	// ExitExprMonetaryNew is called when exiting the ExprMonetaryNew production.
	ExitExprMonetaryNew(c *ExprMonetaryNewContext)

	// ExitExprEnclosed is called when exiting the ExprEnclosed production.
	ExitExprEnclosed(c *ExprEnclosedContext)

	// ExitExprLogicalAnd is called when exiting the ExprLogicalAnd production.
	ExitExprLogicalAnd(c *ExprLogicalAndContext)

	// ExitAllotmentPortionConst is called when exiting the AllotmentPortionConst production.
	ExitAllotmentPortionConst(c *AllotmentPortionConstContext)

	// ExitAllotmentPortionVar is called when exiting the AllotmentPortionVar production.
	ExitAllotmentPortionVar(c *AllotmentPortionVarContext)

	// ExitAllotmentPortionRemaining is called when exiting the AllotmentPortionRemaining production.
	ExitAllotmentPortionRemaining(c *AllotmentPortionRemainingContext)

	// ExitDestinationInOrder is called when exiting the destinationInOrder production.
	ExitDestinationInOrder(c *DestinationInOrderContext)

	// ExitDestinationAllotment is called when exiting the destinationAllotment production.
	ExitDestinationAllotment(c *DestinationAllotmentContext)

	// ExitIsDestination is called when exiting the IsDestination production.
	ExitIsDestination(c *IsDestinationContext)

	// ExitIsKept is called when exiting the IsKept production.
	ExitIsKept(c *IsKeptContext)

	// ExitDestAccount is called when exiting the DestAccount production.
	ExitDestAccount(c *DestAccountContext)

	// ExitDestInOrder is called when exiting the DestInOrder production.
	ExitDestInOrder(c *DestInOrderContext)

	// ExitDestAllotment is called when exiting the DestAllotment production.
	ExitDestAllotment(c *DestAllotmentContext)

	// ExitSrcAccountOverdraftSpecific is called when exiting the SrcAccountOverdraftSpecific production.
	ExitSrcAccountOverdraftSpecific(c *SrcAccountOverdraftSpecificContext)

	// ExitSrcAccountOverdraftUnbounded is called when exiting the SrcAccountOverdraftUnbounded production.
	ExitSrcAccountOverdraftUnbounded(c *SrcAccountOverdraftUnboundedContext)

	// ExitSourceAccount is called when exiting the sourceAccount production.
	ExitSourceAccount(c *SourceAccountContext)

	// ExitSourceInOrder is called when exiting the sourceInOrder production.
	ExitSourceInOrder(c *SourceInOrderContext)

	// ExitSourceMaxed is called when exiting the sourceMaxed production.
	ExitSourceMaxed(c *SourceMaxedContext)

	// ExitSrcAccount is called when exiting the SrcAccount production.
	ExitSrcAccount(c *SrcAccountContext)

	// ExitSrcMaxed is called when exiting the SrcMaxed production.
	ExitSrcMaxed(c *SrcMaxedContext)

	// ExitSrcInOrder is called when exiting the SrcInOrder production.
	ExitSrcInOrder(c *SrcInOrderContext)

	// ExitSourceAllotment is called when exiting the sourceAllotment production.
	ExitSourceAllotment(c *SourceAllotmentContext)

	// ExitSrc is called when exiting the Src production.
	ExitSrc(c *SrcContext)

	// ExitSrcAllotment is called when exiting the SrcAllotment production.
	ExitSrcAllotment(c *SrcAllotmentContext)

	// ExitPrint is called when exiting the Print production.
	ExitPrint(c *PrintContext)

	// ExitSaveFromAccount is called when exiting the SaveFromAccount production.
	ExitSaveFromAccount(c *SaveFromAccountContext)

	// ExitSetTxMeta is called when exiting the SetTxMeta production.
	ExitSetTxMeta(c *SetTxMetaContext)

	// ExitSetAccountMeta is called when exiting the SetAccountMeta production.
	ExitSetAccountMeta(c *SetAccountMetaContext)

	// ExitFail is called when exiting the Fail production.
	ExitFail(c *FailContext)

	// ExitSend is called when exiting the Send production.
	ExitSend(c *SendContext)

	// ExitSendAll is called when exiting the SendAll production.
	ExitSendAll(c *SendAllContext)

	// ExitType_ is called when exiting the type_ production.
	ExitType_(c *Type_Context)

	// ExitOriginAccountMeta is called when exiting the OriginAccountMeta production.
	ExitOriginAccountMeta(c *OriginAccountMetaContext)

	// ExitOriginAccountBalance is called when exiting the OriginAccountBalance production.
	ExitOriginAccountBalance(c *OriginAccountBalanceContext)

	// ExitVarDecl is called when exiting the varDecl production.
	ExitVarDecl(c *VarDeclContext)

	// ExitVarListDecl is called when exiting the varListDecl production.
	ExitVarListDecl(c *VarListDeclContext)

	// ExitScript is called when exiting the script production.
	ExitScript(c *ScriptContext)
}
