// Generated from /home/phlimy/Projects/Contrib/stack/components/ledger/pkg/machine/script/NumScript.g4 by ANTLR 4.9.2
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class NumScriptParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.9.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, NEWLINE=7, WHITESPACE=8, 
		MULTILINE_COMMENT=9, LINE_COMMENT=10, VARS=11, META=12, SET_TX_META=13, 
		SET_ACCOUNT_META=14, PRINT=15, FAIL=16, SEND=17, SOURCE=18, FROM=19, MAX=20, 
		DESTINATION=21, TO=22, ALLOCATE=23, OP_ADD=24, OP_SUB=25, OP_EQ=26, OP_NEQ=27, 
		OP_LT=28, OP_LTE=29, OP_GT=30, OP_GTE=31, OP_NOT=32, OP_AND=33, OP_OR=34, 
		LPAREN=35, RPAREN=36, LBRACK=37, RBRACK=38, LBRACE=39, RBRACE=40, EQ=41, 
		TY_ACCOUNT=42, TY_ASSET=43, TY_NUMBER=44, TY_MONETARY=45, TY_PORTION=46, 
		TY_STRING=47, TY_BOOL=48, STRING=49, PORTION=50, REMAINING=51, KEPT=52, 
		BALANCE=53, SAVE=54, NUMBER=55, PERCENT=56, VARIABLE_NAME=57, ACCOUNT=58, 
		ASSET=59;
	public static final int
		RULE_monetary = 0, RULE_monetaryAll = 1, RULE_literal = 2, RULE_variable = 3, 
		RULE_expression = 4, RULE_allotmentPortion = 5, RULE_destinationInOrder = 6, 
		RULE_destinationAllotment = 7, RULE_keptOrDestination = 8, RULE_destination = 9, 
		RULE_sourceAccountOverdraft = 10, RULE_sourceAccount = 11, RULE_sourceInOrder = 12, 
		RULE_sourceMaxed = 13, RULE_source = 14, RULE_sourceAllotment = 15, RULE_valueAwareSource = 16, 
		RULE_statement = 17, RULE_type_ = 18, RULE_origin = 19, RULE_varDecl = 20, 
		RULE_varListDecl = 21, RULE_script = 22;
	private static String[] makeRuleNames() {
		return new String[] {
			"monetary", "monetaryAll", "literal", "variable", "expression", "allotmentPortion", 
			"destinationInOrder", "destinationAllotment", "keptOrDestination", "destination", 
			"sourceAccountOverdraft", "sourceAccount", "sourceInOrder", "sourceMaxed", 
			"source", "sourceAllotment", "valueAwareSource", "statement", "type_", 
			"origin", "varDecl", "varListDecl", "script"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'*'", "'?'", "':'", "'allowing overdraft up to'", "'allowing unbounded overdraft'", 
			"','", null, null, null, null, "'vars'", "'meta'", "'set_tx_meta'", "'set_account_meta'", 
			"'print'", "'fail'", "'send'", "'source'", "'from'", "'max'", "'destination'", 
			"'to'", "'allocate'", "'+'", "'-'", "'=='", "'!='", "'<'", "'<='", "'>'", 
			"'>='", "'!'", "'&&'", "'||'", "'('", "')'", "'['", "']'", "'{'", "'}'", 
			"'='", "'account'", "'asset'", "'number'", "'monetary'", "'portion'", 
			"'string'", "'bool'", null, null, "'remaining'", "'kept'", "'balance'", 
			"'save'", null, "'%'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, "NEWLINE", "WHITESPACE", "MULTILINE_COMMENT", 
			"LINE_COMMENT", "VARS", "META", "SET_TX_META", "SET_ACCOUNT_META", "PRINT", 
			"FAIL", "SEND", "SOURCE", "FROM", "MAX", "DESTINATION", "TO", "ALLOCATE", 
			"OP_ADD", "OP_SUB", "OP_EQ", "OP_NEQ", "OP_LT", "OP_LTE", "OP_GT", "OP_GTE", 
			"OP_NOT", "OP_AND", "OP_OR", "LPAREN", "RPAREN", "LBRACK", "RBRACK", 
			"LBRACE", "RBRACE", "EQ", "TY_ACCOUNT", "TY_ASSET", "TY_NUMBER", "TY_MONETARY", 
			"TY_PORTION", "TY_STRING", "TY_BOOL", "STRING", "PORTION", "REMAINING", 
			"KEPT", "BALANCE", "SAVE", "NUMBER", "PERCENT", "VARIABLE_NAME", "ACCOUNT", 
			"ASSET"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "NumScript.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public NumScriptParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class MonetaryContext extends ParserRuleContext {
		public ExpressionContext asset;
		public ExpressionContext amt;
		public TerminalNode LBRACK() { return getToken(NumScriptParser.LBRACK, 0); }
		public TerminalNode RBRACK() { return getToken(NumScriptParser.RBRACK, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public MonetaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_monetary; }
	}

	public final MonetaryContext monetary() throws RecognitionException {
		MonetaryContext _localctx = new MonetaryContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_monetary);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(46);
			match(LBRACK);
			setState(47);
			((MonetaryContext)_localctx).asset = expression(0);
			setState(48);
			((MonetaryContext)_localctx).amt = expression(0);
			setState(49);
			match(RBRACK);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MonetaryAllContext extends ParserRuleContext {
		public ExpressionContext asset;
		public TerminalNode LBRACK() { return getToken(NumScriptParser.LBRACK, 0); }
		public TerminalNode RBRACK() { return getToken(NumScriptParser.RBRACK, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public MonetaryAllContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_monetaryAll; }
	}

	public final MonetaryAllContext monetaryAll() throws RecognitionException {
		MonetaryAllContext _localctx = new MonetaryAllContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_monetaryAll);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(51);
			match(LBRACK);
			setState(52);
			((MonetaryAllContext)_localctx).asset = expression(0);
			setState(53);
			match(T__0);
			setState(54);
			match(RBRACK);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LiteralContext extends ParserRuleContext {
		public LiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal; }
	 
		public LiteralContext() { }
		public void copyFrom(LiteralContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class LitPortionContext extends LiteralContext {
		public TerminalNode PORTION() { return getToken(NumScriptParser.PORTION, 0); }
		public LitPortionContext(LiteralContext ctx) { copyFrom(ctx); }
	}
	public static class LitStringContext extends LiteralContext {
		public TerminalNode STRING() { return getToken(NumScriptParser.STRING, 0); }
		public LitStringContext(LiteralContext ctx) { copyFrom(ctx); }
	}
	public static class LitAccountContext extends LiteralContext {
		public TerminalNode ACCOUNT() { return getToken(NumScriptParser.ACCOUNT, 0); }
		public LitAccountContext(LiteralContext ctx) { copyFrom(ctx); }
	}
	public static class LitAssetContext extends LiteralContext {
		public TerminalNode ASSET() { return getToken(NumScriptParser.ASSET, 0); }
		public LitAssetContext(LiteralContext ctx) { copyFrom(ctx); }
	}
	public static class LitNumberContext extends LiteralContext {
		public TerminalNode NUMBER() { return getToken(NumScriptParser.NUMBER, 0); }
		public LitNumberContext(LiteralContext ctx) { copyFrom(ctx); }
	}

	public final LiteralContext literal() throws RecognitionException {
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_literal);
		try {
			setState(61);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ACCOUNT:
				_localctx = new LitAccountContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(56);
				match(ACCOUNT);
				}
				break;
			case ASSET:
				_localctx = new LitAssetContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(57);
				match(ASSET);
				}
				break;
			case NUMBER:
				_localctx = new LitNumberContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(58);
				match(NUMBER);
				}
				break;
			case STRING:
				_localctx = new LitStringContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(59);
				match(STRING);
				}
				break;
			case PORTION:
				_localctx = new LitPortionContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(60);
				match(PORTION);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class VariableContext extends ParserRuleContext {
		public TerminalNode VARIABLE_NAME() { return getToken(NumScriptParser.VARIABLE_NAME, 0); }
		public VariableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variable; }
	}

	public final VariableContext variable() throws RecognitionException {
		VariableContext _localctx = new VariableContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_variable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(63);
			match(VARIABLE_NAME);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
	 
		public ExpressionContext() { }
		public void copyFrom(ExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ExprAddSubContext extends ExpressionContext {
		public ExpressionContext lhs;
		public Token op;
		public ExpressionContext rhs;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode OP_ADD() { return getToken(NumScriptParser.OP_ADD, 0); }
		public TerminalNode OP_SUB() { return getToken(NumScriptParser.OP_SUB, 0); }
		public ExprAddSubContext(ExpressionContext ctx) { copyFrom(ctx); }
	}
	public static class ExprTernaryContext extends ExpressionContext {
		public ExpressionContext cond;
		public ExpressionContext ifTrue;
		public ExpressionContext ifFalse;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ExprTernaryContext(ExpressionContext ctx) { copyFrom(ctx); }
	}
	public static class ExprLogicalNotContext extends ExpressionContext {
		public ExpressionContext lhs;
		public TerminalNode OP_NOT() { return getToken(NumScriptParser.OP_NOT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ExprLogicalNotContext(ExpressionContext ctx) { copyFrom(ctx); }
	}
	public static class ExprArithmeticConditionContext extends ExpressionContext {
		public ExpressionContext lhs;
		public Token op;
		public ExpressionContext rhs;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode OP_EQ() { return getToken(NumScriptParser.OP_EQ, 0); }
		public TerminalNode OP_NEQ() { return getToken(NumScriptParser.OP_NEQ, 0); }
		public TerminalNode OP_LT() { return getToken(NumScriptParser.OP_LT, 0); }
		public TerminalNode OP_LTE() { return getToken(NumScriptParser.OP_LTE, 0); }
		public TerminalNode OP_GT() { return getToken(NumScriptParser.OP_GT, 0); }
		public TerminalNode OP_GTE() { return getToken(NumScriptParser.OP_GTE, 0); }
		public ExprArithmeticConditionContext(ExpressionContext ctx) { copyFrom(ctx); }
	}
	public static class ExprLiteralContext extends ExpressionContext {
		public LiteralContext lit;
		public LiteralContext literal() {
			return getRuleContext(LiteralContext.class,0);
		}
		public ExprLiteralContext(ExpressionContext ctx) { copyFrom(ctx); }
	}
	public static class ExprLogicalOrContext extends ExpressionContext {
		public ExpressionContext lhs;
		public Token op;
		public ExpressionContext rhs;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode OP_OR() { return getToken(NumScriptParser.OP_OR, 0); }
		public ExprLogicalOrContext(ExpressionContext ctx) { copyFrom(ctx); }
	}
	public static class ExprVariableContext extends ExpressionContext {
		public VariableContext var_;
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public ExprVariableContext(ExpressionContext ctx) { copyFrom(ctx); }
	}
	public static class ExprMonetaryNewContext extends ExpressionContext {
		public MonetaryContext mon;
		public MonetaryContext monetary() {
			return getRuleContext(MonetaryContext.class,0);
		}
		public ExprMonetaryNewContext(ExpressionContext ctx) { copyFrom(ctx); }
	}
	public static class ExprEnclosedContext extends ExpressionContext {
		public ExpressionContext expr;
		public TerminalNode LPAREN() { return getToken(NumScriptParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(NumScriptParser.RPAREN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ExprEnclosedContext(ExpressionContext ctx) { copyFrom(ctx); }
	}
	public static class ExprLogicalAndContext extends ExpressionContext {
		public ExpressionContext lhs;
		public Token op;
		public ExpressionContext rhs;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode OP_AND() { return getToken(NumScriptParser.OP_AND, 0); }
		public ExprLogicalAndContext(ExpressionContext ctx) { copyFrom(ctx); }
	}

	public final ExpressionContext expression() throws RecognitionException {
		return expression(0);
	}

	private ExpressionContext expression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
		ExpressionContext _prevctx = _localctx;
		int _startState = 8;
		enterRecursionRule(_localctx, 8, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(75);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case OP_NOT:
				{
				_localctx = new ExprLogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(66);
				match(OP_NOT);
				setState(67);
				((ExprLogicalNotContext)_localctx).lhs = expression(8);
				}
				break;
			case STRING:
			case PORTION:
			case NUMBER:
			case ACCOUNT:
			case ASSET:
				{
				_localctx = new ExprLiteralContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(68);
				((ExprLiteralContext)_localctx).lit = literal();
				}
				break;
			case VARIABLE_NAME:
				{
				_localctx = new ExprVariableContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(69);
				((ExprVariableContext)_localctx).var_ = variable();
				}
				break;
			case LBRACK:
				{
				_localctx = new ExprMonetaryNewContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(70);
				((ExprMonetaryNewContext)_localctx).mon = monetary();
				}
				break;
			case LPAREN:
				{
				_localctx = new ExprEnclosedContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(71);
				match(LPAREN);
				setState(72);
				((ExprEnclosedContext)_localctx).expr = expression(0);
				setState(73);
				match(RPAREN);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(97);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(95);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
					case 1:
						{
						_localctx = new ExprAddSubContext(new ExpressionContext(_parentctx, _parentState));
						((ExprAddSubContext)_localctx).lhs = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(77);
						if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
						setState(78);
						((ExprAddSubContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==OP_ADD || _la==OP_SUB) ) {
							((ExprAddSubContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(79);
						((ExprAddSubContext)_localctx).rhs = expression(11);
						}
						break;
					case 2:
						{
						_localctx = new ExprArithmeticConditionContext(new ExpressionContext(_parentctx, _parentState));
						((ExprArithmeticConditionContext)_localctx).lhs = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(80);
						if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
						setState(81);
						((ExprArithmeticConditionContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OP_EQ) | (1L << OP_NEQ) | (1L << OP_LT) | (1L << OP_LTE) | (1L << OP_GT) | (1L << OP_GTE))) != 0)) ) {
							((ExprArithmeticConditionContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(82);
						((ExprArithmeticConditionContext)_localctx).rhs = expression(10);
						}
						break;
					case 3:
						{
						_localctx = new ExprLogicalAndContext(new ExpressionContext(_parentctx, _parentState));
						((ExprLogicalAndContext)_localctx).lhs = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(83);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(84);
						((ExprLogicalAndContext)_localctx).op = match(OP_AND);
						setState(85);
						((ExprLogicalAndContext)_localctx).rhs = expression(8);
						}
						break;
					case 4:
						{
						_localctx = new ExprLogicalOrContext(new ExpressionContext(_parentctx, _parentState));
						((ExprLogicalOrContext)_localctx).lhs = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(86);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(87);
						((ExprLogicalOrContext)_localctx).op = match(OP_OR);
						setState(88);
						((ExprLogicalOrContext)_localctx).rhs = expression(7);
						}
						break;
					case 5:
						{
						_localctx = new ExprTernaryContext(new ExpressionContext(_parentctx, _parentState));
						((ExprTernaryContext)_localctx).cond = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(89);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(90);
						match(T__1);
						setState(91);
						((ExprTernaryContext)_localctx).ifTrue = expression(0);
						setState(92);
						match(T__2);
						setState(93);
						((ExprTernaryContext)_localctx).ifFalse = expression(3);
						}
						break;
					}
					} 
				}
				setState(99);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class AllotmentPortionContext extends ParserRuleContext {
		public AllotmentPortionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_allotmentPortion; }
	 
		public AllotmentPortionContext() { }
		public void copyFrom(AllotmentPortionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class AllotmentPortionRemainingContext extends AllotmentPortionContext {
		public TerminalNode REMAINING() { return getToken(NumScriptParser.REMAINING, 0); }
		public AllotmentPortionRemainingContext(AllotmentPortionContext ctx) { copyFrom(ctx); }
	}
	public static class AllotmentPortionVarContext extends AllotmentPortionContext {
		public VariableContext por;
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public AllotmentPortionVarContext(AllotmentPortionContext ctx) { copyFrom(ctx); }
	}
	public static class AllotmentPortionConstContext extends AllotmentPortionContext {
		public TerminalNode PORTION() { return getToken(NumScriptParser.PORTION, 0); }
		public AllotmentPortionConstContext(AllotmentPortionContext ctx) { copyFrom(ctx); }
	}

	public final AllotmentPortionContext allotmentPortion() throws RecognitionException {
		AllotmentPortionContext _localctx = new AllotmentPortionContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_allotmentPortion);
		try {
			setState(103);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PORTION:
				_localctx = new AllotmentPortionConstContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(100);
				match(PORTION);
				}
				break;
			case VARIABLE_NAME:
				_localctx = new AllotmentPortionVarContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(101);
				((AllotmentPortionVarContext)_localctx).por = variable();
				}
				break;
			case REMAINING:
				_localctx = new AllotmentPortionRemainingContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(102);
				match(REMAINING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DestinationInOrderContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> amounts = new ArrayList<ExpressionContext>();
		public KeptOrDestinationContext keptOrDestination;
		public List<KeptOrDestinationContext> dests = new ArrayList<KeptOrDestinationContext>();
		public KeptOrDestinationContext remainingDest;
		public TerminalNode LBRACE() { return getToken(NumScriptParser.LBRACE, 0); }
		public List<TerminalNode> NEWLINE() { return getTokens(NumScriptParser.NEWLINE); }
		public TerminalNode NEWLINE(int i) {
			return getToken(NumScriptParser.NEWLINE, i);
		}
		public TerminalNode REMAINING() { return getToken(NumScriptParser.REMAINING, 0); }
		public TerminalNode RBRACE() { return getToken(NumScriptParser.RBRACE, 0); }
		public List<KeptOrDestinationContext> keptOrDestination() {
			return getRuleContexts(KeptOrDestinationContext.class);
		}
		public KeptOrDestinationContext keptOrDestination(int i) {
			return getRuleContext(KeptOrDestinationContext.class,i);
		}
		public List<TerminalNode> MAX() { return getTokens(NumScriptParser.MAX); }
		public TerminalNode MAX(int i) {
			return getToken(NumScriptParser.MAX, i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public DestinationInOrderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_destinationInOrder; }
	}

	public final DestinationInOrderContext destinationInOrder() throws RecognitionException {
		DestinationInOrderContext _localctx = new DestinationInOrderContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_destinationInOrder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(105);
			match(LBRACE);
			setState(106);
			match(NEWLINE);
			setState(112); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(107);
				match(MAX);
				setState(108);
				((DestinationInOrderContext)_localctx).expression = expression(0);
				((DestinationInOrderContext)_localctx).amounts.add(((DestinationInOrderContext)_localctx).expression);
				setState(109);
				((DestinationInOrderContext)_localctx).keptOrDestination = keptOrDestination();
				((DestinationInOrderContext)_localctx).dests.add(((DestinationInOrderContext)_localctx).keptOrDestination);
				setState(110);
				match(NEWLINE);
				}
				}
				setState(114); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==MAX );
			setState(116);
			match(REMAINING);
			setState(117);
			((DestinationInOrderContext)_localctx).remainingDest = keptOrDestination();
			setState(118);
			match(NEWLINE);
			setState(119);
			match(RBRACE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DestinationAllotmentContext extends ParserRuleContext {
		public AllotmentPortionContext allotmentPortion;
		public List<AllotmentPortionContext> portions = new ArrayList<AllotmentPortionContext>();
		public KeptOrDestinationContext keptOrDestination;
		public List<KeptOrDestinationContext> dests = new ArrayList<KeptOrDestinationContext>();
		public TerminalNode LBRACE() { return getToken(NumScriptParser.LBRACE, 0); }
		public List<TerminalNode> NEWLINE() { return getTokens(NumScriptParser.NEWLINE); }
		public TerminalNode NEWLINE(int i) {
			return getToken(NumScriptParser.NEWLINE, i);
		}
		public TerminalNode RBRACE() { return getToken(NumScriptParser.RBRACE, 0); }
		public List<AllotmentPortionContext> allotmentPortion() {
			return getRuleContexts(AllotmentPortionContext.class);
		}
		public AllotmentPortionContext allotmentPortion(int i) {
			return getRuleContext(AllotmentPortionContext.class,i);
		}
		public List<KeptOrDestinationContext> keptOrDestination() {
			return getRuleContexts(KeptOrDestinationContext.class);
		}
		public KeptOrDestinationContext keptOrDestination(int i) {
			return getRuleContext(KeptOrDestinationContext.class,i);
		}
		public DestinationAllotmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_destinationAllotment; }
	}

	public final DestinationAllotmentContext destinationAllotment() throws RecognitionException {
		DestinationAllotmentContext _localctx = new DestinationAllotmentContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_destinationAllotment);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(121);
			match(LBRACE);
			setState(122);
			match(NEWLINE);
			setState(127); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(123);
				((DestinationAllotmentContext)_localctx).allotmentPortion = allotmentPortion();
				((DestinationAllotmentContext)_localctx).portions.add(((DestinationAllotmentContext)_localctx).allotmentPortion);
				setState(124);
				((DestinationAllotmentContext)_localctx).keptOrDestination = keptOrDestination();
				((DestinationAllotmentContext)_localctx).dests.add(((DestinationAllotmentContext)_localctx).keptOrDestination);
				setState(125);
				match(NEWLINE);
				}
				}
				setState(129); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << PORTION) | (1L << REMAINING) | (1L << VARIABLE_NAME))) != 0) );
			setState(131);
			match(RBRACE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeptOrDestinationContext extends ParserRuleContext {
		public KeptOrDestinationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keptOrDestination; }
	 
		public KeptOrDestinationContext() { }
		public void copyFrom(KeptOrDestinationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class IsKeptContext extends KeptOrDestinationContext {
		public TerminalNode KEPT() { return getToken(NumScriptParser.KEPT, 0); }
		public IsKeptContext(KeptOrDestinationContext ctx) { copyFrom(ctx); }
	}
	public static class IsDestinationContext extends KeptOrDestinationContext {
		public TerminalNode TO() { return getToken(NumScriptParser.TO, 0); }
		public DestinationContext destination() {
			return getRuleContext(DestinationContext.class,0);
		}
		public IsDestinationContext(KeptOrDestinationContext ctx) { copyFrom(ctx); }
	}

	public final KeptOrDestinationContext keptOrDestination() throws RecognitionException {
		KeptOrDestinationContext _localctx = new KeptOrDestinationContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_keptOrDestination);
		try {
			setState(136);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TO:
				_localctx = new IsDestinationContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(133);
				match(TO);
				setState(134);
				destination();
				}
				break;
			case KEPT:
				_localctx = new IsKeptContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(135);
				match(KEPT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DestinationContext extends ParserRuleContext {
		public DestinationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_destination; }
	 
		public DestinationContext() { }
		public void copyFrom(DestinationContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DestAccountContext extends DestinationContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public DestAccountContext(DestinationContext ctx) { copyFrom(ctx); }
	}
	public static class DestAllotmentContext extends DestinationContext {
		public DestinationAllotmentContext destinationAllotment() {
			return getRuleContext(DestinationAllotmentContext.class,0);
		}
		public DestAllotmentContext(DestinationContext ctx) { copyFrom(ctx); }
	}
	public static class DestInOrderContext extends DestinationContext {
		public DestinationInOrderContext destinationInOrder() {
			return getRuleContext(DestinationInOrderContext.class,0);
		}
		public DestInOrderContext(DestinationContext ctx) { copyFrom(ctx); }
	}

	public final DestinationContext destination() throws RecognitionException {
		DestinationContext _localctx = new DestinationContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_destination);
		try {
			setState(141);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
			case 1:
				_localctx = new DestAccountContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(138);
				expression(0);
				}
				break;
			case 2:
				_localctx = new DestInOrderContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(139);
				destinationInOrder();
				}
				break;
			case 3:
				_localctx = new DestAllotmentContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(140);
				destinationAllotment();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SourceAccountOverdraftContext extends ParserRuleContext {
		public SourceAccountOverdraftContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sourceAccountOverdraft; }
	 
		public SourceAccountOverdraftContext() { }
		public void copyFrom(SourceAccountOverdraftContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SrcAccountOverdraftSpecificContext extends SourceAccountOverdraftContext {
		public ExpressionContext specific;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SrcAccountOverdraftSpecificContext(SourceAccountOverdraftContext ctx) { copyFrom(ctx); }
	}
	public static class SrcAccountOverdraftUnboundedContext extends SourceAccountOverdraftContext {
		public SrcAccountOverdraftUnboundedContext(SourceAccountOverdraftContext ctx) { copyFrom(ctx); }
	}

	public final SourceAccountOverdraftContext sourceAccountOverdraft() throws RecognitionException {
		SourceAccountOverdraftContext _localctx = new SourceAccountOverdraftContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_sourceAccountOverdraft);
		try {
			setState(146);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__3:
				_localctx = new SrcAccountOverdraftSpecificContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(143);
				match(T__3);
				setState(144);
				((SrcAccountOverdraftSpecificContext)_localctx).specific = expression(0);
				}
				break;
			case T__4:
				_localctx = new SrcAccountOverdraftUnboundedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(145);
				match(T__4);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SourceAccountContext extends ParserRuleContext {
		public ExpressionContext account;
		public SourceAccountOverdraftContext overdraft;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SourceAccountOverdraftContext sourceAccountOverdraft() {
			return getRuleContext(SourceAccountOverdraftContext.class,0);
		}
		public SourceAccountContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sourceAccount; }
	}

	public final SourceAccountContext sourceAccount() throws RecognitionException {
		SourceAccountContext _localctx = new SourceAccountContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_sourceAccount);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(148);
			((SourceAccountContext)_localctx).account = expression(0);
			setState(150);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__3 || _la==T__4) {
				{
				setState(149);
				((SourceAccountContext)_localctx).overdraft = sourceAccountOverdraft();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SourceInOrderContext extends ParserRuleContext {
		public SourceContext source;
		public List<SourceContext> sources = new ArrayList<SourceContext>();
		public TerminalNode LBRACE() { return getToken(NumScriptParser.LBRACE, 0); }
		public List<TerminalNode> NEWLINE() { return getTokens(NumScriptParser.NEWLINE); }
		public TerminalNode NEWLINE(int i) {
			return getToken(NumScriptParser.NEWLINE, i);
		}
		public TerminalNode RBRACE() { return getToken(NumScriptParser.RBRACE, 0); }
		public List<SourceContext> source() {
			return getRuleContexts(SourceContext.class);
		}
		public SourceContext source(int i) {
			return getRuleContext(SourceContext.class,i);
		}
		public SourceInOrderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sourceInOrder; }
	}

	public final SourceInOrderContext sourceInOrder() throws RecognitionException {
		SourceInOrderContext _localctx = new SourceInOrderContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_sourceInOrder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(152);
			match(LBRACE);
			setState(153);
			match(NEWLINE);
			setState(157); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(154);
				((SourceInOrderContext)_localctx).source = source();
				((SourceInOrderContext)_localctx).sources.add(((SourceInOrderContext)_localctx).source);
				setState(155);
				match(NEWLINE);
				}
				}
				setState(159); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MAX) | (1L << OP_NOT) | (1L << LPAREN) | (1L << LBRACK) | (1L << LBRACE) | (1L << STRING) | (1L << PORTION) | (1L << NUMBER) | (1L << VARIABLE_NAME) | (1L << ACCOUNT) | (1L << ASSET))) != 0) );
			setState(161);
			match(RBRACE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SourceMaxedContext extends ParserRuleContext {
		public ExpressionContext max;
		public SourceContext src;
		public TerminalNode MAX() { return getToken(NumScriptParser.MAX, 0); }
		public TerminalNode FROM() { return getToken(NumScriptParser.FROM, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SourceContext source() {
			return getRuleContext(SourceContext.class,0);
		}
		public SourceMaxedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sourceMaxed; }
	}

	public final SourceMaxedContext sourceMaxed() throws RecognitionException {
		SourceMaxedContext _localctx = new SourceMaxedContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_sourceMaxed);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(163);
			match(MAX);
			setState(164);
			((SourceMaxedContext)_localctx).max = expression(0);
			setState(165);
			match(FROM);
			setState(166);
			((SourceMaxedContext)_localctx).src = source();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SourceContext extends ParserRuleContext {
		public SourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_source; }
	 
		public SourceContext() { }
		public void copyFrom(SourceContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SrcAccountContext extends SourceContext {
		public SourceAccountContext sourceAccount() {
			return getRuleContext(SourceAccountContext.class,0);
		}
		public SrcAccountContext(SourceContext ctx) { copyFrom(ctx); }
	}
	public static class SrcMaxedContext extends SourceContext {
		public SourceMaxedContext sourceMaxed() {
			return getRuleContext(SourceMaxedContext.class,0);
		}
		public SrcMaxedContext(SourceContext ctx) { copyFrom(ctx); }
	}
	public static class SrcInOrderContext extends SourceContext {
		public SourceInOrderContext sourceInOrder() {
			return getRuleContext(SourceInOrderContext.class,0);
		}
		public SrcInOrderContext(SourceContext ctx) { copyFrom(ctx); }
	}

	public final SourceContext source() throws RecognitionException {
		SourceContext _localctx = new SourceContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_source);
		try {
			setState(171);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case OP_NOT:
			case LPAREN:
			case LBRACK:
			case STRING:
			case PORTION:
			case NUMBER:
			case VARIABLE_NAME:
			case ACCOUNT:
			case ASSET:
				_localctx = new SrcAccountContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(168);
				sourceAccount();
				}
				break;
			case MAX:
				_localctx = new SrcMaxedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(169);
				sourceMaxed();
				}
				break;
			case LBRACE:
				_localctx = new SrcInOrderContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(170);
				sourceInOrder();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SourceAllotmentContext extends ParserRuleContext {
		public AllotmentPortionContext allotmentPortion;
		public List<AllotmentPortionContext> portions = new ArrayList<AllotmentPortionContext>();
		public SourceContext source;
		public List<SourceContext> sources = new ArrayList<SourceContext>();
		public TerminalNode LBRACE() { return getToken(NumScriptParser.LBRACE, 0); }
		public List<TerminalNode> NEWLINE() { return getTokens(NumScriptParser.NEWLINE); }
		public TerminalNode NEWLINE(int i) {
			return getToken(NumScriptParser.NEWLINE, i);
		}
		public TerminalNode RBRACE() { return getToken(NumScriptParser.RBRACE, 0); }
		public List<TerminalNode> FROM() { return getTokens(NumScriptParser.FROM); }
		public TerminalNode FROM(int i) {
			return getToken(NumScriptParser.FROM, i);
		}
		public List<AllotmentPortionContext> allotmentPortion() {
			return getRuleContexts(AllotmentPortionContext.class);
		}
		public AllotmentPortionContext allotmentPortion(int i) {
			return getRuleContext(AllotmentPortionContext.class,i);
		}
		public List<SourceContext> source() {
			return getRuleContexts(SourceContext.class);
		}
		public SourceContext source(int i) {
			return getRuleContext(SourceContext.class,i);
		}
		public SourceAllotmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sourceAllotment; }
	}

	public final SourceAllotmentContext sourceAllotment() throws RecognitionException {
		SourceAllotmentContext _localctx = new SourceAllotmentContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_sourceAllotment);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(173);
			match(LBRACE);
			setState(174);
			match(NEWLINE);
			setState(180); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(175);
				((SourceAllotmentContext)_localctx).allotmentPortion = allotmentPortion();
				((SourceAllotmentContext)_localctx).portions.add(((SourceAllotmentContext)_localctx).allotmentPortion);
				setState(176);
				match(FROM);
				setState(177);
				((SourceAllotmentContext)_localctx).source = source();
				((SourceAllotmentContext)_localctx).sources.add(((SourceAllotmentContext)_localctx).source);
				setState(178);
				match(NEWLINE);
				}
				}
				setState(182); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << PORTION) | (1L << REMAINING) | (1L << VARIABLE_NAME))) != 0) );
			setState(184);
			match(RBRACE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueAwareSourceContext extends ParserRuleContext {
		public ValueAwareSourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueAwareSource; }
	 
		public ValueAwareSourceContext() { }
		public void copyFrom(ValueAwareSourceContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SrcContext extends ValueAwareSourceContext {
		public SourceContext source() {
			return getRuleContext(SourceContext.class,0);
		}
		public SrcContext(ValueAwareSourceContext ctx) { copyFrom(ctx); }
	}
	public static class SrcAllotmentContext extends ValueAwareSourceContext {
		public SourceAllotmentContext sourceAllotment() {
			return getRuleContext(SourceAllotmentContext.class,0);
		}
		public SrcAllotmentContext(ValueAwareSourceContext ctx) { copyFrom(ctx); }
	}

	public final ValueAwareSourceContext valueAwareSource() throws RecognitionException {
		ValueAwareSourceContext _localctx = new ValueAwareSourceContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_valueAwareSource);
		try {
			setState(188);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
			case 1:
				_localctx = new SrcContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(186);
				source();
				}
				break;
			case 2:
				_localctx = new SrcAllotmentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(187);
				sourceAllotment();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class PrintContext extends StatementContext {
		public ExpressionContext expr;
		public TerminalNode PRINT() { return getToken(NumScriptParser.PRINT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public PrintContext(StatementContext ctx) { copyFrom(ctx); }
	}
	public static class SendAllContext extends StatementContext {
		public MonetaryAllContext monAll;
		public SourceContext src;
		public DestinationContext dest;
		public TerminalNode SEND() { return getToken(NumScriptParser.SEND, 0); }
		public TerminalNode LPAREN() { return getToken(NumScriptParser.LPAREN, 0); }
		public List<TerminalNode> NEWLINE() { return getTokens(NumScriptParser.NEWLINE); }
		public TerminalNode NEWLINE(int i) {
			return getToken(NumScriptParser.NEWLINE, i);
		}
		public TerminalNode RPAREN() { return getToken(NumScriptParser.RPAREN, 0); }
		public MonetaryAllContext monetaryAll() {
			return getRuleContext(MonetaryAllContext.class,0);
		}
		public TerminalNode SOURCE() { return getToken(NumScriptParser.SOURCE, 0); }
		public List<TerminalNode> EQ() { return getTokens(NumScriptParser.EQ); }
		public TerminalNode EQ(int i) {
			return getToken(NumScriptParser.EQ, i);
		}
		public TerminalNode DESTINATION() { return getToken(NumScriptParser.DESTINATION, 0); }
		public SourceContext source() {
			return getRuleContext(SourceContext.class,0);
		}
		public DestinationContext destination() {
			return getRuleContext(DestinationContext.class,0);
		}
		public SendAllContext(StatementContext ctx) { copyFrom(ctx); }
	}
	public static class SaveFromAccountContext extends StatementContext {
		public ExpressionContext mon;
		public MonetaryAllContext monAll;
		public ExpressionContext acc;
		public TerminalNode SAVE() { return getToken(NumScriptParser.SAVE, 0); }
		public TerminalNode FROM() { return getToken(NumScriptParser.FROM, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public MonetaryAllContext monetaryAll() {
			return getRuleContext(MonetaryAllContext.class,0);
		}
		public SaveFromAccountContext(StatementContext ctx) { copyFrom(ctx); }
	}
	public static class SetTxMetaContext extends StatementContext {
		public Token key;
		public ExpressionContext value;
		public TerminalNode SET_TX_META() { return getToken(NumScriptParser.SET_TX_META, 0); }
		public TerminalNode LPAREN() { return getToken(NumScriptParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(NumScriptParser.RPAREN, 0); }
		public TerminalNode STRING() { return getToken(NumScriptParser.STRING, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SetTxMetaContext(StatementContext ctx) { copyFrom(ctx); }
	}
	public static class SetAccountMetaContext extends StatementContext {
		public ExpressionContext acc;
		public Token key;
		public ExpressionContext value;
		public TerminalNode SET_ACCOUNT_META() { return getToken(NumScriptParser.SET_ACCOUNT_META, 0); }
		public TerminalNode LPAREN() { return getToken(NumScriptParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(NumScriptParser.RPAREN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode STRING() { return getToken(NumScriptParser.STRING, 0); }
		public SetAccountMetaContext(StatementContext ctx) { copyFrom(ctx); }
	}
	public static class FailContext extends StatementContext {
		public TerminalNode FAIL() { return getToken(NumScriptParser.FAIL, 0); }
		public FailContext(StatementContext ctx) { copyFrom(ctx); }
	}
	public static class SendContext extends StatementContext {
		public ExpressionContext mon;
		public ValueAwareSourceContext src;
		public DestinationContext dest;
		public TerminalNode SEND() { return getToken(NumScriptParser.SEND, 0); }
		public TerminalNode LPAREN() { return getToken(NumScriptParser.LPAREN, 0); }
		public List<TerminalNode> NEWLINE() { return getTokens(NumScriptParser.NEWLINE); }
		public TerminalNode NEWLINE(int i) {
			return getToken(NumScriptParser.NEWLINE, i);
		}
		public TerminalNode RPAREN() { return getToken(NumScriptParser.RPAREN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode SOURCE() { return getToken(NumScriptParser.SOURCE, 0); }
		public List<TerminalNode> EQ() { return getTokens(NumScriptParser.EQ); }
		public TerminalNode EQ(int i) {
			return getToken(NumScriptParser.EQ, i);
		}
		public TerminalNode DESTINATION() { return getToken(NumScriptParser.DESTINATION, 0); }
		public ValueAwareSourceContext valueAwareSource() {
			return getRuleContext(ValueAwareSourceContext.class,0);
		}
		public DestinationContext destination() {
			return getRuleContext(DestinationContext.class,0);
		}
		public SendContext(StatementContext ctx) { copyFrom(ctx); }
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_statement);
		try {
			setState(267);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
			case 1:
				_localctx = new PrintContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(190);
				match(PRINT);
				setState(191);
				((PrintContext)_localctx).expr = expression(0);
				}
				break;
			case 2:
				_localctx = new SaveFromAccountContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(192);
				match(SAVE);
				setState(195);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
				case 1:
					{
					setState(193);
					((SaveFromAccountContext)_localctx).mon = expression(0);
					}
					break;
				case 2:
					{
					setState(194);
					((SaveFromAccountContext)_localctx).monAll = monetaryAll();
					}
					break;
				}
				setState(197);
				match(FROM);
				setState(198);
				((SaveFromAccountContext)_localctx).acc = expression(0);
				}
				break;
			case 3:
				_localctx = new SetTxMetaContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(200);
				match(SET_TX_META);
				setState(201);
				match(LPAREN);
				setState(202);
				((SetTxMetaContext)_localctx).key = match(STRING);
				setState(203);
				match(T__5);
				setState(204);
				((SetTxMetaContext)_localctx).value = expression(0);
				setState(205);
				match(RPAREN);
				}
				break;
			case 4:
				_localctx = new SetAccountMetaContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(207);
				match(SET_ACCOUNT_META);
				setState(208);
				match(LPAREN);
				setState(209);
				((SetAccountMetaContext)_localctx).acc = expression(0);
				setState(210);
				match(T__5);
				setState(211);
				((SetAccountMetaContext)_localctx).key = match(STRING);
				setState(212);
				match(T__5);
				setState(213);
				((SetAccountMetaContext)_localctx).value = expression(0);
				setState(214);
				match(RPAREN);
				}
				break;
			case 5:
				_localctx = new FailContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(216);
				match(FAIL);
				}
				break;
			case 6:
				_localctx = new SendContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(217);
				match(SEND);
				setState(218);
				((SendContext)_localctx).mon = expression(0);
				setState(219);
				match(LPAREN);
				setState(220);
				match(NEWLINE);
				setState(237);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SOURCE:
					{
					setState(221);
					match(SOURCE);
					setState(222);
					match(EQ);
					setState(223);
					((SendContext)_localctx).src = valueAwareSource();
					setState(224);
					match(NEWLINE);
					setState(225);
					match(DESTINATION);
					setState(226);
					match(EQ);
					setState(227);
					((SendContext)_localctx).dest = destination();
					}
					break;
				case DESTINATION:
					{
					setState(229);
					match(DESTINATION);
					setState(230);
					match(EQ);
					setState(231);
					((SendContext)_localctx).dest = destination();
					setState(232);
					match(NEWLINE);
					setState(233);
					match(SOURCE);
					setState(234);
					match(EQ);
					setState(235);
					((SendContext)_localctx).src = valueAwareSource();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(239);
				match(NEWLINE);
				setState(240);
				match(RPAREN);
				}
				break;
			case 7:
				_localctx = new SendAllContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(242);
				match(SEND);
				setState(243);
				((SendAllContext)_localctx).monAll = monetaryAll();
				setState(244);
				match(LPAREN);
				setState(245);
				match(NEWLINE);
				setState(262);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case SOURCE:
					{
					setState(246);
					match(SOURCE);
					setState(247);
					match(EQ);
					setState(248);
					((SendAllContext)_localctx).src = source();
					setState(249);
					match(NEWLINE);
					setState(250);
					match(DESTINATION);
					setState(251);
					match(EQ);
					setState(252);
					((SendAllContext)_localctx).dest = destination();
					}
					break;
				case DESTINATION:
					{
					setState(254);
					match(DESTINATION);
					setState(255);
					match(EQ);
					setState(256);
					((SendAllContext)_localctx).dest = destination();
					setState(257);
					match(NEWLINE);
					setState(258);
					match(SOURCE);
					setState(259);
					match(EQ);
					setState(260);
					((SendAllContext)_localctx).src = source();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(264);
				match(NEWLINE);
				setState(265);
				match(RPAREN);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Type_Context extends ParserRuleContext {
		public TerminalNode TY_ACCOUNT() { return getToken(NumScriptParser.TY_ACCOUNT, 0); }
		public TerminalNode TY_ASSET() { return getToken(NumScriptParser.TY_ASSET, 0); }
		public TerminalNode TY_NUMBER() { return getToken(NumScriptParser.TY_NUMBER, 0); }
		public TerminalNode TY_STRING() { return getToken(NumScriptParser.TY_STRING, 0); }
		public TerminalNode TY_MONETARY() { return getToken(NumScriptParser.TY_MONETARY, 0); }
		public TerminalNode TY_PORTION() { return getToken(NumScriptParser.TY_PORTION, 0); }
		public TerminalNode TY_BOOL() { return getToken(NumScriptParser.TY_BOOL, 0); }
		public Type_Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type_; }
	}

	public final Type_Context type_() throws RecognitionException {
		Type_Context _localctx = new Type_Context(_ctx, getState());
		enterRule(_localctx, 36, RULE_type_);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(269);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << TY_ACCOUNT) | (1L << TY_ASSET) | (1L << TY_NUMBER) | (1L << TY_MONETARY) | (1L << TY_PORTION) | (1L << TY_STRING) | (1L << TY_BOOL))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OriginContext extends ParserRuleContext {
		public OriginContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_origin; }
	 
		public OriginContext() { }
		public void copyFrom(OriginContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class OriginAccountBalanceContext extends OriginContext {
		public ExpressionContext account;
		public ExpressionContext asset;
		public TerminalNode BALANCE() { return getToken(NumScriptParser.BALANCE, 0); }
		public TerminalNode LPAREN() { return getToken(NumScriptParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(NumScriptParser.RPAREN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public OriginAccountBalanceContext(OriginContext ctx) { copyFrom(ctx); }
	}
	public static class OriginAccountMetaContext extends OriginContext {
		public ExpressionContext account;
		public Token key;
		public TerminalNode META() { return getToken(NumScriptParser.META, 0); }
		public TerminalNode LPAREN() { return getToken(NumScriptParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(NumScriptParser.RPAREN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode STRING() { return getToken(NumScriptParser.STRING, 0); }
		public OriginAccountMetaContext(OriginContext ctx) { copyFrom(ctx); }
	}

	public final OriginContext origin() throws RecognitionException {
		OriginContext _localctx = new OriginContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_origin);
		try {
			setState(285);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case META:
				_localctx = new OriginAccountMetaContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(271);
				match(META);
				setState(272);
				match(LPAREN);
				setState(273);
				((OriginAccountMetaContext)_localctx).account = expression(0);
				setState(274);
				match(T__5);
				setState(275);
				((OriginAccountMetaContext)_localctx).key = match(STRING);
				setState(276);
				match(RPAREN);
				}
				break;
			case BALANCE:
				_localctx = new OriginAccountBalanceContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(278);
				match(BALANCE);
				setState(279);
				match(LPAREN);
				setState(280);
				((OriginAccountBalanceContext)_localctx).account = expression(0);
				setState(281);
				match(T__5);
				setState(282);
				((OriginAccountBalanceContext)_localctx).asset = expression(0);
				setState(283);
				match(RPAREN);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class VarDeclContext extends ParserRuleContext {
		public Type_Context ty;
		public VariableContext name;
		public OriginContext orig;
		public Type_Context type_() {
			return getRuleContext(Type_Context.class,0);
		}
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public TerminalNode EQ() { return getToken(NumScriptParser.EQ, 0); }
		public OriginContext origin() {
			return getRuleContext(OriginContext.class,0);
		}
		public VarDeclContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_varDecl; }
	}

	public final VarDeclContext varDecl() throws RecognitionException {
		VarDeclContext _localctx = new VarDeclContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_varDecl);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(287);
			((VarDeclContext)_localctx).ty = type_();
			setState(288);
			((VarDeclContext)_localctx).name = variable();
			setState(291);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EQ) {
				{
				setState(289);
				match(EQ);
				setState(290);
				((VarDeclContext)_localctx).orig = origin();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class VarListDeclContext extends ParserRuleContext {
		public VarDeclContext varDecl;
		public List<VarDeclContext> v = new ArrayList<VarDeclContext>();
		public TerminalNode VARS() { return getToken(NumScriptParser.VARS, 0); }
		public TerminalNode LBRACE() { return getToken(NumScriptParser.LBRACE, 0); }
		public List<TerminalNode> NEWLINE() { return getTokens(NumScriptParser.NEWLINE); }
		public TerminalNode NEWLINE(int i) {
			return getToken(NumScriptParser.NEWLINE, i);
		}
		public TerminalNode RBRACE() { return getToken(NumScriptParser.RBRACE, 0); }
		public List<VarDeclContext> varDecl() {
			return getRuleContexts(VarDeclContext.class);
		}
		public VarDeclContext varDecl(int i) {
			return getRuleContext(VarDeclContext.class,i);
		}
		public VarListDeclContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_varListDecl; }
	}

	public final VarListDeclContext varListDecl() throws RecognitionException {
		VarListDeclContext _localctx = new VarListDeclContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_varListDecl);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(293);
			match(VARS);
			setState(294);
			match(LBRACE);
			setState(295);
			match(NEWLINE);
			setState(302); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(296);
				((VarListDeclContext)_localctx).varDecl = varDecl();
				((VarListDeclContext)_localctx).v.add(((VarListDeclContext)_localctx).varDecl);
				setState(298); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(297);
					match(NEWLINE);
					}
					}
					setState(300); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==NEWLINE );
				}
				}
				setState(304); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << TY_ACCOUNT) | (1L << TY_ASSET) | (1L << TY_NUMBER) | (1L << TY_MONETARY) | (1L << TY_PORTION) | (1L << TY_STRING) | (1L << TY_BOOL))) != 0) );
			setState(306);
			match(RBRACE);
			setState(307);
			match(NEWLINE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ScriptContext extends ParserRuleContext {
		public VarListDeclContext vars;
		public StatementContext statement;
		public List<StatementContext> stmts = new ArrayList<StatementContext>();
		public TerminalNode EOF() { return getToken(NumScriptParser.EOF, 0); }
		public List<StatementContext> statement() {
			return getRuleContexts(StatementContext.class);
		}
		public StatementContext statement(int i) {
			return getRuleContext(StatementContext.class,i);
		}
		public List<TerminalNode> NEWLINE() { return getTokens(NumScriptParser.NEWLINE); }
		public TerminalNode NEWLINE(int i) {
			return getToken(NumScriptParser.NEWLINE, i);
		}
		public VarListDeclContext varListDecl() {
			return getRuleContext(VarListDeclContext.class,0);
		}
		public ScriptContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_script; }
	}

	public final ScriptContext script() throws RecognitionException {
		ScriptContext _localctx = new ScriptContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_script);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(312);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==NEWLINE) {
				{
				{
				setState(309);
				match(NEWLINE);
				}
				}
				setState(314);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(316);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==VARS) {
				{
				setState(315);
				((ScriptContext)_localctx).vars = varListDecl();
				}
			}

			setState(318);
			((ScriptContext)_localctx).statement = statement();
			((ScriptContext)_localctx).stmts.add(((ScriptContext)_localctx).statement);
			setState(323);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,25,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(319);
					match(NEWLINE);
					setState(320);
					((ScriptContext)_localctx).statement = statement();
					((ScriptContext)_localctx).stmts.add(((ScriptContext)_localctx).statement);
					}
					} 
				}
				setState(325);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,25,_ctx);
			}
			setState(329);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==NEWLINE) {
				{
				{
				setState(326);
				match(NEWLINE);
				}
				}
				setState(331);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(332);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 4:
			return expression_sempred((ExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 10);
		case 1:
			return precpred(_ctx, 9);
		case 2:
			return precpred(_ctx, 7);
		case 3:
			return precpred(_ctx, 6);
		case 4:
			return precpred(_ctx, 2);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3=\u0151\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\3\2\3\2\3"+
		"\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\5\4@\n\4\3\5\3\5\3"+
		"\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6N\n\6\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\7\6b\n\6\f\6\16\6e"+
		"\13\6\3\7\3\7\3\7\5\7j\n\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\6\bs\n\b\r\b\16"+
		"\bt\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\6\t\u0082\n\t\r\t\16\t"+
		"\u0083\3\t\3\t\3\n\3\n\3\n\5\n\u008b\n\n\3\13\3\13\3\13\5\13\u0090\n\13"+
		"\3\f\3\f\3\f\5\f\u0095\n\f\3\r\3\r\5\r\u0099\n\r\3\16\3\16\3\16\3\16\3"+
		"\16\6\16\u00a0\n\16\r\16\16\16\u00a1\3\16\3\16\3\17\3\17\3\17\3\17\3\17"+
		"\3\20\3\20\3\20\5\20\u00ae\n\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21\6\21"+
		"\u00b7\n\21\r\21\16\21\u00b8\3\21\3\21\3\22\3\22\5\22\u00bf\n\22\3\23"+
		"\3\23\3\23\3\23\3\23\5\23\u00c6\n\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
		"\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
		"\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
		"\3\23\3\23\3\23\3\23\3\23\5\23\u00f0\n\23\3\23\3\23\3\23\3\23\3\23\3\23"+
		"\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
		"\3\23\3\23\3\23\5\23\u0109\n\23\3\23\3\23\3\23\5\23\u010e\n\23\3\24\3"+
		"\24\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3"+
		"\25\5\25\u0120\n\25\3\26\3\26\3\26\3\26\5\26\u0126\n\26\3\27\3\27\3\27"+
		"\3\27\3\27\6\27\u012d\n\27\r\27\16\27\u012e\6\27\u0131\n\27\r\27\16\27"+
		"\u0132\3\27\3\27\3\27\3\30\7\30\u0139\n\30\f\30\16\30\u013c\13\30\3\30"+
		"\5\30\u013f\n\30\3\30\3\30\3\30\7\30\u0144\n\30\f\30\16\30\u0147\13\30"+
		"\3\30\7\30\u014a\n\30\f\30\16\30\u014d\13\30\3\30\3\30\3\30\2\3\n\31\2"+
		"\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\2\5\3\2\32\33\3\2\34!\3"+
		"\2,\62\2\u0165\2\60\3\2\2\2\4\65\3\2\2\2\6?\3\2\2\2\bA\3\2\2\2\nM\3\2"+
		"\2\2\fi\3\2\2\2\16k\3\2\2\2\20{\3\2\2\2\22\u008a\3\2\2\2\24\u008f\3\2"+
		"\2\2\26\u0094\3\2\2\2\30\u0096\3\2\2\2\32\u009a\3\2\2\2\34\u00a5\3\2\2"+
		"\2\36\u00ad\3\2\2\2 \u00af\3\2\2\2\"\u00be\3\2\2\2$\u010d\3\2\2\2&\u010f"+
		"\3\2\2\2(\u011f\3\2\2\2*\u0121\3\2\2\2,\u0127\3\2\2\2.\u013a\3\2\2\2\60"+
		"\61\7\'\2\2\61\62\5\n\6\2\62\63\5\n\6\2\63\64\7(\2\2\64\3\3\2\2\2\65\66"+
		"\7\'\2\2\66\67\5\n\6\2\678\7\3\2\289\7(\2\29\5\3\2\2\2:@\7<\2\2;@\7=\2"+
		"\2<@\79\2\2=@\7\63\2\2>@\7\64\2\2?:\3\2\2\2?;\3\2\2\2?<\3\2\2\2?=\3\2"+
		"\2\2?>\3\2\2\2@\7\3\2\2\2AB\7;\2\2B\t\3\2\2\2CD\b\6\1\2DE\7\"\2\2EN\5"+
		"\n\6\nFN\5\6\4\2GN\5\b\5\2HN\5\2\2\2IJ\7%\2\2JK\5\n\6\2KL\7&\2\2LN\3\2"+
		"\2\2MC\3\2\2\2MF\3\2\2\2MG\3\2\2\2MH\3\2\2\2MI\3\2\2\2Nc\3\2\2\2OP\f\f"+
		"\2\2PQ\t\2\2\2Qb\5\n\6\rRS\f\13\2\2ST\t\3\2\2Tb\5\n\6\fUV\f\t\2\2VW\7"+
		"#\2\2Wb\5\n\6\nXY\f\b\2\2YZ\7$\2\2Zb\5\n\6\t[\\\f\4\2\2\\]\7\4\2\2]^\5"+
		"\n\6\2^_\7\5\2\2_`\5\n\6\5`b\3\2\2\2aO\3\2\2\2aR\3\2\2\2aU\3\2\2\2aX\3"+
		"\2\2\2a[\3\2\2\2be\3\2\2\2ca\3\2\2\2cd\3\2\2\2d\13\3\2\2\2ec\3\2\2\2f"+
		"j\7\64\2\2gj\5\b\5\2hj\7\65\2\2if\3\2\2\2ig\3\2\2\2ih\3\2\2\2j\r\3\2\2"+
		"\2kl\7)\2\2lr\7\t\2\2mn\7\26\2\2no\5\n\6\2op\5\22\n\2pq\7\t\2\2qs\3\2"+
		"\2\2rm\3\2\2\2st\3\2\2\2tr\3\2\2\2tu\3\2\2\2uv\3\2\2\2vw\7\65\2\2wx\5"+
		"\22\n\2xy\7\t\2\2yz\7*\2\2z\17\3\2\2\2{|\7)\2\2|\u0081\7\t\2\2}~\5\f\7"+
		"\2~\177\5\22\n\2\177\u0080\7\t\2\2\u0080\u0082\3\2\2\2\u0081}\3\2\2\2"+
		"\u0082\u0083\3\2\2\2\u0083\u0081\3\2\2\2\u0083\u0084\3\2\2\2\u0084\u0085"+
		"\3\2\2\2\u0085\u0086\7*\2\2\u0086\21\3\2\2\2\u0087\u0088\7\30\2\2\u0088"+
		"\u008b\5\24\13\2\u0089\u008b\7\66\2\2\u008a\u0087\3\2\2\2\u008a\u0089"+
		"\3\2\2\2\u008b\23\3\2\2\2\u008c\u0090\5\n\6\2\u008d\u0090\5\16\b\2\u008e"+
		"\u0090\5\20\t\2\u008f\u008c\3\2\2\2\u008f\u008d\3\2\2\2\u008f\u008e\3"+
		"\2\2\2\u0090\25\3\2\2\2\u0091\u0092\7\6\2\2\u0092\u0095\5\n\6\2\u0093"+
		"\u0095\7\7\2\2\u0094\u0091\3\2\2\2\u0094\u0093\3\2\2\2\u0095\27\3\2\2"+
		"\2\u0096\u0098\5\n\6\2\u0097\u0099\5\26\f\2\u0098\u0097\3\2\2\2\u0098"+
		"\u0099\3\2\2\2\u0099\31\3\2\2\2\u009a\u009b\7)\2\2\u009b\u009f\7\t\2\2"+
		"\u009c\u009d\5\36\20\2\u009d\u009e\7\t\2\2\u009e\u00a0\3\2\2\2\u009f\u009c"+
		"\3\2\2\2\u00a0\u00a1\3\2\2\2\u00a1\u009f\3\2\2\2\u00a1\u00a2\3\2\2\2\u00a2"+
		"\u00a3\3\2\2\2\u00a3\u00a4\7*\2\2\u00a4\33\3\2\2\2\u00a5\u00a6\7\26\2"+
		"\2\u00a6\u00a7\5\n\6\2\u00a7\u00a8\7\25\2\2\u00a8\u00a9\5\36\20\2\u00a9"+
		"\35\3\2\2\2\u00aa\u00ae\5\30\r\2\u00ab\u00ae\5\34\17\2\u00ac\u00ae\5\32"+
		"\16\2\u00ad\u00aa\3\2\2\2\u00ad\u00ab\3\2\2\2\u00ad\u00ac\3\2\2\2\u00ae"+
		"\37\3\2\2\2\u00af\u00b0\7)\2\2\u00b0\u00b6\7\t\2\2\u00b1\u00b2\5\f\7\2"+
		"\u00b2\u00b3\7\25\2\2\u00b3\u00b4\5\36\20\2\u00b4\u00b5\7\t\2\2\u00b5"+
		"\u00b7\3\2\2\2\u00b6\u00b1\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00b6\3\2"+
		"\2\2\u00b8\u00b9\3\2\2\2\u00b9\u00ba\3\2\2\2\u00ba\u00bb\7*\2\2\u00bb"+
		"!\3\2\2\2\u00bc\u00bf\5\36\20\2\u00bd\u00bf\5 \21\2\u00be\u00bc\3\2\2"+
		"\2\u00be\u00bd\3\2\2\2\u00bf#\3\2\2\2\u00c0\u00c1\7\21\2\2\u00c1\u010e"+
		"\5\n\6\2\u00c2\u00c5\78\2\2\u00c3\u00c6\5\n\6\2\u00c4\u00c6\5\4\3\2\u00c5"+
		"\u00c3\3\2\2\2\u00c5\u00c4\3\2\2\2\u00c6\u00c7\3\2\2\2\u00c7\u00c8\7\25"+
		"\2\2\u00c8\u00c9\5\n\6\2\u00c9\u010e\3\2\2\2\u00ca\u00cb\7\17\2\2\u00cb"+
		"\u00cc\7%\2\2\u00cc\u00cd\7\63\2\2\u00cd\u00ce\7\b\2\2\u00ce\u00cf\5\n"+
		"\6\2\u00cf\u00d0\7&\2\2\u00d0\u010e\3\2\2\2\u00d1\u00d2\7\20\2\2\u00d2"+
		"\u00d3\7%\2\2\u00d3\u00d4\5\n\6\2\u00d4\u00d5\7\b\2\2\u00d5\u00d6\7\63"+
		"\2\2\u00d6\u00d7\7\b\2\2\u00d7\u00d8\5\n\6\2\u00d8\u00d9\7&\2\2\u00d9"+
		"\u010e\3\2\2\2\u00da\u010e\7\22\2\2\u00db\u00dc\7\23\2\2\u00dc\u00dd\5"+
		"\n\6\2\u00dd\u00de\7%\2\2\u00de\u00ef\7\t\2\2\u00df\u00e0\7\24\2\2\u00e0"+
		"\u00e1\7+\2\2\u00e1\u00e2\5\"\22\2\u00e2\u00e3\7\t\2\2\u00e3\u00e4\7\27"+
		"\2\2\u00e4\u00e5\7+\2\2\u00e5\u00e6\5\24\13\2\u00e6\u00f0\3\2\2\2\u00e7"+
		"\u00e8\7\27\2\2\u00e8\u00e9\7+\2\2\u00e9\u00ea\5\24\13\2\u00ea\u00eb\7"+
		"\t\2\2\u00eb\u00ec\7\24\2\2\u00ec\u00ed\7+\2\2\u00ed\u00ee\5\"\22\2\u00ee"+
		"\u00f0\3\2\2\2\u00ef\u00df\3\2\2\2\u00ef\u00e7\3\2\2\2\u00f0\u00f1\3\2"+
		"\2\2\u00f1\u00f2\7\t\2\2\u00f2\u00f3\7&\2\2\u00f3\u010e\3\2\2\2\u00f4"+
		"\u00f5\7\23\2\2\u00f5\u00f6\5\4\3\2\u00f6\u00f7\7%\2\2\u00f7\u0108\7\t"+
		"\2\2\u00f8\u00f9\7\24\2\2\u00f9\u00fa\7+\2\2\u00fa\u00fb\5\36\20\2\u00fb"+
		"\u00fc\7\t\2\2\u00fc\u00fd\7\27\2\2\u00fd\u00fe\7+\2\2\u00fe\u00ff\5\24"+
		"\13\2\u00ff\u0109\3\2\2\2\u0100\u0101\7\27\2\2\u0101\u0102\7+\2\2\u0102"+
		"\u0103\5\24\13\2\u0103\u0104\7\t\2\2\u0104\u0105\7\24\2\2\u0105\u0106"+
		"\7+\2\2\u0106\u0107\5\36\20\2\u0107\u0109\3\2\2\2\u0108\u00f8\3\2\2\2"+
		"\u0108\u0100\3\2\2\2\u0109\u010a\3\2\2\2\u010a\u010b\7\t\2\2\u010b\u010c"+
		"\7&\2\2\u010c\u010e\3\2\2\2\u010d\u00c0\3\2\2\2\u010d\u00c2\3\2\2\2\u010d"+
		"\u00ca\3\2\2\2\u010d\u00d1\3\2\2\2\u010d\u00da\3\2\2\2\u010d\u00db\3\2"+
		"\2\2\u010d\u00f4\3\2\2\2\u010e%\3\2\2\2\u010f\u0110\t\4\2\2\u0110\'\3"+
		"\2\2\2\u0111\u0112\7\16\2\2\u0112\u0113\7%\2\2\u0113\u0114\5\n\6\2\u0114"+
		"\u0115\7\b\2\2\u0115\u0116\7\63\2\2\u0116\u0117\7&\2\2\u0117\u0120\3\2"+
		"\2\2\u0118\u0119\7\67\2\2\u0119\u011a\7%\2\2\u011a\u011b\5\n\6\2\u011b"+
		"\u011c\7\b\2\2\u011c\u011d\5\n\6\2\u011d\u011e\7&\2\2\u011e\u0120\3\2"+
		"\2\2\u011f\u0111\3\2\2\2\u011f\u0118\3\2\2\2\u0120)\3\2\2\2\u0121\u0122"+
		"\5&\24\2\u0122\u0125\5\b\5\2\u0123\u0124\7+\2\2\u0124\u0126\5(\25\2\u0125"+
		"\u0123\3\2\2\2\u0125\u0126\3\2\2\2\u0126+\3\2\2\2\u0127\u0128\7\r\2\2"+
		"\u0128\u0129\7)\2\2\u0129\u0130\7\t\2\2\u012a\u012c\5*\26\2\u012b\u012d"+
		"\7\t\2\2\u012c\u012b\3\2\2\2\u012d\u012e\3\2\2\2\u012e\u012c\3\2\2\2\u012e"+
		"\u012f\3\2\2\2\u012f\u0131\3\2\2\2\u0130\u012a\3\2\2\2\u0131\u0132\3\2"+
		"\2\2\u0132\u0130\3\2\2\2\u0132\u0133\3\2\2\2\u0133\u0134\3\2\2\2\u0134"+
		"\u0135\7*\2\2\u0135\u0136\7\t\2\2\u0136-\3\2\2\2\u0137\u0139\7\t\2\2\u0138"+
		"\u0137\3\2\2\2\u0139\u013c\3\2\2\2\u013a\u0138\3\2\2\2\u013a\u013b\3\2"+
		"\2\2\u013b\u013e\3\2\2\2\u013c\u013a\3\2\2\2\u013d\u013f\5,\27\2\u013e"+
		"\u013d\3\2\2\2\u013e\u013f\3\2\2\2\u013f\u0140\3\2\2\2\u0140\u0145\5$"+
		"\23\2\u0141\u0142\7\t\2\2\u0142\u0144\5$\23\2\u0143\u0141\3\2\2\2\u0144"+
		"\u0147\3\2\2\2\u0145\u0143\3\2\2\2\u0145\u0146\3\2\2\2\u0146\u014b\3\2"+
		"\2\2\u0147\u0145\3\2\2\2\u0148\u014a\7\t\2\2\u0149\u0148\3\2\2\2\u014a"+
		"\u014d\3\2\2\2\u014b\u0149\3\2\2\2\u014b\u014c\3\2\2\2\u014c\u014e\3\2"+
		"\2\2\u014d\u014b\3\2\2\2\u014e\u014f\7\2\2\3\u014f/\3\2\2\2\35?Macit\u0083"+
		"\u008a\u008f\u0094\u0098\u00a1\u00ad\u00b8\u00be\u00c5\u00ef\u0108\u010d"+
		"\u011f\u0125\u012e\u0132\u013a\u013e\u0145\u014b";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}