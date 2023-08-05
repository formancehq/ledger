// Generated from /home/phlimy/Projects/Contrib/stack/components/ledger/pkg/machine/script/NumScript.g4 by ANTLR 4.9.2
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class NumScriptLexer extends Lexer {
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
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "NEWLINE", "WHITESPACE", 
			"MULTILINE_COMMENT", "LINE_COMMENT", "VARS", "META", "SET_TX_META", "SET_ACCOUNT_META", 
			"PRINT", "FAIL", "SEND", "SOURCE", "FROM", "MAX", "DESTINATION", "TO", 
			"ALLOCATE", "OP_ADD", "OP_SUB", "OP_EQ", "OP_NEQ", "OP_LT", "OP_LTE", 
			"OP_GT", "OP_GTE", "OP_NOT", "OP_AND", "OP_OR", "LPAREN", "RPAREN", "LBRACK", 
			"RBRACK", "LBRACE", "RBRACE", "EQ", "TY_ACCOUNT", "TY_ASSET", "TY_NUMBER", 
			"TY_MONETARY", "TY_PORTION", "TY_STRING", "TY_BOOL", "STRING", "PORTION", 
			"REMAINING", "KEPT", "BALANCE", "SAVE", "NUMBER", "PERCENT", "VARIABLE_NAME", 
			"ACCOUNT", "ASSET"
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


	public NumScriptLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "NumScript.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2=\u0204\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\3\2\3"+
		"\2\3\3\3\3\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5"+
		"\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6"+
		"\3\6\3\6\3\6\3\6\3\6\3\6\3\7\3\7\3\b\6\b\u00b9\n\b\r\b\16\b\u00ba\3\t"+
		"\6\t\u00be\n\t\r\t\16\t\u00bf\3\t\3\t\3\n\3\n\3\n\3\n\3\n\7\n\u00c9\n"+
		"\n\f\n\16\n\u00cc\13\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\7\13\u00d7"+
		"\n\13\f\13\16\13\u00da\13\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3"+
		"\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3"+
		"\16\3\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3"+
		"\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3\21\3"+
		"\21\3\21\3\22\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3"+
		"\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26\3"+
		"\26\3\26\3\26\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3"+
		"\30\3\30\3\30\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3\33\3\34\3\34\3"+
		"\34\3\35\3\35\3\36\3\36\3\36\3\37\3\37\3 \3 \3 \3!\3!\3\"\3\"\3\"\3#\3"+
		"#\3#\3$\3$\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3*\3*\3+\3+\3+\3+\3+\3+\3+"+
		"\3+\3,\3,\3,\3,\3,\3,\3-\3-\3-\3-\3-\3-\3-\3.\3.\3.\3.\3.\3.\3.\3.\3."+
		"\3/\3/\3/\3/\3/\3/\3/\3/\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\61\3\61"+
		"\3\61\3\61\3\61\3\62\3\62\7\62\u019d\n\62\f\62\16\62\u01a0\13\62\3\62"+
		"\3\62\3\63\6\63\u01a5\n\63\r\63\16\63\u01a6\3\63\5\63\u01aa\n\63\3\63"+
		"\3\63\5\63\u01ae\n\63\3\63\6\63\u01b1\n\63\r\63\16\63\u01b2\3\63\6\63"+
		"\u01b6\n\63\r\63\16\63\u01b7\3\63\3\63\6\63\u01bc\n\63\r\63\16\63\u01bd"+
		"\5\63\u01c0\n\63\3\63\5\63\u01c3\n\63\3\64\3\64\3\64\3\64\3\64\3\64\3"+
		"\64\3\64\3\64\3\64\3\65\3\65\3\65\3\65\3\65\3\66\3\66\3\66\3\66\3\66\3"+
		"\66\3\66\3\66\3\67\3\67\3\67\3\67\3\67\38\68\u01e2\n8\r8\168\u01e3\39"+
		"\39\3:\3:\6:\u01ea\n:\r:\16:\u01eb\3:\7:\u01ef\n:\f:\16:\u01f2\13:\3;"+
		"\3;\6;\u01f6\n;\r;\16;\u01f7\3;\7;\u01fb\n;\f;\16;\u01fe\13;\3<\6<\u0201"+
		"\n<\r<\16<\u0202\4\u00ca\u00d8\2=\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23"+
		"\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31"+
		"\61\32\63\33\65\34\67\359\36;\37= ?!A\"C#E$G%I&K\'M(O)Q*S+U,W-Y.[/]\60"+
		"_\61a\62c\63e\64g\65i\66k\67m8o9q:s;u<w=\3\2\f\4\2\f\f\17\17\4\2\13\13"+
		"\"\"\b\2\"\"//\62;C\\aac|\3\2\62;\3\2\"\"\4\2aac|\5\2\62;aac|\5\2C\\a"+
		"ac|\6\2\62<C\\aac|\4\2\61;C\\\2\u0217\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2"+
		"\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2"+
		"\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3"+
		"\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3"+
		"\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65"+
		"\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3"+
		"\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2M\3\2\2"+
		"\2\2O\3\2\2\2\2Q\3\2\2\2\2S\3\2\2\2\2U\3\2\2\2\2W\3\2\2\2\2Y\3\2\2\2\2"+
		"[\3\2\2\2\2]\3\2\2\2\2_\3\2\2\2\2a\3\2\2\2\2c\3\2\2\2\2e\3\2\2\2\2g\3"+
		"\2\2\2\2i\3\2\2\2\2k\3\2\2\2\2m\3\2\2\2\2o\3\2\2\2\2q\3\2\2\2\2s\3\2\2"+
		"\2\2u\3\2\2\2\2w\3\2\2\2\3y\3\2\2\2\5{\3\2\2\2\7}\3\2\2\2\t\177\3\2\2"+
		"\2\13\u0098\3\2\2\2\r\u00b5\3\2\2\2\17\u00b8\3\2\2\2\21\u00bd\3\2\2\2"+
		"\23\u00c3\3\2\2\2\25\u00d2\3\2\2\2\27\u00df\3\2\2\2\31\u00e4\3\2\2\2\33"+
		"\u00e9\3\2\2\2\35\u00f5\3\2\2\2\37\u0106\3\2\2\2!\u010c\3\2\2\2#\u0111"+
		"\3\2\2\2%\u0116\3\2\2\2\'\u011d\3\2\2\2)\u0122\3\2\2\2+\u0126\3\2\2\2"+
		"-\u0132\3\2\2\2/\u0135\3\2\2\2\61\u013e\3\2\2\2\63\u0140\3\2\2\2\65\u0142"+
		"\3\2\2\2\67\u0145\3\2\2\29\u0148\3\2\2\2;\u014a\3\2\2\2=\u014d\3\2\2\2"+
		"?\u014f\3\2\2\2A\u0152\3\2\2\2C\u0154\3\2\2\2E\u0157\3\2\2\2G\u015a\3"+
		"\2\2\2I\u015c\3\2\2\2K\u015e\3\2\2\2M\u0160\3\2\2\2O\u0162\3\2\2\2Q\u0164"+
		"\3\2\2\2S\u0166\3\2\2\2U\u0168\3\2\2\2W\u0170\3\2\2\2Y\u0176\3\2\2\2["+
		"\u017d\3\2\2\2]\u0186\3\2\2\2_\u018e\3\2\2\2a\u0195\3\2\2\2c\u019a\3\2"+
		"\2\2e\u01c2\3\2\2\2g\u01c4\3\2\2\2i\u01ce\3\2\2\2k\u01d3\3\2\2\2m\u01db"+
		"\3\2\2\2o\u01e1\3\2\2\2q\u01e5\3\2\2\2s\u01e7\3\2\2\2u\u01f3\3\2\2\2w"+
		"\u0200\3\2\2\2yz\7,\2\2z\4\3\2\2\2{|\7A\2\2|\6\3\2\2\2}~\7<\2\2~\b\3\2"+
		"\2\2\177\u0080\7c\2\2\u0080\u0081\7n\2\2\u0081\u0082\7n\2\2\u0082\u0083"+
		"\7q\2\2\u0083\u0084\7y\2\2\u0084\u0085\7k\2\2\u0085\u0086\7p\2\2\u0086"+
		"\u0087\7i\2\2\u0087\u0088\7\"\2\2\u0088\u0089\7q\2\2\u0089\u008a\7x\2"+
		"\2\u008a\u008b\7g\2\2\u008b\u008c\7t\2\2\u008c\u008d\7f\2\2\u008d\u008e"+
		"\7t\2\2\u008e\u008f\7c\2\2\u008f\u0090\7h\2\2\u0090\u0091\7v\2\2\u0091"+
		"\u0092\7\"\2\2\u0092\u0093\7w\2\2\u0093\u0094\7r\2\2\u0094\u0095\7\"\2"+
		"\2\u0095\u0096\7v\2\2\u0096\u0097\7q\2\2\u0097\n\3\2\2\2\u0098\u0099\7"+
		"c\2\2\u0099\u009a\7n\2\2\u009a\u009b\7n\2\2\u009b\u009c\7q\2\2\u009c\u009d"+
		"\7y\2\2\u009d\u009e\7k\2\2\u009e\u009f\7p\2\2\u009f\u00a0\7i\2\2\u00a0"+
		"\u00a1\7\"\2\2\u00a1\u00a2\7w\2\2\u00a2\u00a3\7p\2\2\u00a3\u00a4\7d\2"+
		"\2\u00a4\u00a5\7q\2\2\u00a5\u00a6\7w\2\2\u00a6\u00a7\7p\2\2\u00a7\u00a8"+
		"\7f\2\2\u00a8\u00a9\7g\2\2\u00a9\u00aa\7f\2\2\u00aa\u00ab\7\"\2\2\u00ab"+
		"\u00ac\7q\2\2\u00ac\u00ad\7x\2\2\u00ad\u00ae\7g\2\2\u00ae\u00af\7t\2\2"+
		"\u00af\u00b0\7f\2\2\u00b0\u00b1\7t\2\2\u00b1\u00b2\7c\2\2\u00b2\u00b3"+
		"\7h\2\2\u00b3\u00b4\7v\2\2\u00b4\f\3\2\2\2\u00b5\u00b6\7.\2\2\u00b6\16"+
		"\3\2\2\2\u00b7\u00b9\t\2\2\2\u00b8\u00b7\3\2\2\2\u00b9\u00ba\3\2\2\2\u00ba"+
		"\u00b8\3\2\2\2\u00ba\u00bb\3\2\2\2\u00bb\20\3\2\2\2\u00bc\u00be\t\3\2"+
		"\2\u00bd\u00bc\3\2\2\2\u00be\u00bf\3\2\2\2\u00bf\u00bd\3\2\2\2\u00bf\u00c0"+
		"\3\2\2\2\u00c0\u00c1\3\2\2\2\u00c1\u00c2\b\t\2\2\u00c2\22\3\2\2\2\u00c3"+
		"\u00c4\7\61\2\2\u00c4\u00c5\7,\2\2\u00c5\u00ca\3\2\2\2\u00c6\u00c9\5\23"+
		"\n\2\u00c7\u00c9\13\2\2\2\u00c8\u00c6\3\2\2\2\u00c8\u00c7\3\2\2\2\u00c9"+
		"\u00cc\3\2\2\2\u00ca\u00cb\3\2\2\2\u00ca\u00c8\3\2\2\2\u00cb\u00cd\3\2"+
		"\2\2\u00cc\u00ca\3\2\2\2\u00cd\u00ce\7,\2\2\u00ce\u00cf\7\61\2\2\u00cf"+
		"\u00d0\3\2\2\2\u00d0\u00d1\b\n\2\2\u00d1\24\3\2\2\2\u00d2\u00d3\7\61\2"+
		"\2\u00d3\u00d4\7\61\2\2\u00d4\u00d8\3\2\2\2\u00d5\u00d7\13\2\2\2\u00d6"+
		"\u00d5\3\2\2\2\u00d7\u00da\3\2\2\2\u00d8\u00d9\3\2\2\2\u00d8\u00d6\3\2"+
		"\2\2\u00d9\u00db\3\2\2\2\u00da\u00d8\3\2\2\2\u00db\u00dc\5\17\b\2\u00dc"+
		"\u00dd\3\2\2\2\u00dd\u00de\b\13\2\2\u00de\26\3\2\2\2\u00df\u00e0\7x\2"+
		"\2\u00e0\u00e1\7c\2\2\u00e1\u00e2\7t\2\2\u00e2\u00e3\7u\2\2\u00e3\30\3"+
		"\2\2\2\u00e4\u00e5\7o\2\2\u00e5\u00e6\7g\2\2\u00e6\u00e7\7v\2\2\u00e7"+
		"\u00e8\7c\2\2\u00e8\32\3\2\2\2\u00e9\u00ea\7u\2\2\u00ea\u00eb\7g\2\2\u00eb"+
		"\u00ec\7v\2\2\u00ec\u00ed\7a\2\2\u00ed\u00ee\7v\2\2\u00ee\u00ef\7z\2\2"+
		"\u00ef\u00f0\7a\2\2\u00f0\u00f1\7o\2\2\u00f1\u00f2\7g\2\2\u00f2\u00f3"+
		"\7v\2\2\u00f3\u00f4\7c\2\2\u00f4\34\3\2\2\2\u00f5\u00f6\7u\2\2\u00f6\u00f7"+
		"\7g\2\2\u00f7\u00f8\7v\2\2\u00f8\u00f9\7a\2\2\u00f9\u00fa\7c\2\2\u00fa"+
		"\u00fb\7e\2\2\u00fb\u00fc\7e\2\2\u00fc\u00fd\7q\2\2\u00fd\u00fe\7w\2\2"+
		"\u00fe\u00ff\7p\2\2\u00ff\u0100\7v\2\2\u0100\u0101\7a\2\2\u0101\u0102"+
		"\7o\2\2\u0102\u0103\7g\2\2\u0103\u0104\7v\2\2\u0104\u0105\7c\2\2\u0105"+
		"\36\3\2\2\2\u0106\u0107\7r\2\2\u0107\u0108\7t\2\2\u0108\u0109\7k\2\2\u0109"+
		"\u010a\7p\2\2\u010a\u010b\7v\2\2\u010b \3\2\2\2\u010c\u010d\7h\2\2\u010d"+
		"\u010e\7c\2\2\u010e\u010f\7k\2\2\u010f\u0110\7n\2\2\u0110\"\3\2\2\2\u0111"+
		"\u0112\7u\2\2\u0112\u0113\7g\2\2\u0113\u0114\7p\2\2\u0114\u0115\7f\2\2"+
		"\u0115$\3\2\2\2\u0116\u0117\7u\2\2\u0117\u0118\7q\2\2\u0118\u0119\7w\2"+
		"\2\u0119\u011a\7t\2\2\u011a\u011b\7e\2\2\u011b\u011c\7g\2\2\u011c&\3\2"+
		"\2\2\u011d\u011e\7h\2\2\u011e\u011f\7t\2\2\u011f\u0120\7q\2\2\u0120\u0121"+
		"\7o\2\2\u0121(\3\2\2\2\u0122\u0123\7o\2\2\u0123\u0124\7c\2\2\u0124\u0125"+
		"\7z\2\2\u0125*\3\2\2\2\u0126\u0127\7f\2\2\u0127\u0128\7g\2\2\u0128\u0129"+
		"\7u\2\2\u0129\u012a\7v\2\2\u012a\u012b\7k\2\2\u012b\u012c\7p\2\2\u012c"+
		"\u012d\7c\2\2\u012d\u012e\7v\2\2\u012e\u012f\7k\2\2\u012f\u0130\7q\2\2"+
		"\u0130\u0131\7p\2\2\u0131,\3\2\2\2\u0132\u0133\7v\2\2\u0133\u0134\7q\2"+
		"\2\u0134.\3\2\2\2\u0135\u0136\7c\2\2\u0136\u0137\7n\2\2\u0137\u0138\7"+
		"n\2\2\u0138\u0139\7q\2\2\u0139\u013a\7e\2\2\u013a\u013b\7c\2\2\u013b\u013c"+
		"\7v\2\2\u013c\u013d\7g\2\2\u013d\60\3\2\2\2\u013e\u013f\7-\2\2\u013f\62"+
		"\3\2\2\2\u0140\u0141\7/\2\2\u0141\64\3\2\2\2\u0142\u0143\7?\2\2\u0143"+
		"\u0144\7?\2\2\u0144\66\3\2\2\2\u0145\u0146\7#\2\2\u0146\u0147\7?\2\2\u0147"+
		"8\3\2\2\2\u0148\u0149\7>\2\2\u0149:\3\2\2\2\u014a\u014b\7>\2\2\u014b\u014c"+
		"\7?\2\2\u014c<\3\2\2\2\u014d\u014e\7@\2\2\u014e>\3\2\2\2\u014f\u0150\7"+
		"@\2\2\u0150\u0151\7?\2\2\u0151@\3\2\2\2\u0152\u0153\7#\2\2\u0153B\3\2"+
		"\2\2\u0154\u0155\7(\2\2\u0155\u0156\7(\2\2\u0156D\3\2\2\2\u0157\u0158"+
		"\7~\2\2\u0158\u0159\7~\2\2\u0159F\3\2\2\2\u015a\u015b\7*\2\2\u015bH\3"+
		"\2\2\2\u015c\u015d\7+\2\2\u015dJ\3\2\2\2\u015e\u015f\7]\2\2\u015fL\3\2"+
		"\2\2\u0160\u0161\7_\2\2\u0161N\3\2\2\2\u0162\u0163\7}\2\2\u0163P\3\2\2"+
		"\2\u0164\u0165\7\177\2\2\u0165R\3\2\2\2\u0166\u0167\7?\2\2\u0167T\3\2"+
		"\2\2\u0168\u0169\7c\2\2\u0169\u016a\7e\2\2\u016a\u016b\7e\2\2\u016b\u016c"+
		"\7q\2\2\u016c\u016d\7w\2\2\u016d\u016e\7p\2\2\u016e\u016f\7v\2\2\u016f"+
		"V\3\2\2\2\u0170\u0171\7c\2\2\u0171\u0172\7u\2\2\u0172\u0173\7u\2\2\u0173"+
		"\u0174\7g\2\2\u0174\u0175\7v\2\2\u0175X\3\2\2\2\u0176\u0177\7p\2\2\u0177"+
		"\u0178\7w\2\2\u0178\u0179\7o\2\2\u0179\u017a\7d\2\2\u017a\u017b\7g\2\2"+
		"\u017b\u017c\7t\2\2\u017cZ\3\2\2\2\u017d\u017e\7o\2\2\u017e\u017f\7q\2"+
		"\2\u017f\u0180\7p\2\2\u0180\u0181\7g\2\2\u0181\u0182\7v\2\2\u0182\u0183"+
		"\7c\2\2\u0183\u0184\7t\2\2\u0184\u0185\7{\2\2\u0185\\\3\2\2\2\u0186\u0187"+
		"\7r\2\2\u0187\u0188\7q\2\2\u0188\u0189\7t\2\2\u0189\u018a\7v\2\2\u018a"+
		"\u018b\7k\2\2\u018b\u018c\7q\2\2\u018c\u018d\7p\2\2\u018d^\3\2\2\2\u018e"+
		"\u018f\7u\2\2\u018f\u0190\7v\2\2\u0190\u0191\7t\2\2\u0191\u0192\7k\2\2"+
		"\u0192\u0193\7p\2\2\u0193\u0194\7i\2\2\u0194`\3\2\2\2\u0195\u0196\7d\2"+
		"\2\u0196\u0197\7q\2\2\u0197\u0198\7q\2\2\u0198\u0199\7n\2\2\u0199b\3\2"+
		"\2\2\u019a\u019e\7$\2\2\u019b\u019d\t\4\2\2\u019c\u019b\3\2\2\2\u019d"+
		"\u01a0\3\2\2\2\u019e\u019c\3\2\2\2\u019e\u019f\3\2\2\2\u019f\u01a1\3\2"+
		"\2\2\u01a0\u019e\3\2\2\2\u01a1\u01a2\7$\2\2\u01a2d\3\2\2\2\u01a3\u01a5"+
		"\t\5\2\2\u01a4\u01a3\3\2\2\2\u01a5\u01a6\3\2\2\2\u01a6\u01a4\3\2\2\2\u01a6"+
		"\u01a7\3\2\2\2\u01a7\u01a9\3\2\2\2\u01a8\u01aa\t\6\2\2\u01a9\u01a8\3\2"+
		"\2\2\u01a9\u01aa\3\2\2\2\u01aa\u01ab\3\2\2\2\u01ab\u01ad\7\61\2\2\u01ac"+
		"\u01ae\t\6\2\2\u01ad\u01ac\3\2\2\2\u01ad\u01ae\3\2\2\2\u01ae\u01b0\3\2"+
		"\2\2\u01af\u01b1\t\5\2\2\u01b0\u01af\3\2\2\2\u01b1\u01b2\3\2\2\2\u01b2"+
		"\u01b0\3\2\2\2\u01b2\u01b3\3\2\2\2\u01b3\u01c3\3\2\2\2\u01b4\u01b6\t\5"+
		"\2\2\u01b5\u01b4\3\2\2\2\u01b6\u01b7\3\2\2\2\u01b7\u01b5\3\2\2\2\u01b7"+
		"\u01b8\3\2\2\2\u01b8\u01bf\3\2\2\2\u01b9\u01bb\7\60\2\2\u01ba\u01bc\t"+
		"\5\2\2\u01bb\u01ba\3\2\2\2\u01bc\u01bd\3\2\2\2\u01bd\u01bb\3\2\2\2\u01bd"+
		"\u01be\3\2\2\2\u01be\u01c0\3\2\2\2\u01bf\u01b9\3\2\2\2\u01bf\u01c0\3\2"+
		"\2\2\u01c0\u01c1\3\2\2\2\u01c1\u01c3\7\'\2\2\u01c2\u01a4\3\2\2\2\u01c2"+
		"\u01b5\3\2\2\2\u01c3f\3\2\2\2\u01c4\u01c5\7t\2\2\u01c5\u01c6\7g\2\2\u01c6"+
		"\u01c7\7o\2\2\u01c7\u01c8\7c\2\2\u01c8\u01c9\7k\2\2\u01c9\u01ca\7p\2\2"+
		"\u01ca\u01cb\7k\2\2\u01cb\u01cc\7p\2\2\u01cc\u01cd\7i\2\2\u01cdh\3\2\2"+
		"\2\u01ce\u01cf\7m\2\2\u01cf\u01d0\7g\2\2\u01d0\u01d1\7r\2\2\u01d1\u01d2"+
		"\7v\2\2\u01d2j\3\2\2\2\u01d3\u01d4\7d\2\2\u01d4\u01d5\7c\2\2\u01d5\u01d6"+
		"\7n\2\2\u01d6\u01d7\7c\2\2\u01d7\u01d8\7p\2\2\u01d8\u01d9\7e\2\2\u01d9"+
		"\u01da\7g\2\2\u01dal\3\2\2\2\u01db\u01dc\7u\2\2\u01dc\u01dd\7c\2\2\u01dd"+
		"\u01de\7x\2\2\u01de\u01df\7g\2\2\u01dfn\3\2\2\2\u01e0\u01e2\t\5\2\2\u01e1"+
		"\u01e0\3\2\2\2\u01e2\u01e3\3\2\2\2\u01e3\u01e1\3\2\2\2\u01e3\u01e4\3\2"+
		"\2\2\u01e4p\3\2\2\2\u01e5\u01e6\7\'\2\2\u01e6r\3\2\2\2\u01e7\u01e9\7&"+
		"\2\2\u01e8\u01ea\t\7\2\2\u01e9\u01e8\3\2\2\2\u01ea\u01eb\3\2\2\2\u01eb"+
		"\u01e9\3\2\2\2\u01eb\u01ec\3\2\2\2\u01ec\u01f0\3\2\2\2\u01ed\u01ef\t\b"+
		"\2\2\u01ee\u01ed\3\2\2\2\u01ef\u01f2\3\2\2\2\u01f0\u01ee\3\2\2\2\u01f0"+
		"\u01f1\3\2\2\2\u01f1t\3\2\2\2\u01f2\u01f0\3\2\2\2\u01f3\u01f5\7B\2\2\u01f4"+
		"\u01f6\t\t\2\2\u01f5\u01f4\3\2\2\2\u01f6\u01f7\3\2\2\2\u01f7\u01f5\3\2"+
		"\2\2\u01f7\u01f8\3\2\2\2\u01f8\u01fc\3\2\2\2\u01f9\u01fb\t\n\2\2\u01fa"+
		"\u01f9\3\2\2\2\u01fb\u01fe\3\2\2\2\u01fc\u01fa\3\2\2\2\u01fc\u01fd\3\2"+
		"\2\2\u01fdv\3\2\2\2\u01fe\u01fc\3\2\2\2\u01ff\u0201\t\13\2\2\u0200\u01ff"+
		"\3\2\2\2\u0201\u0202\3\2\2\2\u0202\u0200\3\2\2\2\u0202\u0203\3\2\2\2\u0203"+
		"x\3\2\2\2\27\2\u00ba\u00bf\u00c8\u00ca\u00d8\u019e\u01a6\u01a9\u01ad\u01b2"+
		"\u01b7\u01bd\u01bf\u01c2\u01e3\u01eb\u01f0\u01f7\u01fc\u0202\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}