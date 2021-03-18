// Generated from /home/andr3a/workspace/delta/src/main/antlr4/io/delta/sql/parser/DeltaSqlBase.g4 by ANTLR 4.7
package io.delta.sql.parser;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class DeltaSqlBaseLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.7", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, BY=5, COMMENT=6, CONVERT=7, DELTA=8, DESC=9, 
		DESCRIBE=10, DETAIL=11, GENERATE=12, DRY=13, HISTORY=14, HOURS=15, LIMIT=16, 
		MINUS=17, NOT=18, NULL=19, FOR=20, TABLE=21, PARTITIONED=22, RETAIN=23, 
		RUN=24, TO=25, VACUUM=26, STRING=27, BIGINT_LITERAL=28, SMALLINT_LITERAL=29, 
		TINYINT_LITERAL=30, INTEGER_VALUE=31, DECIMAL_VALUE=32, DOUBLE_LITERAL=33, 
		BIGDECIMAL_LITERAL=34, IDENTIFIER=35, BACKQUOTED_IDENTIFIER=36, SIMPLE_COMMENT=37, 
		BRACKETED_COMMENT=38, WS=39, UNRECOGNIZED=40;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "BY", "COMMENT", "CONVERT", "DELTA", "DESC", 
		"DESCRIBE", "DETAIL", "GENERATE", "DRY", "HISTORY", "HOURS", "LIMIT", 
		"MINUS", "NOT", "NULL", "FOR", "TABLE", "PARTITIONED", "RETAIN", "RUN", 
		"TO", "VACUUM", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", 
		"INTEGER_VALUE", "DECIMAL_VALUE", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", 
		"IDENTIFIER", "BACKQUOTED_IDENTIFIER", "DECIMAL_DIGITS", "EXPONENT", "DIGIT", 
		"LETTER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "'.'", "','", "'BY'", "'COMMENT'", "'CONVERT'", "'DELTA'", 
		"'DESC'", "'DESCRIBE'", "'DETAIL'", "'GENERATE'", "'DRY'", "'HISTORY'", 
		"'HOURS'", "'LIMIT'", "'-'", null, "'NULL'", "'FOR'", "'TABLE'", "'PARTITIONED'", 
		"'RETAIN'", "'RUN'", "'TO'", "'VACUUM'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, "BY", "COMMENT", "CONVERT", "DELTA", "DESC", 
		"DESCRIBE", "DETAIL", "GENERATE", "DRY", "HISTORY", "HOURS", "LIMIT", 
		"MINUS", "NOT", "NULL", "FOR", "TABLE", "PARTITIONED", "RETAIN", "RUN", 
		"TO", "VACUUM", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", 
		"INTEGER_VALUE", "DECIMAL_VALUE", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", 
		"IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", 
		"WS", "UNRECOGNIZED"
	};
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


	  /**
	   * Verify whether current token is a valid decimal token (which contains dot).
	   * Returns true if the character that follows the token is not a digit or letter or underscore.
	   *
	   * For example:
	   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
	   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
	   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
	   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is folllowed
	   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
	   * which is not a digit or letter or underscore.
	   */
	  public boolean isValidDecimal() {
	    int nextChar = _input.LA(1);
	    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
	      nextChar == '_') {
	      return false;
	    } else {
	      return true;
	    }
	  }


	public DeltaSqlBaseLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "DeltaSqlBase.g4"; }

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

	@Override
	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 31:
			return DECIMAL_VALUE_sempred((RuleContext)_localctx, predIndex);
		case 32:
			return DOUBLE_LITERAL_sempred((RuleContext)_localctx, predIndex);
		case 33:
			return BIGDECIMAL_LITERAL_sempred((RuleContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean DECIMAL_VALUE_sempred(RuleContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return isValidDecimal();
		}
		return true;
	}
	private boolean DOUBLE_LITERAL_sempred(RuleContext _localctx, int predIndex) {
		switch (predIndex) {
		case 1:
			return isValidDecimal();
		}
		return true;
	}
	private boolean BIGDECIMAL_LITERAL_sempred(RuleContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return isValidDecimal();
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2*\u01ad\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\7"+
		"\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3"+
		"\f\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\16\3"+
		"\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3"+
		"\20\3\20\3\20\3\21\3\21\3\21\3\21\3\21\3\21\3\22\3\22\3\23\3\23\3\23\3"+
		"\23\5\23\u00b9\n\23\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\26"+
		"\3\26\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27"+
		"\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\31"+
		"\3\32\3\32\3\32\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\34"+
		"\7\34\u00ef\n\34\f\34\16\34\u00f2\13\34\3\34\3\34\3\34\3\34\3\34\7\34"+
		"\u00f9\n\34\f\34\16\34\u00fc\13\34\3\34\5\34\u00ff\n\34\3\35\6\35\u0102"+
		"\n\35\r\35\16\35\u0103\3\35\3\35\3\36\6\36\u0109\n\36\r\36\16\36\u010a"+
		"\3\36\3\36\3\37\6\37\u0110\n\37\r\37\16\37\u0111\3\37\3\37\3 \6 \u0117"+
		"\n \r \16 \u0118\3!\6!\u011c\n!\r!\16!\u011d\3!\3!\3!\3!\5!\u0124\n!\3"+
		"!\3!\5!\u0128\n!\3\"\6\"\u012b\n\"\r\"\16\"\u012c\3\"\5\"\u0130\n\"\3"+
		"\"\3\"\3\"\3\"\5\"\u0136\n\"\3\"\3\"\3\"\5\"\u013b\n\"\3#\6#\u013e\n#"+
		"\r#\16#\u013f\3#\5#\u0143\n#\3#\3#\3#\3#\3#\5#\u014a\n#\3#\3#\3#\3#\3"+
		"#\5#\u0151\n#\3$\3$\3$\6$\u0156\n$\r$\16$\u0157\3%\3%\3%\3%\7%\u015e\n"+
		"%\f%\16%\u0161\13%\3%\3%\3&\6&\u0166\n&\r&\16&\u0167\3&\3&\7&\u016c\n"+
		"&\f&\16&\u016f\13&\3&\3&\6&\u0173\n&\r&\16&\u0174\5&\u0177\n&\3\'\3\'"+
		"\5\'\u017b\n\'\3\'\6\'\u017e\n\'\r\'\16\'\u017f\3(\3(\3)\3)\3*\3*\3*\3"+
		"*\7*\u018a\n*\f*\16*\u018d\13*\3*\5*\u0190\n*\3*\5*\u0193\n*\3*\3*\3+"+
		"\3+\3+\3+\7+\u019b\n+\f+\16+\u019e\13+\3+\3+\3+\3+\3+\3,\6,\u01a6\n,\r"+
		",\16,\u01a7\3,\3,\3-\3-\3\u019c\2.\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n"+
		"\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30"+
		"/\31\61\32\63\33\65\34\67\359\36;\37= ?!A\"C#E$G%I&K\2M\2O\2Q\2S\'U(W"+
		")Y*\3\2\n\4\2))^^\4\2$$^^\3\2bb\4\2--//\3\2\62;\3\2C\\\4\2\f\f\17\17\5"+
		"\2\13\f\17\17\"\"\2\u01cd\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2"+
		"\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2"+
		"\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3"+
		"\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2"+
		"\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67"+
		"\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2"+
		"\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2S\3\2\2\2\2U\3\2\2\2\2W\3\2\2\2"+
		"\2Y\3\2\2\2\3[\3\2\2\2\5]\3\2\2\2\7_\3\2\2\2\ta\3\2\2\2\13c\3\2\2\2\r"+
		"f\3\2\2\2\17n\3\2\2\2\21v\3\2\2\2\23|\3\2\2\2\25\u0081\3\2\2\2\27\u008a"+
		"\3\2\2\2\31\u0091\3\2\2\2\33\u009a\3\2\2\2\35\u009e\3\2\2\2\37\u00a6\3"+
		"\2\2\2!\u00ac\3\2\2\2#\u00b2\3\2\2\2%\u00b8\3\2\2\2\'\u00ba\3\2\2\2)\u00bf"+
		"\3\2\2\2+\u00c3\3\2\2\2-\u00c9\3\2\2\2/\u00d5\3\2\2\2\61\u00dc\3\2\2\2"+
		"\63\u00e0\3\2\2\2\65\u00e3\3\2\2\2\67\u00fe\3\2\2\29\u0101\3\2\2\2;\u0108"+
		"\3\2\2\2=\u010f\3\2\2\2?\u0116\3\2\2\2A\u0127\3\2\2\2C\u013a\3\2\2\2E"+
		"\u0150\3\2\2\2G\u0155\3\2\2\2I\u0159\3\2\2\2K\u0176\3\2\2\2M\u0178\3\2"+
		"\2\2O\u0181\3\2\2\2Q\u0183\3\2\2\2S\u0185\3\2\2\2U\u0196\3\2\2\2W\u01a5"+
		"\3\2\2\2Y\u01ab\3\2\2\2[\\\7*\2\2\\\4\3\2\2\2]^\7+\2\2^\6\3\2\2\2_`\7"+
		"\60\2\2`\b\3\2\2\2ab\7.\2\2b\n\3\2\2\2cd\7D\2\2de\7[\2\2e\f\3\2\2\2fg"+
		"\7E\2\2gh\7Q\2\2hi\7O\2\2ij\7O\2\2jk\7G\2\2kl\7P\2\2lm\7V\2\2m\16\3\2"+
		"\2\2no\7E\2\2op\7Q\2\2pq\7P\2\2qr\7X\2\2rs\7G\2\2st\7T\2\2tu\7V\2\2u\20"+
		"\3\2\2\2vw\7F\2\2wx\7G\2\2xy\7N\2\2yz\7V\2\2z{\7C\2\2{\22\3\2\2\2|}\7"+
		"F\2\2}~\7G\2\2~\177\7U\2\2\177\u0080\7E\2\2\u0080\24\3\2\2\2\u0081\u0082"+
		"\7F\2\2\u0082\u0083\7G\2\2\u0083\u0084\7U\2\2\u0084\u0085\7E\2\2\u0085"+
		"\u0086\7T\2\2\u0086\u0087\7K\2\2\u0087\u0088\7D\2\2\u0088\u0089\7G\2\2"+
		"\u0089\26\3\2\2\2\u008a\u008b\7F\2\2\u008b\u008c\7G\2\2\u008c\u008d\7"+
		"V\2\2\u008d\u008e\7C\2\2\u008e\u008f\7K\2\2\u008f\u0090\7N\2\2\u0090\30"+
		"\3\2\2\2\u0091\u0092\7I\2\2\u0092\u0093\7G\2\2\u0093\u0094\7P\2\2\u0094"+
		"\u0095\7G\2\2\u0095\u0096\7T\2\2\u0096\u0097\7C\2\2\u0097\u0098\7V\2\2"+
		"\u0098\u0099\7G\2\2\u0099\32\3\2\2\2\u009a\u009b\7F\2\2\u009b\u009c\7"+
		"T\2\2\u009c\u009d\7[\2\2\u009d\34\3\2\2\2\u009e\u009f\7J\2\2\u009f\u00a0"+
		"\7K\2\2\u00a0\u00a1\7U\2\2\u00a1\u00a2\7V\2\2\u00a2\u00a3\7Q\2\2\u00a3"+
		"\u00a4\7T\2\2\u00a4\u00a5\7[\2\2\u00a5\36\3\2\2\2\u00a6\u00a7\7J\2\2\u00a7"+
		"\u00a8\7Q\2\2\u00a8\u00a9\7W\2\2\u00a9\u00aa\7T\2\2\u00aa\u00ab\7U\2\2"+
		"\u00ab \3\2\2\2\u00ac\u00ad\7N\2\2\u00ad\u00ae\7K\2\2\u00ae\u00af\7O\2"+
		"\2\u00af\u00b0\7K\2\2\u00b0\u00b1\7V\2\2\u00b1\"\3\2\2\2\u00b2\u00b3\7"+
		"/\2\2\u00b3$\3\2\2\2\u00b4\u00b5\7P\2\2\u00b5\u00b6\7Q\2\2\u00b6\u00b9"+
		"\7V\2\2\u00b7\u00b9\7#\2\2\u00b8\u00b4\3\2\2\2\u00b8\u00b7\3\2\2\2\u00b9"+
		"&\3\2\2\2\u00ba\u00bb\7P\2\2\u00bb\u00bc\7W\2\2\u00bc\u00bd\7N\2\2\u00bd"+
		"\u00be\7N\2\2\u00be(\3\2\2\2\u00bf\u00c0\7H\2\2\u00c0\u00c1\7Q\2\2\u00c1"+
		"\u00c2\7T\2\2\u00c2*\3\2\2\2\u00c3\u00c4\7V\2\2\u00c4\u00c5\7C\2\2\u00c5"+
		"\u00c6\7D\2\2\u00c6\u00c7\7N\2\2\u00c7\u00c8\7G\2\2\u00c8,\3\2\2\2\u00c9"+
		"\u00ca\7R\2\2\u00ca\u00cb\7C\2\2\u00cb\u00cc\7T\2\2\u00cc\u00cd\7V\2\2"+
		"\u00cd\u00ce\7K\2\2\u00ce\u00cf\7V\2\2\u00cf\u00d0\7K\2\2\u00d0\u00d1"+
		"\7Q\2\2\u00d1\u00d2\7P\2\2\u00d2\u00d3\7G\2\2\u00d3\u00d4\7F\2\2\u00d4"+
		".\3\2\2\2\u00d5\u00d6\7T\2\2\u00d6\u00d7\7G\2\2\u00d7\u00d8\7V\2\2\u00d8"+
		"\u00d9\7C\2\2\u00d9\u00da\7K\2\2\u00da\u00db\7P\2\2\u00db\60\3\2\2\2\u00dc"+
		"\u00dd\7T\2\2\u00dd\u00de\7W\2\2\u00de\u00df\7P\2\2\u00df\62\3\2\2\2\u00e0"+
		"\u00e1\7V\2\2\u00e1\u00e2\7Q\2\2\u00e2\64\3\2\2\2\u00e3\u00e4\7X\2\2\u00e4"+
		"\u00e5\7C\2\2\u00e5\u00e6\7E\2\2\u00e6\u00e7\7W\2\2\u00e7\u00e8\7W\2\2"+
		"\u00e8\u00e9\7O\2\2\u00e9\66\3\2\2\2\u00ea\u00f0\7)\2\2\u00eb\u00ef\n"+
		"\2\2\2\u00ec\u00ed\7^\2\2\u00ed\u00ef\13\2\2\2\u00ee\u00eb\3\2\2\2\u00ee"+
		"\u00ec\3\2\2\2\u00ef\u00f2\3\2\2\2\u00f0\u00ee\3\2\2\2\u00f0\u00f1\3\2"+
		"\2\2\u00f1\u00f3\3\2\2\2\u00f2\u00f0\3\2\2\2\u00f3\u00ff\7)\2\2\u00f4"+
		"\u00fa\7$\2\2\u00f5\u00f9\n\3\2\2\u00f6\u00f7\7^\2\2\u00f7\u00f9\13\2"+
		"\2\2\u00f8\u00f5\3\2\2\2\u00f8\u00f6\3\2\2\2\u00f9\u00fc\3\2\2\2\u00fa"+
		"\u00f8\3\2\2\2\u00fa\u00fb\3\2\2\2\u00fb\u00fd\3\2\2\2\u00fc\u00fa\3\2"+
		"\2\2\u00fd\u00ff\7$\2\2\u00fe\u00ea\3\2\2\2\u00fe\u00f4\3\2\2\2\u00ff"+
		"8\3\2\2\2\u0100\u0102\5O(\2\u0101\u0100\3\2\2\2\u0102\u0103\3\2\2\2\u0103"+
		"\u0101\3\2\2\2\u0103\u0104\3\2\2\2\u0104\u0105\3\2\2\2\u0105\u0106\7N"+
		"\2\2\u0106:\3\2\2\2\u0107\u0109\5O(\2\u0108\u0107\3\2\2\2\u0109\u010a"+
		"\3\2\2\2\u010a\u0108\3\2\2\2\u010a\u010b\3\2\2\2\u010b\u010c\3\2\2\2\u010c"+
		"\u010d\7U\2\2\u010d<\3\2\2\2\u010e\u0110\5O(\2\u010f\u010e\3\2\2\2\u0110"+
		"\u0111\3\2\2\2\u0111\u010f\3\2\2\2\u0111\u0112\3\2\2\2\u0112\u0113\3\2"+
		"\2\2\u0113\u0114\7[\2\2\u0114>\3\2\2\2\u0115\u0117\5O(\2\u0116\u0115\3"+
		"\2\2\2\u0117\u0118\3\2\2\2\u0118\u0116\3\2\2\2\u0118\u0119\3\2\2\2\u0119"+
		"@\3\2\2\2\u011a\u011c\5O(\2\u011b\u011a\3\2\2\2\u011c\u011d\3\2\2\2\u011d"+
		"\u011b\3\2\2\2\u011d\u011e\3\2\2\2\u011e\u011f\3\2\2\2\u011f\u0120\5M"+
		"\'\2\u0120\u0128\3\2\2\2\u0121\u0123\5K&\2\u0122\u0124\5M\'\2\u0123\u0122"+
		"\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u0125\3\2\2\2\u0125\u0126\6!\2\2\u0126"+
		"\u0128\3\2\2\2\u0127\u011b\3\2\2\2\u0127\u0121\3\2\2\2\u0128B\3\2\2\2"+
		"\u0129\u012b\5O(\2\u012a\u0129\3\2\2\2\u012b\u012c\3\2\2\2\u012c\u012a"+
		"\3\2\2\2\u012c\u012d\3\2\2\2\u012d\u012f\3\2\2\2\u012e\u0130\5M\'\2\u012f"+
		"\u012e\3\2\2\2\u012f\u0130\3\2\2\2\u0130\u0131\3\2\2\2\u0131\u0132\7F"+
		"\2\2\u0132\u013b\3\2\2\2\u0133\u0135\5K&\2\u0134\u0136\5M\'\2\u0135\u0134"+
		"\3\2\2\2\u0135\u0136\3\2\2\2\u0136\u0137\3\2\2\2\u0137\u0138\7F\2\2\u0138"+
		"\u0139\6\"\3\2\u0139\u013b\3\2\2\2\u013a\u012a\3\2\2\2\u013a\u0133\3\2"+
		"\2\2\u013bD\3\2\2\2\u013c\u013e\5O(\2\u013d\u013c\3\2\2\2\u013e\u013f"+
		"\3\2\2\2\u013f\u013d\3\2\2\2\u013f\u0140\3\2\2\2\u0140\u0142\3\2\2\2\u0141"+
		"\u0143\5M\'\2\u0142\u0141\3\2\2\2\u0142\u0143\3\2\2\2\u0143\u0144\3\2"+
		"\2\2\u0144\u0145\7D\2\2\u0145\u0146\7F\2\2\u0146\u0151\3\2\2\2\u0147\u0149"+
		"\5K&\2\u0148\u014a\5M\'\2\u0149\u0148\3\2\2\2\u0149\u014a\3\2\2\2\u014a"+
		"\u014b\3\2\2\2\u014b\u014c\7D\2\2\u014c\u014d\7F\2\2\u014d\u014e\3\2\2"+
		"\2\u014e\u014f\6#\4\2\u014f\u0151\3\2\2\2\u0150\u013d\3\2\2\2\u0150\u0147"+
		"\3\2\2\2\u0151F\3\2\2\2\u0152\u0156\5Q)\2\u0153\u0156\5O(\2\u0154\u0156"+
		"\7a\2\2\u0155\u0152\3\2\2\2\u0155\u0153\3\2\2\2\u0155\u0154\3\2\2\2\u0156"+
		"\u0157\3\2\2\2\u0157\u0155\3\2\2\2\u0157\u0158\3\2\2\2\u0158H\3\2\2\2"+
		"\u0159\u015f\7b\2\2\u015a\u015e\n\4\2\2\u015b\u015c\7b\2\2\u015c\u015e"+
		"\7b\2\2\u015d\u015a\3\2\2\2\u015d\u015b\3\2\2\2\u015e\u0161\3\2\2\2\u015f"+
		"\u015d\3\2\2\2\u015f\u0160\3\2\2\2\u0160\u0162\3\2\2\2\u0161\u015f\3\2"+
		"\2\2\u0162\u0163\7b\2\2\u0163J\3\2\2\2\u0164\u0166\5O(\2\u0165\u0164\3"+
		"\2\2\2\u0166\u0167\3\2\2\2\u0167\u0165\3\2\2\2\u0167\u0168\3\2\2\2\u0168"+
		"\u0169\3\2\2\2\u0169\u016d\7\60\2\2\u016a\u016c\5O(\2\u016b\u016a\3\2"+
		"\2\2\u016c\u016f\3\2\2\2\u016d\u016b\3\2\2\2\u016d\u016e\3\2\2\2\u016e"+
		"\u0177\3\2\2\2\u016f\u016d\3\2\2\2\u0170\u0172\7\60\2\2\u0171\u0173\5"+
		"O(\2\u0172\u0171\3\2\2\2\u0173\u0174\3\2\2\2\u0174\u0172\3\2\2\2\u0174"+
		"\u0175\3\2\2\2\u0175\u0177\3\2\2\2\u0176\u0165\3\2\2\2\u0176\u0170\3\2"+
		"\2\2\u0177L\3\2\2\2\u0178\u017a\7G\2\2\u0179\u017b\t\5\2\2\u017a\u0179"+
		"\3\2\2\2\u017a\u017b\3\2\2\2\u017b\u017d\3\2\2\2\u017c\u017e\5O(\2\u017d"+
		"\u017c\3\2\2\2\u017e\u017f\3\2\2\2\u017f\u017d\3\2\2\2\u017f\u0180\3\2"+
		"\2\2\u0180N\3\2\2\2\u0181\u0182\t\6\2\2\u0182P\3\2\2\2\u0183\u0184\t\7"+
		"\2\2\u0184R\3\2\2\2\u0185\u0186\7/\2\2\u0186\u0187\7/\2\2\u0187\u018b"+
		"\3\2\2\2\u0188\u018a\n\b\2\2\u0189\u0188\3\2\2\2\u018a\u018d\3\2\2\2\u018b"+
		"\u0189\3\2\2\2\u018b\u018c\3\2\2\2\u018c\u018f\3\2\2\2\u018d\u018b\3\2"+
		"\2\2\u018e\u0190\7\17\2\2\u018f\u018e\3\2\2\2\u018f\u0190\3\2\2\2\u0190"+
		"\u0192\3\2\2\2\u0191\u0193\7\f\2\2\u0192\u0191\3\2\2\2\u0192\u0193\3\2"+
		"\2\2\u0193\u0194\3\2\2\2\u0194\u0195\b*\2\2\u0195T\3\2\2\2\u0196\u0197"+
		"\7\61\2\2\u0197\u0198\7,\2\2\u0198\u019c\3\2\2\2\u0199\u019b\13\2\2\2"+
		"\u019a\u0199\3\2\2\2\u019b\u019e\3\2\2\2\u019c\u019d\3\2\2\2\u019c\u019a"+
		"\3\2\2\2\u019d\u019f\3\2\2\2\u019e\u019c\3\2\2\2\u019f\u01a0\7,\2\2\u01a0"+
		"\u01a1\7\61\2\2\u01a1\u01a2\3\2\2\2\u01a2\u01a3\b+\2\2\u01a3V\3\2\2\2"+
		"\u01a4\u01a6\t\t\2\2\u01a5\u01a4\3\2\2\2\u01a6\u01a7\3\2\2\2\u01a7\u01a5"+
		"\3\2\2\2\u01a7\u01a8\3\2\2\2\u01a8\u01a9\3\2\2\2\u01a9\u01aa\b,\2\2\u01aa"+
		"X\3\2\2\2\u01ab\u01ac\13\2\2\2\u01acZ\3\2\2\2\'\2\u00b8\u00ee\u00f0\u00f8"+
		"\u00fa\u00fe\u0103\u010a\u0111\u0118\u011d\u0123\u0127\u012c\u012f\u0135"+
		"\u013a\u013f\u0142\u0149\u0150\u0155\u0157\u015d\u015f\u0167\u016d\u0174"+
		"\u0176\u017a\u017f\u018b\u018f\u0192\u019c\u01a7\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}