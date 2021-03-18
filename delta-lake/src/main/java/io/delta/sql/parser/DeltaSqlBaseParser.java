// Generated from /home/andr3a/workspace/delta/src/main/antlr4/io/delta/sql/parser/DeltaSqlBase.g4 by ANTLR 4.7
package io.delta.sql.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class DeltaSqlBaseParser extends Parser {
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
		BRACKETED_COMMENT=38, WS=39, UNRECOGNIZED=40, DELIMITER=41;
	public static final int
		RULE_singleStatement = 0, RULE_statement = 1, RULE_qualifiedName = 2, 
		RULE_identifier = 3, RULE_quotedIdentifier = 4, RULE_colTypeList = 5, 
		RULE_colType = 6, RULE_dataType = 7, RULE_number = 8, RULE_nonReserved = 9;
	public static final String[] ruleNames = {
		"singleStatement", "statement", "qualifiedName", "identifier", "quotedIdentifier", 
		"colTypeList", "colType", "dataType", "number", "nonReserved"
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
		"WS", "UNRECOGNIZED", "DELIMITER"
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

	@Override
	public String getGrammarFileName() { return "DeltaSqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }


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

	public DeltaSqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(DeltaSqlBaseParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(20);
			statement();
			setState(21);
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
	public static class PassThroughContext extends StatementContext {
		public PassThroughContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterPassThrough(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitPassThrough(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitPassThrough(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeDeltaDetailContext extends StatementContext {
		public Token path;
		public QualifiedNameContext table;
		public TerminalNode DETAIL() { return getToken(DeltaSqlBaseParser.DETAIL, 0); }
		public TerminalNode DESC() { return getToken(DeltaSqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(DeltaSqlBaseParser.DESCRIBE, 0); }
		public TerminalNode STRING() { return getToken(DeltaSqlBaseParser.STRING, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public DescribeDeltaDetailContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterDescribeDeltaDetail(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitDescribeDeltaDetail(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitDescribeDeltaDetail(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ConvertContext extends StatementContext {
		public QualifiedNameContext table;
		public TerminalNode CONVERT() { return getToken(DeltaSqlBaseParser.CONVERT, 0); }
		public TerminalNode TO() { return getToken(DeltaSqlBaseParser.TO, 0); }
		public TerminalNode DELTA() { return getToken(DeltaSqlBaseParser.DELTA, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode PARTITIONED() { return getToken(DeltaSqlBaseParser.PARTITIONED, 0); }
		public TerminalNode BY() { return getToken(DeltaSqlBaseParser.BY, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public ConvertContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterConvert(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitConvert(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitConvert(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class VacuumTableContext extends StatementContext {
		public Token path;
		public QualifiedNameContext table;
		public TerminalNode VACUUM() { return getToken(DeltaSqlBaseParser.VACUUM, 0); }
		public TerminalNode STRING() { return getToken(DeltaSqlBaseParser.STRING, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode RETAIN() { return getToken(DeltaSqlBaseParser.RETAIN, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public TerminalNode HOURS() { return getToken(DeltaSqlBaseParser.HOURS, 0); }
		public TerminalNode DRY() { return getToken(DeltaSqlBaseParser.DRY, 0); }
		public TerminalNode RUN() { return getToken(DeltaSqlBaseParser.RUN, 0); }
		public VacuumTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterVacuumTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitVacuumTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitVacuumTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class GenerateContext extends StatementContext {
		public IdentifierContext modeName;
		public QualifiedNameContext table;
		public TerminalNode GENERATE() { return getToken(DeltaSqlBaseParser.GENERATE, 0); }
		public TerminalNode FOR() { return getToken(DeltaSqlBaseParser.FOR, 0); }
		public TerminalNode TABLE() { return getToken(DeltaSqlBaseParser.TABLE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public GenerateContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterGenerate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitGenerate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitGenerate(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeDeltaHistoryContext extends StatementContext {
		public Token path;
		public QualifiedNameContext table;
		public Token limit;
		public TerminalNode HISTORY() { return getToken(DeltaSqlBaseParser.HISTORY, 0); }
		public TerminalNode DESC() { return getToken(DeltaSqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(DeltaSqlBaseParser.DESCRIBE, 0); }
		public TerminalNode STRING() { return getToken(DeltaSqlBaseParser.STRING, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIMIT() { return getToken(DeltaSqlBaseParser.LIMIT, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(DeltaSqlBaseParser.INTEGER_VALUE, 0); }
		public DescribeDeltaHistoryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterDescribeDeltaHistory(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitDescribeDeltaHistory(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitDescribeDeltaHistory(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_statement);
		int _la;
		try {
			int _alt;
			setState(78);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
			case 1:
				_localctx = new VacuumTableContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(23);
				match(VACUUM);
				setState(26);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
					{
					setState(24);
					((VacuumTableContext)_localctx).path = match(STRING);
					}
					break;
				case BY:
				case CONVERT:
				case DELTA:
				case DESC:
				case DESCRIBE:
				case DETAIL:
				case GENERATE:
				case DRY:
				case HOURS:
				case LIMIT:
				case FOR:
				case TABLE:
				case PARTITIONED:
				case RETAIN:
				case RUN:
				case TO:
				case VACUUM:
				case IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(25);
					((VacuumTableContext)_localctx).table = qualifiedName();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(32);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==RETAIN) {
					{
					setState(28);
					match(RETAIN);
					setState(29);
					number();
					setState(30);
					match(HOURS);
					}
				}

				setState(36);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DRY) {
					{
					setState(34);
					match(DRY);
					setState(35);
					match(RUN);
					}
				}

				}
				break;
			case 2:
				_localctx = new DescribeDeltaDetailContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(38);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(39);
				match(DETAIL);
				setState(42);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
					{
					setState(40);
					((DescribeDeltaDetailContext)_localctx).path = match(STRING);
					}
					break;
				case BY:
				case CONVERT:
				case DELTA:
				case DESC:
				case DESCRIBE:
				case DETAIL:
				case GENERATE:
				case DRY:
				case HOURS:
				case LIMIT:
				case FOR:
				case TABLE:
				case PARTITIONED:
				case RETAIN:
				case RUN:
				case TO:
				case VACUUM:
				case IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(41);
					((DescribeDeltaDetailContext)_localctx).table = qualifiedName();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 3:
				_localctx = new GenerateContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(44);
				match(GENERATE);
				setState(45);
				((GenerateContext)_localctx).modeName = identifier();
				setState(46);
				match(FOR);
				setState(47);
				match(TABLE);
				setState(48);
				((GenerateContext)_localctx).table = qualifiedName();
				}
				break;
			case 4:
				_localctx = new DescribeDeltaHistoryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(50);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(51);
				match(HISTORY);
				setState(54);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case STRING:
					{
					setState(52);
					((DescribeDeltaHistoryContext)_localctx).path = match(STRING);
					}
					break;
				case BY:
				case CONVERT:
				case DELTA:
				case DESC:
				case DESCRIBE:
				case DETAIL:
				case GENERATE:
				case DRY:
				case HOURS:
				case LIMIT:
				case FOR:
				case TABLE:
				case PARTITIONED:
				case RETAIN:
				case RUN:
				case TO:
				case VACUUM:
				case IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(53);
					((DescribeDeltaHistoryContext)_localctx).table = qualifiedName();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(58);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIMIT) {
					{
					setState(56);
					match(LIMIT);
					setState(57);
					((DescribeDeltaHistoryContext)_localctx).limit = match(INTEGER_VALUE);
					}
				}

				}
				break;
			case 5:
				_localctx = new ConvertContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(60);
				match(CONVERT);
				setState(61);
				match(TO);
				setState(62);
				match(DELTA);
				setState(63);
				((ConvertContext)_localctx).table = qualifiedName();
				setState(70);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(64);
					match(PARTITIONED);
					setState(65);
					match(BY);
					setState(66);
					match(T__0);
					setState(67);
					colTypeList();
					setState(68);
					match(T__1);
					}
				}

				}
				break;
			case 6:
				_localctx = new PassThroughContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(75);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,7,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(72);
						matchWildcard();
						}
						} 
					}
					setState(77);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,7,_ctx);
				}
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

	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_qualifiedName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(80);
			identifier();
			setState(85);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(81);
				match(T__2);
				setState(82);
				identifier();
				}
				}
				setState(87);
				_errHandler.sync(this);
				_la = _input.LA(1);
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

	public static class IdentifierContext extends ParserRuleContext {
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
	 
		public IdentifierContext() { }
		public void copyFrom(IdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QuotedIdentifierAlternativeContext extends IdentifierContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public QuotedIdentifierAlternativeContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterQuotedIdentifierAlternative(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitQuotedIdentifierAlternative(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifierAlternative(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnquotedIdentifierContext extends IdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(DeltaSqlBaseParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_identifier);
		try {
			setState(91);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(88);
				match(IDENTIFIER);
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new QuotedIdentifierAlternativeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(89);
				quotedIdentifier();
				}
				break;
			case BY:
			case CONVERT:
			case DELTA:
			case DESC:
			case DESCRIBE:
			case DETAIL:
			case GENERATE:
			case DRY:
			case HOURS:
			case LIMIT:
			case FOR:
			case TABLE:
			case PARTITIONED:
			case RETAIN:
			case RUN:
			case TO:
			case VACUUM:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(90);
				nonReserved();
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

	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(DeltaSqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(93);
			match(BACKQUOTED_IDENTIFIER);
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

	public static class ColTypeListContext extends ParserRuleContext {
		public List<ColTypeContext> colType() {
			return getRuleContexts(ColTypeContext.class);
		}
		public ColTypeContext colType(int i) {
			return getRuleContext(ColTypeContext.class,i);
		}
		public ColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitColTypeList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitColTypeList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeListContext colTypeList() throws RecognitionException {
		ColTypeListContext _localctx = new ColTypeListContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_colTypeList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(95);
			colType();
			setState(100);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(96);
				match(T__3);
				setState(97);
				colType();
				}
				}
				setState(102);
				_errHandler.sync(this);
				_la = _input.LA(1);
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

	public static class ColTypeContext extends ParserRuleContext {
		public IdentifierContext colName;
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(DeltaSqlBaseParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(DeltaSqlBaseParser.NULL, 0); }
		public TerminalNode COMMENT() { return getToken(DeltaSqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(DeltaSqlBaseParser.STRING, 0); }
		public ColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitColType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitColType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeContext colType() throws RecognitionException {
		ColTypeContext _localctx = new ColTypeContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_colType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(103);
			((ColTypeContext)_localctx).colName = identifier();
			setState(104);
			dataType();
			setState(107);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(105);
				match(NOT);
				setState(106);
				match(NULL);
				}
			}

			setState(111);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(109);
				match(COMMENT);
				setState(110);
				match(STRING);
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

	public static class DataTypeContext extends ParserRuleContext {
		public DataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataType; }
	 
		public DataTypeContext() { }
		public void copyFrom(DataTypeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class PrimitiveDataTypeContext extends DataTypeContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(DeltaSqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(DeltaSqlBaseParser.INTEGER_VALUE, i);
		}
		public PrimitiveDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterPrimitiveDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitPrimitiveDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitPrimitiveDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_dataType);
		int _la;
		try {
			_localctx = new PrimitiveDataTypeContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(113);
			identifier();
			setState(124);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(114);
				match(T__0);
				setState(115);
				match(INTEGER_VALUE);
				setState(120);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(116);
					match(T__3);
					setState(117);
					match(INTEGER_VALUE);
					}
					}
					setState(122);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(123);
				match(T__1);
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

	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(DeltaSqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(DeltaSqlBaseParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BigIntLiteralContext extends NumberContext {
		public TerminalNode BIGINT_LITERAL() { return getToken(DeltaSqlBaseParser.BIGINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(DeltaSqlBaseParser.MINUS, 0); }
		public BigIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterBigIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitBigIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitBigIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TinyIntLiteralContext extends NumberContext {
		public TerminalNode TINYINT_LITERAL() { return getToken(DeltaSqlBaseParser.TINYINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(DeltaSqlBaseParser.MINUS, 0); }
		public TinyIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterTinyIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitTinyIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitTinyIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BigDecimalLiteralContext extends NumberContext {
		public TerminalNode BIGDECIMAL_LITERAL() { return getToken(DeltaSqlBaseParser.BIGDECIMAL_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(DeltaSqlBaseParser.MINUS, 0); }
		public BigDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterBigDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitBigDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitBigDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_LITERAL() { return getToken(DeltaSqlBaseParser.DOUBLE_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(DeltaSqlBaseParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(DeltaSqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(DeltaSqlBaseParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SmallIntLiteralContext extends NumberContext {
		public TerminalNode SMALLINT_LITERAL() { return getToken(DeltaSqlBaseParser.SMALLINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(DeltaSqlBaseParser.MINUS, 0); }
		public SmallIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterSmallIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitSmallIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitSmallIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_number);
		int _la;
		try {
			setState(154);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
			case 1:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(127);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(126);
					match(MINUS);
					}
				}

				setState(129);
				match(DECIMAL_VALUE);
				}
				break;
			case 2:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(131);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(130);
					match(MINUS);
					}
				}

				setState(133);
				match(INTEGER_VALUE);
				}
				break;
			case 3:
				_localctx = new BigIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(135);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(134);
					match(MINUS);
					}
				}

				setState(137);
				match(BIGINT_LITERAL);
				}
				break;
			case 4:
				_localctx = new SmallIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(139);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(138);
					match(MINUS);
					}
				}

				setState(141);
				match(SMALLINT_LITERAL);
				}
				break;
			case 5:
				_localctx = new TinyIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(143);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(142);
					match(MINUS);
					}
				}

				setState(145);
				match(TINYINT_LITERAL);
				}
				break;
			case 6:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(147);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(146);
					match(MINUS);
					}
				}

				setState(149);
				match(DOUBLE_LITERAL);
				}
				break;
			case 7:
				_localctx = new BigDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(151);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(150);
					match(MINUS);
					}
				}

				setState(153);
				match(BIGDECIMAL_LITERAL);
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

	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode VACUUM() { return getToken(DeltaSqlBaseParser.VACUUM, 0); }
		public TerminalNode RETAIN() { return getToken(DeltaSqlBaseParser.RETAIN, 0); }
		public TerminalNode HOURS() { return getToken(DeltaSqlBaseParser.HOURS, 0); }
		public TerminalNode DRY() { return getToken(DeltaSqlBaseParser.DRY, 0); }
		public TerminalNode RUN() { return getToken(DeltaSqlBaseParser.RUN, 0); }
		public TerminalNode CONVERT() { return getToken(DeltaSqlBaseParser.CONVERT, 0); }
		public TerminalNode TO() { return getToken(DeltaSqlBaseParser.TO, 0); }
		public TerminalNode DELTA() { return getToken(DeltaSqlBaseParser.DELTA, 0); }
		public TerminalNode PARTITIONED() { return getToken(DeltaSqlBaseParser.PARTITIONED, 0); }
		public TerminalNode BY() { return getToken(DeltaSqlBaseParser.BY, 0); }
		public TerminalNode DESC() { return getToken(DeltaSqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(DeltaSqlBaseParser.DESCRIBE, 0); }
		public TerminalNode LIMIT() { return getToken(DeltaSqlBaseParser.LIMIT, 0); }
		public TerminalNode DETAIL() { return getToken(DeltaSqlBaseParser.DETAIL, 0); }
		public TerminalNode GENERATE() { return getToken(DeltaSqlBaseParser.GENERATE, 0); }
		public TerminalNode FOR() { return getToken(DeltaSqlBaseParser.FOR, 0); }
		public TerminalNode TABLE() { return getToken(DeltaSqlBaseParser.TABLE, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeltaSqlBaseListener ) ((DeltaSqlBaseListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeltaSqlBaseVisitor ) return ((DeltaSqlBaseVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(156);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << BY) | (1L << CONVERT) | (1L << DELTA) | (1L << DESC) | (1L << DESCRIBE) | (1L << DETAIL) | (1L << GENERATE) | (1L << DRY) | (1L << HOURS) | (1L << LIMIT) | (1L << FOR) | (1L << TABLE) | (1L << PARTITIONED) | (1L << RETAIN) | (1L << RUN) | (1L << TO) | (1L << VACUUM))) != 0)) ) {
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

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3+\u00a1\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\3\2\3\2\3\2\3\3\3\3\3\3\5\3\35\n\3\3\3\3\3\3\3\3\3\5\3#\n\3\3\3\3"+
		"\3\5\3\'\n\3\3\3\3\3\3\3\3\3\5\3-\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\5\39\n\3\3\3\3\3\5\3=\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\5\3I\n\3\3\3\7\3L\n\3\f\3\16\3O\13\3\5\3Q\n\3\3\4\3\4\3\4\7\4V\n"+
		"\4\f\4\16\4Y\13\4\3\5\3\5\3\5\5\5^\n\5\3\6\3\6\3\7\3\7\3\7\7\7e\n\7\f"+
		"\7\16\7h\13\7\3\b\3\b\3\b\3\b\5\bn\n\b\3\b\3\b\5\br\n\b\3\t\3\t\3\t\3"+
		"\t\3\t\7\ty\n\t\f\t\16\t|\13\t\3\t\5\t\177\n\t\3\n\5\n\u0082\n\n\3\n\3"+
		"\n\5\n\u0086\n\n\3\n\3\n\5\n\u008a\n\n\3\n\3\n\5\n\u008e\n\n\3\n\3\n\5"+
		"\n\u0092\n\n\3\n\3\n\5\n\u0096\n\n\3\n\3\n\5\n\u009a\n\n\3\n\5\n\u009d"+
		"\n\n\3\13\3\13\3\13\3M\2\f\2\4\6\b\n\f\16\20\22\24\2\4\3\2\13\f\6\2\7"+
		"\7\t\17\21\22\26\34\2\u00b8\2\26\3\2\2\2\4P\3\2\2\2\6R\3\2\2\2\b]\3\2"+
		"\2\2\n_\3\2\2\2\fa\3\2\2\2\16i\3\2\2\2\20s\3\2\2\2\22\u009c\3\2\2\2\24"+
		"\u009e\3\2\2\2\26\27\5\4\3\2\27\30\7\2\2\3\30\3\3\2\2\2\31\34\7\34\2\2"+
		"\32\35\7\35\2\2\33\35\5\6\4\2\34\32\3\2\2\2\34\33\3\2\2\2\35\"\3\2\2\2"+
		"\36\37\7\31\2\2\37 \5\22\n\2 !\7\21\2\2!#\3\2\2\2\"\36\3\2\2\2\"#\3\2"+
		"\2\2#&\3\2\2\2$%\7\17\2\2%\'\7\32\2\2&$\3\2\2\2&\'\3\2\2\2\'Q\3\2\2\2"+
		"()\t\2\2\2),\7\r\2\2*-\7\35\2\2+-\5\6\4\2,*\3\2\2\2,+\3\2\2\2-Q\3\2\2"+
		"\2./\7\16\2\2/\60\5\b\5\2\60\61\7\26\2\2\61\62\7\27\2\2\62\63\5\6\4\2"+
		"\63Q\3\2\2\2\64\65\t\2\2\2\658\7\20\2\2\669\7\35\2\2\679\5\6\4\28\66\3"+
		"\2\2\28\67\3\2\2\29<\3\2\2\2:;\7\22\2\2;=\7!\2\2<:\3\2\2\2<=\3\2\2\2="+
		"Q\3\2\2\2>?\7\t\2\2?@\7\33\2\2@A\7\n\2\2AH\5\6\4\2BC\7\30\2\2CD\7\7\2"+
		"\2DE\7\3\2\2EF\5\f\7\2FG\7\4\2\2GI\3\2\2\2HB\3\2\2\2HI\3\2\2\2IQ\3\2\2"+
		"\2JL\13\2\2\2KJ\3\2\2\2LO\3\2\2\2MN\3\2\2\2MK\3\2\2\2NQ\3\2\2\2OM\3\2"+
		"\2\2P\31\3\2\2\2P(\3\2\2\2P.\3\2\2\2P\64\3\2\2\2P>\3\2\2\2PM\3\2\2\2Q"+
		"\5\3\2\2\2RW\5\b\5\2ST\7\5\2\2TV\5\b\5\2US\3\2\2\2VY\3\2\2\2WU\3\2\2\2"+
		"WX\3\2\2\2X\7\3\2\2\2YW\3\2\2\2Z^\7%\2\2[^\5\n\6\2\\^\5\24\13\2]Z\3\2"+
		"\2\2][\3\2\2\2]\\\3\2\2\2^\t\3\2\2\2_`\7&\2\2`\13\3\2\2\2af\5\16\b\2b"+
		"c\7\6\2\2ce\5\16\b\2db\3\2\2\2eh\3\2\2\2fd\3\2\2\2fg\3\2\2\2g\r\3\2\2"+
		"\2hf\3\2\2\2ij\5\b\5\2jm\5\20\t\2kl\7\24\2\2ln\7\25\2\2mk\3\2\2\2mn\3"+
		"\2\2\2nq\3\2\2\2op\7\b\2\2pr\7\35\2\2qo\3\2\2\2qr\3\2\2\2r\17\3\2\2\2"+
		"s~\5\b\5\2tu\7\3\2\2uz\7!\2\2vw\7\6\2\2wy\7!\2\2xv\3\2\2\2y|\3\2\2\2z"+
		"x\3\2\2\2z{\3\2\2\2{}\3\2\2\2|z\3\2\2\2}\177\7\4\2\2~t\3\2\2\2~\177\3"+
		"\2\2\2\177\21\3\2\2\2\u0080\u0082\7\23\2\2\u0081\u0080\3\2\2\2\u0081\u0082"+
		"\3\2\2\2\u0082\u0083\3\2\2\2\u0083\u009d\7\"\2\2\u0084\u0086\7\23\2\2"+
		"\u0085\u0084\3\2\2\2\u0085\u0086\3\2\2\2\u0086\u0087\3\2\2\2\u0087\u009d"+
		"\7!\2\2\u0088\u008a\7\23\2\2\u0089\u0088\3\2\2\2\u0089\u008a\3\2\2\2\u008a"+
		"\u008b\3\2\2\2\u008b\u009d\7\36\2\2\u008c\u008e\7\23\2\2\u008d\u008c\3"+
		"\2\2\2\u008d\u008e\3\2\2\2\u008e\u008f\3\2\2\2\u008f\u009d\7\37\2\2\u0090"+
		"\u0092\7\23\2\2\u0091\u0090\3\2\2\2\u0091\u0092\3\2\2\2\u0092\u0093\3"+
		"\2\2\2\u0093\u009d\7 \2\2\u0094\u0096\7\23\2\2\u0095\u0094\3\2\2\2\u0095"+
		"\u0096\3\2\2\2\u0096\u0097\3\2\2\2\u0097\u009d\7#\2\2\u0098\u009a\7\23"+
		"\2\2\u0099\u0098\3\2\2\2\u0099\u009a\3\2\2\2\u009a\u009b\3\2\2\2\u009b"+
		"\u009d\7$\2\2\u009c\u0081\3\2\2\2\u009c\u0085\3\2\2\2\u009c\u0089\3\2"+
		"\2\2\u009c\u008d\3\2\2\2\u009c\u0091\3\2\2\2\u009c\u0095\3\2\2\2\u009c"+
		"\u0099\3\2\2\2\u009d\23\3\2\2\2\u009e\u009f\t\3\2\2\u009f\25\3\2\2\2\32"+
		"\34\"&,8<HMPW]fmqz~\u0081\u0085\u0089\u008d\u0091\u0095\u0099\u009c";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}