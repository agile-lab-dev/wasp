// Generated from /home/andr3a/workspace/delta/src/main/antlr4/io/delta/sql/parser/DeltaSqlBase.g4 by ANTLR 4.7
package io.delta.sql.parser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link DeltaSqlBaseParser}.
 */
public interface DeltaSqlBaseListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link DeltaSqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void enterSingleStatement(DeltaSqlBaseParser.SingleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeltaSqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void exitSingleStatement(DeltaSqlBaseParser.SingleStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code vacuumTable}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterVacuumTable(DeltaSqlBaseParser.VacuumTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code vacuumTable}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitVacuumTable(DeltaSqlBaseParser.VacuumTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeDeltaDetail}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeDeltaDetail(DeltaSqlBaseParser.DescribeDeltaDetailContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeDeltaDetail}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeDeltaDetail(DeltaSqlBaseParser.DescribeDeltaDetailContext ctx);
	/**
	 * Enter a parse tree produced by the {@code generate}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterGenerate(DeltaSqlBaseParser.GenerateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code generate}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitGenerate(DeltaSqlBaseParser.GenerateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeDeltaHistory}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeDeltaHistory(DeltaSqlBaseParser.DescribeDeltaHistoryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeDeltaHistory}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeDeltaHistory(DeltaSqlBaseParser.DescribeDeltaHistoryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code convert}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterConvert(DeltaSqlBaseParser.ConvertContext ctx);
	/**
	 * Exit a parse tree produced by the {@code convert}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitConvert(DeltaSqlBaseParser.ConvertContext ctx);
	/**
	 * Enter a parse tree produced by the {@code passThrough}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterPassThrough(DeltaSqlBaseParser.PassThroughContext ctx);
	/**
	 * Exit a parse tree produced by the {@code passThrough}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitPassThrough(DeltaSqlBaseParser.PassThroughContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeltaSqlBaseParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedName(DeltaSqlBaseParser.QualifiedNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeltaSqlBaseParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedName(DeltaSqlBaseParser.QualifiedNameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link DeltaSqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterUnquotedIdentifier(DeltaSqlBaseParser.UnquotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link DeltaSqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitUnquotedIdentifier(DeltaSqlBaseParser.UnquotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link DeltaSqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifierAlternative(DeltaSqlBaseParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link DeltaSqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifierAlternative(DeltaSqlBaseParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeltaSqlBaseParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifier(DeltaSqlBaseParser.QuotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeltaSqlBaseParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifier(DeltaSqlBaseParser.QuotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeltaSqlBaseParser#colTypeList}.
	 * @param ctx the parse tree
	 */
	void enterColTypeList(DeltaSqlBaseParser.ColTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeltaSqlBaseParser#colTypeList}.
	 * @param ctx the parse tree
	 */
	void exitColTypeList(DeltaSqlBaseParser.ColTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeltaSqlBaseParser#colType}.
	 * @param ctx the parse tree
	 */
	void enterColType(DeltaSqlBaseParser.ColTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeltaSqlBaseParser#colType}.
	 * @param ctx the parse tree
	 */
	void exitColType(DeltaSqlBaseParser.ColTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code primitiveDataType}
	 * labeled alternative in {@link DeltaSqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterPrimitiveDataType(DeltaSqlBaseParser.PrimitiveDataTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code primitiveDataType}
	 * labeled alternative in {@link DeltaSqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitPrimitiveDataType(DeltaSqlBaseParser.PrimitiveDataTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDecimalLiteral(DeltaSqlBaseParser.DecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDecimalLiteral(DeltaSqlBaseParser.DecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterIntegerLiteral(DeltaSqlBaseParser.IntegerLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitIntegerLiteral(DeltaSqlBaseParser.IntegerLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterBigIntLiteral(DeltaSqlBaseParser.BigIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitBigIntLiteral(DeltaSqlBaseParser.BigIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterSmallIntLiteral(DeltaSqlBaseParser.SmallIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitSmallIntLiteral(DeltaSqlBaseParser.SmallIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterTinyIntLiteral(DeltaSqlBaseParser.TinyIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitTinyIntLiteral(DeltaSqlBaseParser.TinyIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDoubleLiteral(DeltaSqlBaseParser.DoubleLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDoubleLiteral(DeltaSqlBaseParser.DoubleLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterBigDecimalLiteral(DeltaSqlBaseParser.BigDecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitBigDecimalLiteral(DeltaSqlBaseParser.BigDecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeltaSqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(DeltaSqlBaseParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeltaSqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(DeltaSqlBaseParser.NonReservedContext ctx);
}