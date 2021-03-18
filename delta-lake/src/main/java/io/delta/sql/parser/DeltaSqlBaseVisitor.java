// Generated from /home/andr3a/workspace/delta/src/main/antlr4/io/delta/sql/parser/DeltaSqlBase.g4 by ANTLR 4.7
package io.delta.sql.parser;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link DeltaSqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface DeltaSqlBaseVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link DeltaSqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleStatement(DeltaSqlBaseParser.SingleStatementContext ctx);
	/**
	 * Visit a parse tree produced by the {@code vacuumTable}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVacuumTable(DeltaSqlBaseParser.VacuumTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code describeDeltaDetail}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeDeltaDetail(DeltaSqlBaseParser.DescribeDeltaDetailContext ctx);
	/**
	 * Visit a parse tree produced by the {@code generate}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGenerate(DeltaSqlBaseParser.GenerateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code describeDeltaHistory}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeDeltaHistory(DeltaSqlBaseParser.DescribeDeltaHistoryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code convert}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConvert(DeltaSqlBaseParser.ConvertContext ctx);
	/**
	 * Visit a parse tree produced by the {@code passThrough}
	 * labeled alternative in {@link DeltaSqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPassThrough(DeltaSqlBaseParser.PassThroughContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeltaSqlBaseParser#qualifiedName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedName(DeltaSqlBaseParser.QualifiedNameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link DeltaSqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnquotedIdentifier(DeltaSqlBaseParser.UnquotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link DeltaSqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifierAlternative(DeltaSqlBaseParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeltaSqlBaseParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifier(DeltaSqlBaseParser.QuotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeltaSqlBaseParser#colTypeList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColTypeList(DeltaSqlBaseParser.ColTypeListContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeltaSqlBaseParser#colType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColType(DeltaSqlBaseParser.ColTypeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code primitiveDataType}
	 * labeled alternative in {@link DeltaSqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimitiveDataType(DeltaSqlBaseParser.PrimitiveDataTypeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDecimalLiteral(DeltaSqlBaseParser.DecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntegerLiteral(DeltaSqlBaseParser.IntegerLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBigIntLiteral(DeltaSqlBaseParser.BigIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSmallIntLiteral(DeltaSqlBaseParser.SmallIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTinyIntLiteral(DeltaSqlBaseParser.TinyIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDoubleLiteral(DeltaSqlBaseParser.DoubleLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link DeltaSqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBigDecimalLiteral(DeltaSqlBaseParser.BigDecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeltaSqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonReserved(DeltaSqlBaseParser.NonReservedContext ctx);
}