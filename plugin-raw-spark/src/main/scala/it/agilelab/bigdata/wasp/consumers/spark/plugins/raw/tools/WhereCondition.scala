package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools

import it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools.WhereCondition.PartitionsCombination
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

/**
  * Represents a single where condition used to filter the read dataframe
  * @param outPartitionsCombination the combinations of colName=colValue of the partitions specified in the outputModel
  * @param inPartitionsCombinations  the combinations of colName=colValue of the partitions specified in the inputModel
  *                                 that are not part of the outputModel
  */
case class WhereCondition(outPartitionsCombination: PartitionsCombination,
                          inPartitionsCombinations: List[PartitionsCombination]) {

  /**
    * Each outPartitionsCombination is put in AND with the OR of each innPartitionsCombination
    * Example:
    *   outPartitionsCombination = [ a=1, b=2 ]
    *   inPartitionsCombination = [ [c=3, d=5], [c=3, d=6], [c=4, d=5], [c=4, d=6] ]
    *
    *   output = (a=1 AND b=2) AND ( (c=3 AND d=5) OR (c=3 AND d=6) OR (c=4 AND d=5) OR (c=4 AND d=6) )
    *
    * @return the Spark Column used to filter a DataFrame
    */
  def toSparkColumn: Column = {
    andCondition(outPartitionsCombination).and(orColumns(inPartitionsCombinations.map(andCondition)))
  }

  private def andCondition(partitions: PartitionsCombination): Column = partitions match {
    case Partition(colName, colValue) :: Nil => col(colName).equalTo(lit(colValue))
    case _ =>
      partitions.foldLeft(lit(true)) {
        case (conditionSoFar, Partition(colName, colValue)) =>
          conditionSoFar.and(col(colName).equalTo(lit(colValue)))
      }
  }

  private def orColumns(columns: List[Column]): Column = columns match {
    case Nil => lit(true)
    case _ =>
      columns.foldLeft(lit(false)) {
        case (conditionSoFar, column) =>
          conditionSoFar.or(column)
      }
  }

}

object WhereCondition {
  type PartitionsCombination = List[Partition]
}