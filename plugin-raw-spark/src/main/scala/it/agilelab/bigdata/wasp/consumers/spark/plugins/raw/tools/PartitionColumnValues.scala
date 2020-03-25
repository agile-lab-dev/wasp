package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools

case class PartitionColumnValues(name: String, values: List[String])

case class Partition(colName: String, colValue: String) {
  def toDirName: String = s"$colName=$colValue"
}

object Partition {
  def apply(dirName: String): Partition = {
    val splits = dirName.split("=")
    if (splits.length == 2) {
      new Partition(splits(0), splits(1))
    } else {
      throw new IllegalArgumentException("Wrong input directory name")
    }
  }
}