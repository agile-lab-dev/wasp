package it.agilelab.bigdata.wasp.consumers.spark

import org.apache.spark.sql.DataFrame

package object strategies {

 implicit class MapReaderKeyDataFrame(dataFrames: Map[ReaderKey, DataFrame]) {

   def getFirstDataFrame :DataFrame = dataFrames.head._2

   def getFirstReaderKey : ReaderKey= dataFrames.head._1

   def getAllDataFrames: Iterable[DataFrame] = dataFrames.values

   def getAllReaderKeys: Iterable[ReaderKey] = dataFrames.keys

   def filterByName(name : String): Iterable[DataFrame] =
     dataFrames.filter(e=> e._1.name.equalsIgnoreCase(name)).values

   def filterBySourceTypeName(sourceTypeName : String):  Iterable[DataFrame] =
     dataFrames.filter(e => e._1.sourceTypeName.equalsIgnoreCase(sourceTypeName)).values

   def getByReaderKey(name : String,sourceTypeName : String) : Option[DataFrame] =
     dataFrames.get(ReaderKey(name,sourceTypeName))

  }

}
