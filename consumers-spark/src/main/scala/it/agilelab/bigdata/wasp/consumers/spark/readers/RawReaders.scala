package it.agilelab.bigdata.wasp.consumers.spark.readers

import java.net.URI
import scala.concurrent.Await

import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.RawBL
import it.agilelab.bigdata.wasp.core.models.RawModel
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

class HDFSReader(hdfsModel: RawModel) extends StaticReader {
	override val name: String = hdfsModel.name
	override val readerType: String = "hdfs"
	
	override def read(sc: SparkContext): DataFrame = {
		// get sql context
		val sqlContext = SQLContext.getOrCreate(sc)
		
		// setup reader
		val schema: StructType = DataType.fromJson(hdfsModel.schema).asInstanceOf[StructType]
		val options = hdfsModel.options
		val reader = sqlContext.read
			.schema(schema)
			.format(options.format)
			.options(options.extraOptions.getOrElse(Map()))
		
		// calculate path
		val path = if (hdfsModel.timed) {
			// the path is timed; find and return the most recent subdirectory
			val hdfsPath = new Path(hdfsModel.uri)
			val hdfs = hdfsPath.getFileSystem(sc.hadoopConfiguration)
			val subdirectories = hdfs.listStatus(hdfsPath)
				.toList
				.filter(_.isDirectory)
			val mostRecentSubdirectory = subdirectories.sortBy(_.getPath.getName)
			  .reverse
			  .head
			  .getPath
			
			mostRecentSubdirectory.toString
		} else {
			// the path is not timed; return it as-is
			hdfsModel.uri
		}
		
		// read
		reader.load(path)
	}
}

object RawReader {
	
	def create(rawBL: RawBL, id: String, name: String): Option[StaticReader] = {
		// get the raw model using the provided id & bl
		val rawModelOpt = rawBL.getById(id)
		
		// if we found a model, try to return the correct reader
		if (rawModelOpt.isDefined) {
			val rawModel = rawModelOpt.get
			
			// return the correct reader using the uri scheme
			val scheme = new URI(rawModel.uri).getScheme
			scheme match {
				case "hdfs" => Some(new HDFSReader(rawModel))
				case _ => None
			}
		} else {
			None
		}
	}
}
