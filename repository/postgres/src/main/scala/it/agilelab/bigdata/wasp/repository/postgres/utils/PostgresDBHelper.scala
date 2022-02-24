package it.agilelab.bigdata.wasp.repository.postgres.utils

import it.agilelab.bigdata.wasp.core.logging.Logging
import org.postgresql.util.PGobject
import spray.json.JsValue

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

trait PostgresDBHelper extends ConnectionSupport with ConnectionInfoProvider with Logging{



  private def sqlExecution[S]()(thunk: (Connection) => S): S = {
    val connection = getConnection()
    connection.setAutoCommit(false)
    try {
      val output = thunk(connection)
      connection.commit()
      output
    } catch {
      case sqlException : SQLException => {
        logger.error(sqlException.getMessage, sqlException)
        throw sqlException
      }
      case e: Throwable =>
        val message = s"Problem on postgres query : ${e.getMessage}"
        logger.error(message, e)
        throw new SQLException(message, e)
    } finally {
      if (connection != null && !connection.isClosed) connection.close()
    }
  }


  protected def execute(updates: String*): Unit = {
    sqlExecution() { connection =>
      updates.map { s =>
        logger.debug(s"Executing the postgres query : $s;")
        val preparedStatement = connection.prepareStatement(s)
        preparedStatement.execute()
      }
    }
  }


  def convertToJson(json : JsValue): PGobject = {
    import org.postgresql.util.PGobject
    val jsonObject = new PGobject
    jsonObject.setType("json")
    jsonObject.setValue(json.toString())
    jsonObject
  }


  private def definePreparedStatement(connection : Connection,query : String,values:Array[Any]): PreparedStatement ={
    val preparedStatement = connection.prepareStatement(query)
    logger.debug(s"Executing the postgres query : $query: ${values.mkString(",")}")
    values.zipWithIndex.foreach{case (value,index)=>
      val i = index +1
      value match {
        case v: String => preparedStatement.setString(i,v)
        case v: Int => preparedStatement.setInt(i,v)
        case v: Long => preparedStatement.setLong(i,v)
        case v: Float => preparedStatement.setFloat(i,v)
        case v: Double => preparedStatement.setDouble(i,v)
        case v: Boolean => preparedStatement.setBoolean(i,v)
        case v: Array[Byte] => preparedStatement.setBytes(i,v)
        case j:JsValue => preparedStatement.setObject(i,convertToJson(j))
      }
    }
    preparedStatement
  }

  private def execute(update: String,values:Array[Any]): Unit = {
    sqlExecution() { connection =>
      logger.debug(s"Executing the postgres query : $update;")
      definePreparedStatement(connection,update,values).execute()
      }
    }



  private def executeQuery[S](query: String, values : Array[Any])(mapper: (ResultSet) => S) : Seq[S]= {
    sqlExecution() { connection =>
        logger.debug(s"Executing the postgres query : $query;")
        resultSetInterator[S](definePreparedStatement(connection, query, values).executeQuery(),mapper)
    }
  }

  private def resultSetInterator[S](resultSet: ResultSet,mapper: (ResultSet) => S): Seq[S] ={
    Iterator.continually {resultSet != null && resultSet.next()}.takeWhile(hasNext => hasNext)
      .map(_=> {mapper(resultSet)}).toStream.toList
  }



  private[postgres] def selectAll[T](table : String, columns : Array[String], whereCondition : Option[Array[(String,Any)]]=None,sortCondition : Option[String]=None,limitOption : Option[Int]=None)(mapper : ResultSet=> T) : Seq[T] ={
    val where = whereCondition.map(w=>  s" WHERE ${w.map(e=> s"${e._1}=?").mkString(" AND ")} ").getOrElse("")
    val whereValue = whereCondition.map(_.map(_._2)).getOrElse(Array.empty)
    val sort = sortCondition.map(s=> s" ORDER BY $s").getOrElse("")
    val limit = limitOption.map(l=> s" LIMIT $l").getOrElse("")
    executeQuery(s"SELECT ${columns.mkString(",")} FROM $table$where$sort$limit",whereValue)(mapper)
  }

  protected def insert[T](table : String, obj:T)(mapper : T=> Array[(String,Any)]) : Unit = {
    val objTransformed = mapper(obj)
    execute(s"INSERT INTO $table (${objTransformed.map(_._1).mkString(",")}) VALUES (${objTransformed.map(e=> "?").mkString(",")})",
      objTransformed.map(_._2))
  }

  protected def insertReturning[T,R](table : String, obj:T,columnsResult : Array[String])(mapperFromModel : T=> Array[(String,Any)],mapperFromResultSet : ResultSet => R) : Seq[R] = {
    val objTransformed = mapperFromModel(obj)
    val query = s"INSERT INTO $table(${objTransformed.map(_._1).mkString(",")}) VALUES(${objTransformed.map(_=> "?").mkString(",")}) RETURNING ${columnsResult.mkString(",")}"
    executeQuery(query,objTransformed.map(_._2))(mapperFromResultSet)
  }

  protected def updateBy[T](table : String, obj:T, whereCondition : Array[(String,Any)])(mapper : T=> Array[(String,Any)]) : Unit = {
    val objTransformed = mapper(obj)
    val where = s" WHERE ${whereCondition.map(s=> s"${s._1}=?").mkString(" AND ")} "
    execute(s"update $table SET ${objTransformed.map(e=> s"${e._1} = ?").mkString(",")} $where",
      objTransformed.map(_._2) ++ whereCondition.map(_._2))
  }


  protected def dropTable(table : String) : Unit = {
    execute(s"DROP TABLE IF EXISTS $table")
  }

  protected def delete(table : String, whereCondition : Option[Array[(String,Any)]]=None) : Unit = {
    val where = whereCondition.map(w=>  s" WHERE ${w.map(e=> s"${e._1}=?").mkString(" AND ")} ").getOrElse("")
    val whereValue = whereCondition.map(_.map(_._2)).getOrElse(Array.empty)
    execute(s"DELETE FROM $table $where",whereValue)
  }









}
