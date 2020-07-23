package it.agile.bigdata.wasp.repository.postgres.utils

import java.sql.{ResultSet, SQLException, Statement}

import it.agilelab.bigdata.wasp.core.logging.Logging

trait PostgresDBHelper extends ConnectionSupport with ConnectionInfoProvider with Logging{

  private def sqlExecution[S]()(thunk: (Statement) => S): S = {
    val connection = getConnection()
    connection.setAutoCommit(false)
    val statement = connection.createStatement()
    try {
      val output = thunk(statement)
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
      if (statement != null && !statement.isClosed) statement.close()
      if (connection != null && !connection.isClosed) connection.close()
    }
  }


  protected def execute(updates: String*): Unit = {
    sqlExecution() { statement =>
      updates.map { s =>
        logger.debug(s"Executing the postgres query : $s;")
        statement.execute(s)
      }
    }
  }


  private def executeQuery[S](queries: String*)(mapper: (ResultSet) => S) : Seq[S]= {
    sqlExecution() { statement =>
      queries.flatMap { s =>
        logger.debug(s"Executing the postgres query : $s;")
        resultSetInterator[S](statement.executeQuery(s),mapper)
      }
    }
  }

  private def resultSetInterator[S](resultSet: ResultSet,mapper: (ResultSet) => S): Seq[S] ={
    Iterator.continually {resultSet != null && resultSet.next()}.takeWhile(hasNext => hasNext)
      .map(_=> mapper(resultSet)).toSeq
  }



  private[postgres] def selectAll[T](table : String, columns : Array[String], whereCondition : Option[String]=None)(mapper : ResultSet=> T) : Seq[T] ={
    val where = whereCondition.map(w=> s" WHERE $w")
    executeQuery(s"SELECT ${columns.mkString(",")} FROM $table${where.getOrElse("")}")(mapper)
  }

  protected def insert[T](table : String, obj:T)(mapper : T=> Array[(String,Any)]) : Unit = {
    val objTransformed = mapper(obj)
    execute(s"INSERT INTO $table (${objTransformed.map(_._1).mkString(",")}) VALUES (${objTransformed.map(e=> format(e._2)).mkString(",")})")
  }

  protected def updateBy[T](table : String, obj:T, whereCondition : String)(mapper : T=> Array[(String,Any)]) : Unit = {
    val objTransformed = mapper(obj)
    execute(s"update $table SET ${objTransformed.map(e=> s"${e._1}=${format(e._2)}").mkString(",")} where $whereCondition")
  }

  private def format(value : Any): String = {
    if(value.isInstanceOf[String]) s"'$value'"
    else if (value.isInstanceOf[Long] ||
      value.isInstanceOf[Int] ||
      value.isInstanceOf[Double] ||
      value.isInstanceOf[Float] ||
      value.isInstanceOf[Boolean] ) value.toString
    else if (value == null) null
    else throw new Exception(s"Problem to map ${value} into a string : it is a ${value.getClass.getSimpleName}")
    }


  protected def delete(table : String, whereCondition : Option[String]=None) : Unit = {
    val where = whereCondition.map(w=> s" WHERE $w")
    execute(s"DELETE FROM $table ${where.getOrElse("")}")
  }









}
