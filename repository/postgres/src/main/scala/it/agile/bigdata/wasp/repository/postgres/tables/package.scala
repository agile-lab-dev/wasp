package it.agile.bigdata.wasp.repository.postgres

import java.sql.ResultSet


package object tables {

  implicit class ResultSetHelper(resultSet : ResultSet ) {

    def getOption[T](field : String): Option[T] ={
      resultSet.getObject(field) match {
        case null => None
        case v => Some(v.asInstanceOf[T])
      }
    }


  }
}
