package it.agilelab.bigdata.wasp.master.web.utils

case class PaginationInfo(page: Integer,
                          rows : Integer,
                          numFound : Long,
                          lengthPage: Integer)
