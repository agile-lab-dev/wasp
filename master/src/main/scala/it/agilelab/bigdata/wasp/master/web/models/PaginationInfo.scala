package it.agilelab.bigdata.wasp.master.web.models

case class PaginationInfo(page: Integer,
                          rows : Integer,
                          numFound : Long,
                          lengthPage: Integer)
