package it.agilelab.bigdata.wasp.whitelabel.models.test


case class TestDocument(id: String,
                        number: Int,
                        nested: TestNestedDocument)


case class TestNestedDocument(field1: String,
                              field2: Long,
                              field3: Option[String])