package it.agilelab.bigdata.wasp.whitelabel.models.test

case class TestDocument(id: String,
                        number: Int,
                        nested: TestNestedDocument)

case class TestNestedDocument(field1: String,
                              field2: Long,
                              field3: Option[String])


case class TestCheckpointDocument(version: String,
                                  id: String,
                                  value: Int,
                                  sum: Int = -1,
                                  oldSumInt: Int = -1,
                                  oldSumString: String = "-1")