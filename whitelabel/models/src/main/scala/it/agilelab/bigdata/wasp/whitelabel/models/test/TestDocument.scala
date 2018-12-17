package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models.{Metadata, MetadataModel}

case class TestDocument(id: String,
                        number: Int,
                        nested: TestNestedDocument)

case class TestDocumentHbaseMultiClustering(id: String,
                                            number: Int,
                                            nested: TestNestedDocument,
                                            number_clustering: Int)

case class TestNestedDocument(field1: String,
                              field2: Long,
                              field3: Option[String])


case class TestCheckpointDocument(version: String,
                                  id: String,
                                  value: Int,
                                  sum: Int = -1,
                                  oldSumInt: Int = -1,
                                  oldSumString: String = "-1")

case class TestDocumentWithMetadata(override val metadata: MetadataModel,
                                    id: String,
                                    number: Int,
                                    nested: TestNestedDocument) extends Metadata