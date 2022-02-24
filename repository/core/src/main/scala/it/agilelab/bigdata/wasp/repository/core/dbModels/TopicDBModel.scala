package it.agilelab.bigdata.wasp.repository.core.dbModels

import org.bson.BsonDocument
import it.agilelab.bigdata.wasp.models._

trait TopicDBModel extends Model

case class TopicDBModelV1(override val name: String,
                        creationTime: Long,
                        partitions: Int,
                        replicas: Int,
                        topicDataType: String,
                        keyFieldName: Option[String],
                        headersFieldName: Option[String],
                        valueFieldsNames: Option[Seq[String]],
                        useAvroSchemaManager: Boolean,
                        schema: BsonDocument,
                        topicCompression: TopicCompression = TopicCompression.Disabled,
                        subjectStrategy: SubjectStrategy = SubjectStrategy.None,
                        keySchema: Option[String] = None
                       ) extends TopicDBModel


