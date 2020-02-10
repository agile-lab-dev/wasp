package it.agilelab.bigdata.wasp.consumers.spark.utils

import com.typesafe.config.Config
import it.agilelab.darwin.manager.{AvroSchemaManager, AvroSchemaManagerFactory}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast}
import org.apache.spark.sql.types.{BinaryType, ObjectType, StructType}

import scala.reflect.{ClassTag, classTag}

object AvroEncoders {
  def avroEncoder[A: ClassTag](
                                readerSchema: org.apache.avro.Schema,
                                avroSchemaManagerConfig: Config,
                                toGenericRecord: A => org.apache.avro.generic.GenericRecord,
                                fromGenericRecord: org.apache.avro.generic.GenericRecord => A
                              ): Encoder[A] = {
    avroEncoder(
      readerSchema,
      () => AvroSchemaManagerFactory.initialize(avroSchemaManagerConfig),
      toGenericRecord,
      fromGenericRecord
    )
  }

  def avroEncoder[A: ClassTag](readerSchema: org.apache.avro.Schema,
                               avroSchemaManager: () => AvroSchemaManager,
                               toGenericRecord: A => org.apache.avro.generic.GenericRecord,
                               fromGenericRecord: org.apache.avro.generic.GenericRecord => A
                              ): Encoder[A] = {
    ExpressionEncoder[A](
      schema = new StructType().add("value", BinaryType),
      flat = true,
      serializer = Seq(
        EncodeUsingAvro[A](
          BoundReference(0, ObjectType(classOf[AnyRef]), nullable = true),
          readerSchema.toString(),
          toGenericRecord
        )
      ),
      deserializer = DecodeUsingAvro[A](
        Cast(GetColumnByOrdinal(0, BinaryType), BinaryType),
        classTag[A],
        readerSchema.toString(),
        avroSchemaManager,
        fromGenericRecord
      ),
      clsTag = classTag[A]
    )
  }
}
