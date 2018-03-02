package it.agilelab.bigdata.wasp.core.models

import java.time.Instant

import com.google.gson.reflect.TypeToken
import org.json4s.{CustomSerializer, DefaultFormats, Formats}
import org.json4s.JsonAST.JObject

import scala.util.parsing.json.JSONObject
import scala.reflect.runtime.universe._


class IndexModelBuilder[Stage <: IndexModelBuilder.Stage, Kind <: IndexModelBuilder.DataStoreKind : TypeTag] private
(name: String, schema: IndexModelBuilder.UntypedSchema, config: IndexModelBuilder.UntypedConfig, isRolling: Boolean,
 idField: Option[String]) {


  def named(name: String): IndexModelBuilder[Stage with IndexModelBuilder.Stage.Name, Kind] =
    new IndexModelBuilder(name, schema, config, isRolling, idField)

  def schema(schema: IndexModelBuilder.Schema[Kind]): IndexModelBuilder[Stage with IndexModelBuilder.Stage.Schema[Kind], Kind] =
    new IndexModelBuilder(name, schema, config, isRolling, idField)

  def config(config: IndexModelBuilder.Config[Kind]): IndexModelBuilder[Stage with IndexModelBuilder.Stage.Config[Kind], Kind] =
    new IndexModelBuilder(name, schema, config, isRolling, idField)

  def rolling: IndexModelBuilder[Stage, Kind] = new IndexModelBuilder(name, schema, config, true, idField)

  def id(id: String): IndexModelBuilder[Stage, Kind] = new IndexModelBuilder(name, schema, config, true, Some(id))

  def build(implicit evidence: Stage <:< IndexModelBuilder.Stage.Complete[Kind]): IndexModel = {


    schema.augment(
      config.augment(
        IndexModel(
          name = if (name.toLowerCase().endsWith("_index")) name else s"${name.toLowerCase}_index",
          creationTime = System.currentTimeMillis(),
          schema = None)))


  }


}


object IndexModelBuilder {

  def forSolr: IndexModelBuilder[Stage.DataStore[DataStoreKind.Solr], DataStoreKind.Solr] = new IndexModelBuilder("",
    Solr.Schema(), Solr.Config(), false, None)

  def forElastic: IndexModelBuilder[Stage.DataStore[DataStoreKind.Elastic], DataStoreKind.Elastic] = new
      IndexModelBuilder("", Elastic.Schema(JObject()), Elastic.Config(), false, None)

  sealed trait Stage {}

  sealed trait DataStoreKind {}

  sealed trait UntypedSchema {
    def augment(model: IndexModel): IndexModel
  }

  sealed trait Schema[Kind <: DataStoreKind] extends UntypedSchema

  sealed trait UntypedConfig {
    def augment(model: IndexModel): IndexModel
  }

  sealed trait Config[Kind <: DataStoreKind] extends UntypedConfig {

  }


  object Elastic {

    case class Schema(jsonSchema: JObject) extends IndexModelBuilder.Schema[DataStoreKind.Elastic] {
      override def augment(model: IndexModel): IndexModel = {

        import org.json4s.NoTypeHints
        import org.json4s.native.Serialization

        implicit val formats = Serialization.formats(NoTypeHints)

        model.copy(schema = Some(Serialization.write(jsonSchema)))

      }
    }

    case class Config(shards: Int = 1, replica: Int = 1, pushdownQuery: Option[String] = None) extends
      IndexModelBuilder.Config[DataStoreKind.Elastic] {
      override def augment(model: IndexModel): IndexModel = {
        model.copy(numShards = Some(shards), replicationFactor = Some(replica), query = pushdownQuery)
      }
    }

    object Config {
      def default = Config()
    }

  }

  object Solr {

    sealed trait SolrMissingFieldSort

    /**
      *
      * The solr type.
      *
      * @param name     The name of the fieldType for this field. This will be found in the name attribute on the fieldType
      *                 definition. Every field must have a type.
      * @tparam T The type of the field
      */
    abstract class Type[+T](val name: String) {

    }

    case class Config(shards: Int = 1, replica: Int = 1) extends IndexModelBuilder.Config[DataStoreKind.Solr] {
      override def augment(model: IndexModel): IndexModel = {
        model.copy(numShards = Some(shards), replicationFactor = Some(replica))
      }
    }

    case class Schema(solrFields: Solr.FieldData[_]*) extends IndexModelBuilder.Schema[DataStoreKind.Solr] {
      override def augment(model: IndexModel): IndexModel = {

        import org.json4s.NoTypeHints
        import org.json4s.JsonAST._
        import org.json4s.native.Serialization


        class SolrTypeSerializer extends CustomSerializer[Solr.Type[_]](format => ( {
          case _ => throw new RuntimeException("Deserialization not implemented")
        }, {
          case x: Solr.Type[_] => JString(x.name)
        }
        ))


        implicit val formats: Formats = Serialization.formats(NoTypeHints) + new SolrTypeSerializer

        model.copy(schema = Some(Serialization.write(solrFields)))


      }
    }

    /**
      *
      * Class representing a Field of Solr Index.
      *
      * @param name                     The name of the field. Field names should consist of alphanumeric or underscore characters only and not
      *                                 start with a digit. This is not currently strictly enforced, but other field names will not have first
      *                                 class support from all components and back compatibility is not guaranteed. Names with both leading and
      *                                 trailing underscores (e.g., _version_) are reserved. Every field must have a name.
      * @param type                     The type of the field.
      * @param defaultValue             The default value for the field if ingested document does not contain it.
      * @param indexed                  If true, the value of the field can be used in queries to retrieve matching documents.
      * @param stored                   If true, the actual value of the field can be retrieved by queries.
      * @param docValues                If true, the value of the field will be put in a column-oriented DocValues structure.
      * @param sortMissing              Control the placement of documents when a sort field is not present.
      * @param multiValued              If true, indicates that a single document might contain multiple values for this field type.
      * @param omitNorms                If true, omits the norms associated with this field (this disables length normalization for the
      *                                 field, and saves some memory). Defaults to true for all primitive (non-analyzed) field types,
      *                                 such as int, float, data, bool, and string. Only full-text fields or fields need norms.
      * @param omitTermFreqAndPositions if true, omits term frequency, positions, and payloads from postings for this field.
      *                                 This can be a performance boost for fields that don’t require that information.
      *                                 It also reduces the storage space required for the index.
      *                                 Queries that rely on position that are issued on a field with this option will
      *                                 silently fail to find documents.
      *                                 This property defaults to true for all field types that are not text fields.
      * @param omitPositions            Similar to omitTermFreqAndPositions but preserves term frequency information.
      * @param termVectors              instruct Solr to maintain full term vectors for each document
      * @param termPositions            instruct Solr to include position of term occurrence in term vectors
      * @param termOffsets              instruct Solr to include  offset of term occurrence in term vectors
      * @param termPayloads             instruct Solr to include payload of term occurrence in term vectors
      * @param required                 Instructs Solr to reject any attempts to add a document which does not have a value for this field.
      *                                 This property defaults to false.
      * @param useDocValuesAsStored     If the field has docValues enabled, setting this to true would allow the field to be
      *                                 returned as if it were a stored field (even if it has stored=false) when matching
      *                                 “*” in an fl parameter.
      * @param large                    Large fields are always lazy loaded and will only take up space in the document cache if the actual
      *                                 value is < 512KB. This option requires stored="true" and multiValued="false".
      *                                 It’s intended for fields that might have very large values so that they don’t get cached in memory.
      * @tparam A The type contained in this solr field
      *
      */
    case class FieldData[+A] private(name: String,
                                     `type`: Type[A],
                                     defaultValue: Option[A] = None,
                                     indexed: Boolean = true,
                                     stored: Boolean = true,
                                     docValues: Option[Boolean] = None,
                                     sortMissing: Option[SolrMissingFieldSort] = None,
                                     multiValued: Option[Boolean] = None,
                                     omitNorms: Option[Boolean] = None,
                                     omitTermFreqAndPositions: Option[Boolean] = None,
                                     omitPositions: Option[Boolean] = None,
                                     termVectors: Option[Boolean] = None,
                                     termPositions: Option[Boolean] = None,
                                     termOffsets: Option[Boolean] = None,
                                     termPayloads: Option[Boolean] = None,
                                     required: Boolean = false,
                                     useDocValuesAsStored: Option[Boolean] = None,
                                     large: Option[Boolean] = None)

    object Config {
      def default = Config()
    }

    object Field {

      /**
        *
        * Class representing a Field of Solr Index.
        *
        * @param name                     The name of the field. Field names should consist of alphanumeric or underscore characters only and not
        *                                 start with a digit. This is not currently strictly enforced, but other field names will not have first
        *                                 class support from all components and back compatibility is not guaranteed. Names with both leading and
        *                                 trailing underscores (e.g., _version_) are reserved. Every field must have a name.
        * @param solrType                 The type of the field.
        * @param defaultValue             The default value for the field if ingested document does not contain it.
        * @param indexed                  If true, the value of the field can be used in queries to retrieve matching documents.
        * @param stored                   If true, the actual value of the field can be retrieved by queries.
        * @param docValues                If true, the value of the field will be put in a column-oriented DocValues structure.
        * @param sortMissing              Control the placement of documents when a sort field is not present.
        * @param multiValued              If true, indicates that a single document might contain multiple values for this field type.
        * @param omitNorms                If true, omits the norms associated with this field (this disables length normalization for the
        *                                 field, and saves some memory). Defaults to true for all primitive (non-analyzed) field types,
        *                                 such as int, float, data, bool, and string. Only full-text fields or fields need norms.
        * @param omitTermFreqAndPositions if true, omits term frequency, positions, and payloads from postings for this field.
        *                                 This can be a performance boost for fields that don’t require that information.
        *                                 It also reduces the storage space required for the index.
        *                                 Queries that rely on position that are issued on a field with this option will
        *                                 silently fail to find documents.
        *                                 This property defaults to true for all field types that are not text fields.
        * @param omitPositions            Similar to omitTermFreqAndPositions but preserves term frequency information.
        * @param termVectors              instruct Solr to maintain full term vectors for each document
        * @param termPositions            instruct Solr to include position of term occurrence in term vectors
        * @param termOffsets              instruct Solr to include  offset of term occurrence in term vectors
        * @param termPayloads             instruct Solr to include payload of term occurrence in term vectors
        * @param required                 Instructs Solr to reject any attempts to add a document which does not have a value for this field.
        *                                 This property defaults to false.
        * @param useDocValuesAsStored     If the field has docValues enabled, setting this to true would allow the field to be
        *                                 returned as if it were a stored field (even if it has stored=false) when matching
        *                                 “*” in an fl parameter.
        * @param large                    Large fields are always lazy loaded and will only take up space in the document cache if the actual
        *                                 value is < 512KB. This option requires stored="true" and multiValued="false".
        *                                 It’s intended for fields that might have very large values so that they don’t get cached in memory.
        * @tparam A The type contained in this solr field
        *
        */
      def apply[A, B](name: String,
                      solrType: Type[A],
                      defaultValue: Option[A] = None,
                      indexed: Boolean = true,
                      stored: Boolean = true,
                      docValues: Option[Boolean] = None,
                      sortMissing: Option[SolrMissingFieldSort] = None,
                      multiValued: Option[Boolean] = None,
                      omitNorms: Option[Boolean] = None,
                      omitTermFreqAndPositions: Option[Boolean] = None,
                      omitPositions: Option[Boolean] = None,
                      termVectors: Option[Boolean] = None,
                      termPositions: Option[Boolean] = None,
                      termOffsets: Option[Boolean] = None,
                      termPayloads: Option[Boolean] = None,
                      required: Boolean = false,
                      useDocValuesAsStored: Option[Boolean] = None,
                      large: Option[Boolean] = None)(implicit evidence: B <:< A): FieldData[A] = FieldData[A](
        name,
        solrType,
        defaultValue,
        indexed,
        stored,
        docValues,
        sortMissing,
        multiValued,
        omitNorms,
        omitTermFreqAndPositions,
        omitPositions,
        termVectors,
        termPositions,
        termOffsets,
        termPayloads,
        required,
        useDocValuesAsStored,
        large)
    }


    object SolrMissingFieldSort {

      case object SortMissingLast extends SolrMissingFieldSort

      case object SortMissingFirst extends SolrMissingFieldSort

    }

    object Type {

      case class Custom(override val name: String) extends Type[Any](name)

      /**
        * Binary data, base64 encoded.
        */
      case object Binary extends Type[Array[Byte]]("binary")

      /**
        * Contains either true or false. Values of "1", "t", or "T" in the first character are interpreted as true.
        * Any other values in the first character are interpreted as false.
        */
      case object Bool extends Type[Boolean]("boolean")

      /**
        * Supports currencies and exchange rates.
        */
      case object Currency extends Type[String]("currency")

      /**
        * Spatial Search: a latitude/longitude coordinate pair; possibly multi-valued for multiple points.
        * Usually it’s specified as "lat,lon" order with a comma.
        */
      case object LatLonPointSpatial extends Type[(Double, Double)]("location")

      /**
        *
        * Spatial Search: A single-valued n-dimensional point. It’s both for sorting spatial data that is not lat-lon,
        * and for some more rare use-cases. (NOTE: this is not related to the "Point" based numeric fields)
        */
      case object Point extends Type[(Double, Double)]("point")

      /** String (UTF-8 encoded string or Unicode). Strings are intended for small fields and are not tokenized or analyzed
        * in any way. They have a hard limit of slightly less than 32K.
        */
      case object String extends Type[String]("string")

      /**
        * Text, usually multiple words or tokens.
        */
      case object Text extends Type[String]("text_general")

      /**
        * Date field. Represents a point in time with millisecond precision. See the section Working with Dates.
        * precisionStep="0" minimizes index size; precisionStep="8" (the default) enables more efficient range queries. For
        * single valued fields, use docValues="true" for efficient sorting.
        */
      case object TrieDate extends Type[Instant]("date")

      /**
        * Double field (64-bit IEEE floating point). precisionStep="0" minimizes index size; precisionStep="8" (the default)
        * enables more efficient range queries. For single valued fields, use docValues="true" for efficient sorting.
        */
      case object TrieDouble extends Type[Double]("tdouble")

      /**
        * Floating point field (32-bit IEEE floating point) . precisionStep="0" enables efficient numeric sorting and
        * minimizes index size; precisionStep="8" (the default) enables efficient range queries. Use docValues="true"
        * for efficient sorting. For single valued fields, use docValues="true" for efficient sorting.
        */
      case object TrieFloat extends Type[Float]("tfloat")

      /**
        * Integer field (32-bit signed integer). precisionStep="0" enables efficient numeric sorting and minimizes index size;
        * precisionStep="8" (the default) enables efficient range queries. For single valued fields, use docValues="true"
        * for efficient sorting.
        */
      case object TrieInt extends Type[Int]("tint")

      /**
        * Long field (64-bit signed integer). precisionStep="0" minimizes index size; precisionStep="8" (the default)
        * enables more efficient range queries. For single valued fields, use docValues="true" for efficient sorting.
        */
      case object TrieLong extends Type[Double]("tlong")

    }

  }

  object DataStoreKind {

    sealed trait Elastic extends DataStoreKind {}

    sealed trait Solr extends DataStoreKind {}

  }

  object Stage {

    type Complete[Kind <: DataStoreKind] = Name with DataStore[Kind] with Schema[Kind] with Config[Kind]

    sealed trait Name extends Stage

    sealed trait DataStore[Kind <: DataStoreKind] extends Stage

    sealed trait Schema[Kind <: DataStoreKind] extends Stage

    sealed trait Config[Kind <: DataStoreKind] extends Stage

    sealed trait Rolling extends Stage

  }

}


object Runner extends App {

  import IndexModelBuilder._

  val elastic = IndexModelBuilder.forElastic
                                  .named("pippo_index")
                                  .schema(Elastic.Schema(JObject()))
                                  .config(Elastic.Config(shards = 3, replica = 4))
                                  .build


  val solr = IndexModelBuilder.forSolr
                              .named("pippo")
                              .schema(Solr.Schema(
                                  Solr.Field("ciccio", Solr.Type.Binary),
                                  Solr.Field("ciccio1", Solr.Type.Bool),
                                  Solr.Field("ciccio2", Solr.Type.Text, Some("default")),
                                  Solr.Field("ciccio3", Solr.Type.TrieInt, Some(1)),
                                  Solr.Field("ciccio5", Solr.Type.TrieInt, Some(2))))
                              .config(Solr.Config(shards = 3, replica = 4))
                              .build


  println(elastic)
  println(solr)

}
