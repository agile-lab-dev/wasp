package it.agilelab.bigdata.wasp.models

import java.time.Instant
import spray.json._
import DefaultJsonProtocol._
import it.agilelab.bigdata.wasp.models.IndexModelBuilder.Solr


/**
  * A builder able to create instances of [[IndexModel]].
  *
  * @param name The current index name
  * @param schema The current index schema
  * @param config The current index config
  * @param isRolling Whether the index should be rolling
  * @param id Wether the index has an id field
  * @param options Wether the index has an options field
  * @tparam Stage The current [[Stage]] of the builder.
  * @tparam Kind The kind of DataStore whose index is being built.
  */
class IndexModelBuilder[Stage <: IndexModelBuilder.Stage, Kind <: IndexModelBuilder.DataStoreKind] private
(name: String, schema: IndexModelBuilder.UntypedSchema, config: IndexModelBuilder.UntypedConfig, isRolling: Boolean,
 id: Option[String], options: Map[String, String]) {


  /**
    * Assigns a name to the index
    * @param name The name of the index
    * @return An instance of builder with Name stage completed
    */
  def named(name: String): IndexModelBuilder[Stage with IndexModelBuilder.Stage.Name, Kind] =
    new IndexModelBuilder(name, schema, config, isRolling, id, options)

  /**
    * Assigns a schema to the index
    * @param schema The schema of the index
    * @return An instance of builder with Schema stage completed
    */
  def schema(schema: IndexModelBuilder.Schema[Kind]): IndexModelBuilder[Stage with IndexModelBuilder.Stage.Schema[Kind], Kind] =
    new IndexModelBuilder(name, schema, config, isRolling, id, options)

  /**
    * Assigns a config to the index
    * @param config The config of the index
    * @return An instance of builder with Config stage completed
    */
  def config(config: IndexModelBuilder.Config[Kind]): IndexModelBuilder[Stage with IndexModelBuilder.Stage.Config[Kind], Kind] =
    new IndexModelBuilder(name, schema, config, isRolling, id, options)

  /**
    * Mark the index as rolling
    * @return An instance of builder with Rolling stage completed
    */
  def rolling:  IndexModelBuilder[Stage with IndexModelBuilder.Stage.Rolling, Kind] = new IndexModelBuilder(name,
    schema, config, true, id, options)

  /**
    * Assings an id field to the index
    * @return An instance of builder with Id stage completed
    */
  def id(id: String): IndexModelBuilder[Stage with IndexModelBuilder.Stage.Id, Kind] =
    new IndexModelBuilder(name, schema, config, isRolling, Some(id), options)
  
  /**
    * Assings an options field to the index
    * @return An instance of builder with Options stage completed
    */
  def options(options: Map[String, String]): IndexModelBuilder[Stage with IndexModelBuilder.Stage.Options, Kind] =
    new IndexModelBuilder(name, schema, config, isRolling, id, options)

  /**
    * Builds an index model, this method is callable only when Stage is a subclass of [[IndexModelBuilder.Stage.Complete]]
    * (has more trait mixed but not less).
    *
    * @param evidence The evidence that the current Stage is a subclass of [[IndexModelBuilder.Stage.Complete]]
    * @return The built [[IndexModel]]
    */
  def build(implicit evidence: Stage <:< IndexModelBuilder.Stage.Complete[Kind]): IndexModel =
    schema.augment(
      config.augment(
        IndexModel(
          name = if (name.toLowerCase().endsWith("_index")) name else s"${name.toLowerCase}_index",
          creationTime = System.currentTimeMillis(),
          rollingIndex = isRolling,
          idField = id,
          schema = None,
          options = options)
      )
    )


}

object SpraySolrProtocol extends DefaultJsonProtocol {
  import spray.json._

  implicit def typeSerializer[A]: JsonFormat[Solr.Type[A]] = new JsonFormat[Solr.Type[A]] {
    override def read(json: JsValue): Solr.Type[A] = deserializationError("deserialization not implemented")

    override def write(obj: Solr.Type[A]): JsValue = JsString(obj.name)
  }

  implicit def typeSerializerUnderscore: JsonFormat[Solr.Type[_]] = new JsonFormat[Solr.Type[_]] {
    override def read(json: JsValue): Solr.Type[_] = deserializationError("deserialization not implemented")

    override def write(obj: Solr.Type[_]): JsValue = JsString(obj.name)
  }

  implicit object missingFieldSortSerializer extends JsonFormat[Solr.SolrMissingFieldSort] {
    override def read(json: JsValue): Solr.SolrMissingFieldSort = deserializationError("deserialization not implemented")

    override def write(obj: Solr.SolrMissingFieldSort): JsValue = JsString("")
  }

  implicit val fieldSerializer: JsonFormat[Solr.Field[_]] = new JsonFormat[Solr.Field[_]] {
    override def read(json: JsValue): Solr.Field[_] = deserializationError("deserialization not implemented")

    override def write(obj: Solr.Field[_]): JsValue =
      obj.jsonFormat.write(obj)
  }

  def fieldSerializerNonImplicit[A: JsonFormat]: JsonFormat[Solr.Field[A]] = jsonFormat(
    Solr.Field.apply[A],
    "name",
    "type",
    "defaultValue",
    "indexed",
    "stored",
    "docValues",
    "sortMissing",
    "multiValued",
    "omitNorms",
    "omitTermFreqAndPositions",
    "omitPositions",
    "termVectors",
    "termPositions",
    "termOffsets",
    "termPayloads",
    "required",
    "useDocValuesAsStored",
    "large"
  )

  implicit val instantSerializer: JsonFormat[java.time.Instant] = new JsonFormat[java.time.Instant] {
    override def read(json: JsValue): java.time.Instant = deserializationError("deserialization not implemented")

    override def write(obj: java.time.Instant): JsValue =
      JsString(obj.toString)
  }
}


/**
  * Companion object of [[IndexModelBuilder]], contains the syntax.
  *
  * import IndexModelBuilder._ when you want to construct an [[IndexModel]].
  */
object IndexModelBuilder {

  /**
    * Creates an [[IndexModelBuilder]] setup to create Solr Indices
    * @return The builder preconfigured for Solr indices building
    */
  def forSolr: IndexModelBuilder[Stage.DataStore[DataStoreKind.Solr], DataStoreKind.Solr] = new IndexModelBuilder("",
    Solr.Schema(), Solr.Config(), false, None, Map.empty)

  /**
    * creates an [[IndexModelBuilder]] setup to create Elastic Indices
    * @return The builder preconfigured for Elastic indices building
    */
  def forElastic: IndexModelBuilder[Stage.DataStore[DataStoreKind.Elastic], DataStoreKind.Elastic] = new
      IndexModelBuilder("", Elastic.Schema(JsObject()), Elastic.Config(), false, None, Map.empty)


  /**
    * A trait marking stages of the building.
    */
  sealed trait Stage {}

  /**
    * A trait marking the type of index being built.
    */
  sealed trait DataStoreKind {}

  /**
    * A trait Definining how a schema should augment the IndexModel.
    */
  sealed trait UntypedSchema {
    /**
      * Implementor of Untyped schema should augment the supplied model parameter and return the augmented version.
      *
      * @param model THe model to augment
      * @return The augmented model
      */
    def augment(model: IndexModel): IndexModel
  }

  /**
    * A trait marking schemas as having a target DataStore [[Kind]]
    * @tparam Kind The target datastore Kind
    */
  sealed trait Schema[Kind <: DataStoreKind] extends UntypedSchema

  /**
    * A trait defining how a config should augment the indexModel
    */
  sealed trait UntypedConfig {
    def augment(model: IndexModel): IndexModel
  }

  /**
    * A trait marking configs as having a target DataStore [[Kind]]
    * @tparam Kind The target datastore Kind
    */
  sealed trait Config[Kind <: DataStoreKind] extends UntypedConfig {

  }


  /**
    * Object grouping Elastic customization
    */
  object Elastic {

    /**
      * An elastic Schema.
      *
      * @param jsonSchema The schema as a [[JsValue]]
      */
    case class Schema(jsonSchema: JsValue) extends IndexModelBuilder.Schema[DataStoreKind.Elastic] {

      /**
        * Augments the index model with the serialized json schema as string.
        *
        * @param model THe model to augment
        * @return The augmented model
        */
      override def augment(model: IndexModel): IndexModel = {
        model.copy(schema = Some(jsonSchema.toJson.toString))
      }
    }

    /**
      * An elastic config.
      * @param shards The number of index shards
      * @param replica The number of index replicas
      * @param pushdownQuery The query to use in spark query pushdown
      */
    case class Config(shards: Int = 1, replica: Int = 1, pushdownQuery: Option[String] = None) extends
      IndexModelBuilder.Config[DataStoreKind.Elastic] {
      override def augment(model: IndexModel): IndexModel = {
        model.copy(numShards = Some(shards), replicationFactor = Some(replica), query = pushdownQuery)
      }
    }

    /**
      * Object grouping factories for [[Config]]
      */
    object Config {

      /**
        * A default configuration for [[Elastic]]
        * @return The default configuration.
        */
      def default: Config = Config()
    }

  }

  /**
    * Object grouping Solr customization
    */
  object Solr {

    /**
      * Trait marking SolrMissingFieldSort alternatives
      */
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

    /**
      * A Solr configuration.
      *
      * @param shards The number of index shards
      * @param replica The number of index replicas
      */
    case class Config(shards: Int = 1, replica: Int = 1) extends IndexModelBuilder.Config[DataStoreKind.Solr] {
      override def augment(model: IndexModel): IndexModel = {
        model.copy(numShards = Some(shards), replicationFactor = Some(replica))
      }
    }

    /**
      * A Solr schema.
      * @param solrFields A sequence of [[Solr.Field]]
      */
    case class Schema(solrFields: Solr.Field[_]*) extends IndexModelBuilder.Schema[DataStoreKind.Solr] {

      /**
        * Augments the index model with the serialized json schema as string.
        * @param model The model to augment
        * @return The augmented model
        */
      override def augment(model: IndexModel): IndexModel = {
        import it.agilelab.bigdata.wasp.models.SpraySolrProtocol.fieldSerializer
        model.copy(schema = Some(solrFields.toJson.toString))
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
    case class Field[+A: JsonFormat] private(name: String,
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
                                 large: Option[Boolean] = None) {
      lazy val jsonFormat: JsonFormat[Solr.Field[_]] = {
        import SpraySolrProtocol._
        fieldSerializerNonImplicit[A].asInstanceOf[JsonFormat[Solr.Field[_]]]
      }
    }

    /**
      * objects grouping factories for [[Config]]
      */
    object Config {
      /**
        * A default configuration.
        * @return A default configuration.
        */
      def default: Config = Config()
    }


    /**
      * Object grouping alternatives for [[SolrMissingFieldSort]]
      */
    object SolrMissingFieldSort {

      /**
        * Sort documents having missing field last.
        */
      case object SortMissingLast extends SolrMissingFieldSort

      /**
        * Sort documents having missing field first.
        */
      case object SortMissingFirst extends SolrMissingFieldSort

    }


    /**
      * Object grouping Solr field types, use Custom if you want to use a custom type.
      */
    object Type {

      /**
        * A custom type.
        * @param name     The name of the fieldType for this field. This will be found in the name attribute on the fieldType
        *                 definition. Every field must have a type.
        */
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
      case object TrieDate extends Type[Instant]("pdate")

      /**
        * Double field (64-bit IEEE floating point). precisionStep="0" minimizes index size; precisionStep="8" (the default)
        * enables more efficient range queries. For single valued fields, use docValues="true" for efficient sorting.
        */
      case object TrieDouble extends Type[Double]("pdouble")

      /**
        * Floating point field (32-bit IEEE floating point) . precisionStep="0" enables efficient numeric sorting and
        * minimizes index size; precisionStep="8" (the default) enables efficient range queries. Use docValues="true"
        * for efficient sorting. For single valued fields, use docValues="true" for efficient sorting.
        */
      case object TrieFloat extends Type[Float]("pfloat")

      /**
        * Integer field (32-bit signed integer). precisionStep="0" enables efficient numeric sorting and minimizes index size;
        * precisionStep="8" (the default) enables efficient range queries. For single valued fields, use docValues="true"
        * for efficient sorting.
        */
      case object TrieInt extends Type[Int]("pint")

      /**
        * Long field (64-bit signed integer). precisionStep="0" minimizes index size; precisionStep="8" (the default)
        * enables more efficient range queries. For single valued fields, use docValues="true" for efficient sorting.
        */
      case object TrieLong extends Type[Double]("plong")

    }

  }

  /**
    * Object grouping defined [[DataStoreKind]]
    */
  object DataStoreKind {

    /**
      * Elastic [[DataStoreKind]]
      */
    sealed trait Elastic extends DataStoreKind {}

    /**
      * Solr [[DataStoreKind]]
      */
    sealed trait Solr extends DataStoreKind {}

  }

  /**
    * Objects grouping Builder Stages.
    */
  object Stage {


    /**
      * Describe the target type of [[IndexModelBuilder]] seek before build method can be called.
      * @tparam Kind The [[DataStoreKind]]
      */
    type Complete[Kind <: DataStoreKind] = Name with DataStore[Kind] with Schema[Kind] with Config[Kind]

    /**
      * Index has a name
      */
    sealed trait Name extends Stage

    /**
      * Index has an assigned [[DataStoreKind]].
      *
      * @tparam Kind The kind of [[DataStoreKind]]
      */
    sealed trait DataStore[Kind <: DataStoreKind] extends Stage

    /**
      * Index has an assigned schema for [[DataStoreKind]].
      *
      * @tparam Kind The kind of [[DataStoreKind]]
      */
    sealed trait Schema[Kind <: DataStoreKind] extends Stage

    /**
      * Index has an assigned config for [[DataStoreKind]].
      *
      * @tparam Kind The kind of [[DataStoreKind]]
      */
    sealed trait Config[Kind <: DataStoreKind] extends Stage

    /**
      * Index has been configured as Rolling
      */
    sealed trait Rolling extends Stage

    /**
      * Index has been assigned an Id
      */
    sealed trait Id extends Stage
  
    /**
      * Index has an assigned Options
      */
    sealed trait Options extends Stage

  }

}
