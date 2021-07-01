package it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.spark.sql.{DataFrame, Encoder}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

/**
  * Case class representing a Oracle row mutation.
  * You can map the mutations in that object by calling
  * [[it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc.GoldenGateMutationUtils#mapIntoCaseClass(org.apache.spark.sql.Dataset, org.apache.spark.sql.Encoder)]]
  *
  * @tparam A the class representing the structure of the table, nb the fields name are used by the encoder, so the must correspond to the names used in the mutation
  */
final case class TableMutationFlatModel[A](
    table: String,
    op_type: String,
    op_ts: String,
    current_ts: String,
    pos: String,
    primary_keys: Seq[String],
    tokens: Map[String, String] = Map(),
    innerTable: A
) {

  def extractInnerModel(): A = innerTable

  def apply[T](
      table: String,
      op_type: String,
      op_ts: String,
      current_ts: String,
      pos: String,
      primary_keys: Seq[String],
      tokens: Map[String, String],
      innerTable: T
  )(implicit evidence: Encoder[T]): TableMutationFlatModel[T] =
    TableMutationFlatModel(table, op_type, op_ts, current_ts, pos, primary_keys, tokens, innerTable)

  def map(f: A => A): TableMutationFlatModel[A] = {
    TableMutationFlatModel(table, op_type, op_ts, current_ts, pos, primary_keys, tokens, f(innerTable))
  }

  def flatMap[B <: Product](f: TableMutationFlatModel[A] => TableMutationFlatModel[B]): TableMutationFlatModel[B] =
    f(this)
}

/**
  * object containing non trivial function utils in the case of cdc goldengate mutations.
  * Avoid the call of [[it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc.GoldenGateMutationUtils#mapIntoCaseClass(org.apache.spark.sql.Dataset, org.apache.spark.sql.Encoder)]]
  * and [[it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc.GoldenGateMutationUtils#extractTableDataset(org.apache.spark.sql.Dataset, org.apache.spark.sql.Encoder)]] if not
  * mandatory because they add an additional cost by inserting a mapping phase in the spark pipeline.
  */
object GoldenGateMutationUtils {

  import org.apache.spark.sql.{Dataset, Encoder}

  import scala.language.higherKinds

  def ggMandatoryFields(): Set[String] =
    Set[String]("table", "op_type", "op_ts", "current_ts", "pos", "primary_keys", "tokens")

  def extractTableFields(df: DataFrame): List[String] = df.columns.toSet.diff(ggMandatoryFields()).toList

  def extractGGDataframe(df: DataFrame): DataFrame = df.selectExpr(ggMandatoryFields().toList: _*)

  def extractTableDataframe(df: DataFrame): DataFrame = df.selectExpr(extractTableFields(df): _*)

  def extractTableDataset[A: Encoder](df: DataFrame): Dataset[A] = df.selectExpr(extractTableFields(df): _*).as[A]

  def mapIntoCaseClass[A](
      df: DataFrame
  )(implicit evidence: Encoder[TableMutationFlatModel[A]]): Dataset[TableMutationFlatModel[A]] = {
    import org.apache.spark.sql.functions.struct

    val fields = GoldenGateMutationUtils.extractTableFields(df)

    df.withColumn(
        "innerTable",
        struct(fields.head, fields.tail: _*)
      )
      .as[TableMutationFlatModel[A]]
  }
}

/**
  * Names used by goldengate to name the various operations.
  */
private[cdc] object GoldengateOperations extends Operation {
  override def insert: OperationType = "I"

  override def update: OperationType = "U"

  override def delete: OperationType = "D"

  override def truncate: OperationType = "T"
}

/**
  * Strategy that enable to map a flat mutation model to be mapped to an insert/update/delete
  * object that can be sent to the CDC plugin that writes on DeltaLake.
  * So having has input the raw flat mutations coming from a goldengate topic
  * it will produce in output a dataframe composed of rows
  * that has the shape accepted in input by the cdc plugin.
  *
  * NB:
  *    - this strategy is used to map the mutation incoming from what in the
  *    oracle language is known as: Row Formatter, if you need to map a message
  *    that is incoming from an Operation Formatter you need to wait the new
  *    feature for that. More details are available under:
  *    [[https://docs.oracle.com/goldengate/bd1221/gg-bd/GADBD/GUID-F0FA2781-0802-4530-B1F0-5E102B982EC0.htm#GADBD481 operation vs row formatter]]
  *
  *    - to enable the correct working of the strategy you need to ensure at
  * runtime the configuration with path: goldengate.key.fields.
  * This configuration is required and contains the list of primary keys fields
  * for the mutation table. Suppose for example to have a table with the following structure:
  *        SHOP_TABLE ===>
  *             "PRODUCT_AMOUNT": Integer
  *             "TRANSACTION_ID": Integer
  *             "ORDER_DATE": Timestamp
  *             "PRODUCT_PRICE": Char
  *             "ORDER_ID": Integer
  *             "CUST_CODE": Long
  *             "PRODUCT_CODE": String
  * and the primary key of this table is composed by the fields:
  *  - CUST_CODE
  *  - ORDER_DATE
  *  - PRODUCT_CODE
  *  - ORDER_ID
  *
  * in this case you need to insert the configuration the following line:
  *
  * goldengate.key.fields=["CUST_CODE", "ORDER_DATE", "PRODUCT_CODE", "ORDER_ID"]"
  *
  */
class GoldenGateAdapterFlatModelStrategy extends Strategy with Logging {

  /**
    * Eventual preparation function of the initial DF
    * an example can be the removal of fields that are not required to be mapped
    * in the table or a mapping with a default value.
    *
    * @return final output DF
    */
  def prepareInitialDf: DataFrame => DataFrame = (df: DataFrame) => df

  /**
    * Eventual enrichment function of the final DF.
    *
    * @return final output DF
    */
  def enrichFinalDf: DataFrame => DataFrame = (df: DataFrame) => df

  /**
    * Strategy that read data from Kafka, transform the format
    * to make it compliant with the DataLake format that the writer expect.
    *
    * @param dataFrames the dataframe that need to be transformed
    * @return a dataframe transformed that can be sent to the CDCWriter
    */
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {
    import scala.util.{Failure, Success, Try}

    logger.info(s"Strategy configuration: $configuration")

    val df = prepareInitialDf(dataFrames.head._2)

    val ggFields = GoldenGateMutationUtils.ggMandatoryFields()

    if (!df.columns.toSet.intersect(ggFields).equals(ggFields)) {
      throw new IllegalStateException(
        s"error, to use the GoldenGateAdapterStrategy the dataframe " +
          s"is expected to have the required goldengate fields: ${ggFields.mkString("[", " ,", "]")}"
      )
    }

    val configKey = "goldengate.key.fields"
    val primaryKeys: Seq[String] = Try(configuration.getStringList(configKey)) match {
      case Failure(err) =>
        throw new IllegalStateException(
          s"the configuration $configKey is not present, cannot start " +
            s"the Strategy due unability to extract the primary key for the mutation",
          err.getCause
        )
      case Success(keys) => keys.asScala.toSeq
    }

    val cdcDf = new GoldenGateConversion {
      override protected def conversionToCdcFormat(dataFrame: DataFrame): DataFrame = {

        conversionToCdcFormat(df, primaryKeys)
      }
    }.conversion(
      df
    )

    enrichFinalDf(cdcDf)
  }

}


/**
  * Implementation of the conversion to provide a compliant Dataframe for the
  * [[GoldenGateConversion#conversion]].
  * The implementation must be compliant with the Goldengate documentation:
  * [[https://docs.oracle.com/goldengate/bd1221/gg-bd/GADBD/GUID-F0FA2781-0802-4530-B1F0-5E102B982EC0.htm#GADBD479" Goldengate Doc]]
  *
  * NB: reason about the changes of the primary key. See in particular the configuration key called gg.handler.name.format.pkUpdateHandlingformat.pkUpdateHandling
  * in the page [[https://docs.oracle.com/goldengate/bd1221/gg-bd/GADBD/GUID-F0FA2781-0802-4530-B1F0-5E102B982EC0.htm#GADBD482 gg docs]]
  *
  * the configuration gg.handler.name.format.includePrimaryKeys is required to be set to true.
  */
trait GoldenGateConversion extends CdcMapper {

  import org.apache.spark.sql.functions.{col, lit}

  def operations: Operation = GoldengateOperations

  override def BEFORE: String = "before"

  override def AFTER: String = "after"

  override def OPERATION: String = "op_type"

  // the field op_ts play the role of monotonic timestamp
  override def TIMESTAMP: String = "op_ts"

  // the configuration gg.handler.name.format.includePosition must be set to true,
  // it represent the position in the trail file.
  override def COMMIT_ID: String = "pos"

  override def PRIMARY_KEY: String = "primary_keys"

  private val usedFields = Set(BEFORE, AFTER, TIMESTAMP, COMMIT_ID)

  private val unusedFields: List[String] = GoldenGateMutationUtils
    .ggMandatoryFields()
    .diff(usedFields)
    .toList

  val row_number = "row_number"

  def insertMappingFunction(df: DataFrame, keys: Seq[String]): DataFrame = {
    import org.apache.spark.sql.functions.{struct, when}
    // assuming that, since the schema is the same for all the entries in the dataset
    // the primary key will be always composed the same set of fields

    val fields = GoldenGateMutationUtils.extractTableFields(df)

    df.drop(unusedFields: _*)
      .withColumn(
        AFTER,
        when(struct(fields.head, fields.tail: _*).isNotNull, struct(fields.head, fields.tail: _*))
          .otherwise(lit(null))
      )
      .withColumn(PRIMARY_KEY, struct(keys.head, keys.tail: _*))
      .drop(fields: _*)
      .withColumn(BEFORE, lit(null).cast(df.select(fields.head, fields.tail: _*).schema))
  }

  def updateMappingFunction(df: DataFrame, keys: Seq[String]): DataFrame = insertMappingFunction(df, keys)

  def deleteMappingFunction(df: DataFrame, keys: Seq[String]): DataFrame =
    insertMappingFunction(df, keys)
      .select(
        col(TIMESTAMP),
        col(COMMIT_ID),
        col(BEFORE).as(AFTER),
        col(PRIMARY_KEY),
        col(AFTER).as(BEFORE)
      )

  def truncateMappingFunction(df: DataFrame, keys: Seq[String]): DataFrame = {
    import org.apache.spark.sql.{Dataset, Row}
    import org.apache.spark.sql.catalyst.encoders.RowEncoder

    val oldSchema = df.schema

    val failIfNotEmpty: Dataset[Row] = df.map { row =>
      throw new IllegalStateException("Cannot handle truncate operation")
      row
    }(RowEncoder(oldSchema))

    // the insertMappingFunction is called to use the dataframe that pass through the mapping
    // function that has only the scope of fail if at least a  record is present, this is an
    // ugly workaround to fail if the method is called on an rdd non empty without calling an
    // action on an rdd. Consider also that the insertMappingFunction if called on an empty rdd
    // has the only side-effect of changing the shape of the rdd to enable the union with the others
    insertMappingFunction(failIfNotEmpty, keys)
  }

  protected def conversionToCdcFormat(df: DataFrame, keys: Seq[String]): DataFrame = {
    /*   TODO: implement the truncate
         now are impossible to implement now because the DeltaWriter requires the values of the primary keys
         and now are not available.
         a solution could be generate the examples and configure the Goldengate by insert the before state
         as follow: https://docs.oracle.com/goldengate/1212/gg-winux/GWURF/gg_parameters077.htm#GWURF515
         another solution is to adapt the DeltaLakeWriter to write delete, update and truncate even if
         the before key is not set correctly
         i cannot do the code for testing the truncate because the example with the keys isn't present
     */

    import org.apache.spark.sql.functions._

    Seq(
      insertMappingFunction(df.where(col(OPERATION).equalTo(operations.insert)), keys)
        .withColumn(OPERATION, lit("insert")),
      updateMappingFunction(df.where(col(OPERATION).equalTo(operations.update)), keys)
        .withColumn(OPERATION, lit("update")),
      deleteMappingFunction(df.where(col(OPERATION).equalTo(operations.delete)), keys)
        .withColumn(OPERATION, lit("delete")),
      truncateMappingFunction(df.where(col(OPERATION).equalTo(operations.truncate)), keys)
        .withColumn(OPERATION, lit("delete"))
    ).reduce(_.union(_))
  }
}
