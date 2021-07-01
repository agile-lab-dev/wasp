package it.agilelab.bigdata.wasp.consumers.spark.strategies.cdc.table

case class OrderTable(
    CUST_CODE: Option[String] = None,
    ORDER_DATE: Option[String] = None,
    PRODUCT_CODE: Option[String] = None,
    ORDER_ID: Option[String] = None,
    PRODUCT_PRICE: Option[Double] = None,
    PRODUCT_AMOUNT: Option[Double] = None,
    TRANSACTION_ID: Option[String] = None
)

final case class OrderTableGoldenGate(
    var table: String,
    var op_type: String,
    var op_ts: String,
    var current_ts: String,
    var pos: String,
    var primary_keys: Seq[String],
    var tokens: Map[String, String] = Map(),
    var CUST_CODE: Option[String] = None,
    var ORDER_DATE: Option[String] = None,
    var PRODUCT_CODE: Option[String] = None,
    var ORDER_ID: Option[String] = None,
    var PRODUCT_PRICE: Option[Double] = None,
    var PRODUCT_AMOUNT: Option[Double] = None,
    var TRANSACTION_ID: Option[String] = None
) {
  def this() = this("", "", "", "", "", Seq.empty, Map(), None, None, None, None, None, None, None)
}
