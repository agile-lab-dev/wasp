package it.agilelab.bigdata.wasp.models

/**
  * Object used to represents all the fields used to represent a generic
  * mutation inside the cdcPlugin, this object has been placed here because
  * all the cdc adapters (like debezium, goldengate etc etc...) need to know
  * how to map the fields into a compliant dataframe.
  */
object GenericCdcMutationFields {

  val AFTER_IMAGE  = "afterImage"
  val BEFORE_IMAGE = "beforeImage"
  val TIMESTAMP    = "timestamp"
  val COMMIT_ID    = "commitId"
  val TYPE         = "type"
  val PRIMARY_KEY  = "key"

}
