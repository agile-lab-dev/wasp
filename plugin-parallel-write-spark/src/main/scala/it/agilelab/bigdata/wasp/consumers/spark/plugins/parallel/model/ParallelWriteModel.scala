package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model

import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct, GenericProduct}
import it.agilelab.bigdata.wasp.models.DatastoreModel


/**
 * Model describing a parallel write destination
 * @param writerDetails details needed by writer
 * @param entityDetails entity coordinates needed to retrieve entity endpoints
 */
case class ParallelWriteModel(writerDetails: WriterDetails, entityDetails: Map[String, String])
