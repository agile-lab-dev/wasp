package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates


/**
 * Model describing a parallel write destination
 * @param writerDetails details needed by writer
 * @param entityDetails entity coordinates needed to retrieve entity endpoints
 */
case class ParallelWriteModel(writerDetails: WriterDetails, entityDetails: CatalogCoordinates)
