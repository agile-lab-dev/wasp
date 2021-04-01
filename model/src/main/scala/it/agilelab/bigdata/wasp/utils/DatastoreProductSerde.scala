package it.agilelab.bigdata.wasp.utils

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct

/**
	* Base trait for `DataStoreProduct` serde.
	*
	* @author Nicolò Bidotti
	*/
trait DatastoreProductSerde {
	// field names
	protected val categoryField = "category"
	protected val productField = "product"
}
