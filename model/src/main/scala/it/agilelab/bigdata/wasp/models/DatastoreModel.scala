package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct}

/**
	* Base datastore model.
	*
	* @author Nicolò Bidotti
	*/
abstract class DatastoreModel extends Model {
	def datastoreProduct: DatastoreProduct
}

