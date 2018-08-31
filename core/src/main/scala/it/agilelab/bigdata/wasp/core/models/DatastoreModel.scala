package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.datastores.DatastoreCategory

/**
	* Base datastore model.
	*
	* @author Nicolò Bidotti
	*/
abstract class DatastoreModel[DSC <: DatastoreCategory] extends Model
