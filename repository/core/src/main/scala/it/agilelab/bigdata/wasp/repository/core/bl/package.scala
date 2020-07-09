package it.agilelab.bigdata.wasp.repository.core

/**
	* Type aliases for commonly used structural types built with the BLs. These will also help when migrating away from
	* structural typing.
	*
	* @author Nicolò Bidotti
	*/
package object bl {
	type DatastoreModelBLs = {
		val indexBL: IndexBL
		val topicBL: TopicBL
		val rawBL: RawBL
		val keyValueBL: KeyValueBL
	}
}
