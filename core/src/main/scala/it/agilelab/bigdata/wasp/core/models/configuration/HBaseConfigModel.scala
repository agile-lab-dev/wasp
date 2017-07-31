package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model

/**
	* Configuration model for HBase.
	*
	*/
case class HBaseConfigModel(
														 coreSiteXmlPath: String,
														 hbaseSiteXmlPath: String,
														 name: String
                           ) extends Model