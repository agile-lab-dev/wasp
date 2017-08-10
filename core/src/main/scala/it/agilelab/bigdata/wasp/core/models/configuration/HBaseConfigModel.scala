package it.agilelab.bigdata.wasp.core.models.configuration

/**
	* Configuration model for HBase.
	*
	* @author Nicolò Bidotti
	*/
case class HBaseConfigModel(
	                          coreSiteXmlPath: String,
                            hbaseSiteXmlPath: String
                           )