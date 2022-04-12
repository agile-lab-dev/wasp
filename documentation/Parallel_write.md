# Parallel write integration
## Configure WASP project to support parallel write
1. Add `wasp-plugin-parallel-write-kafka` dependency to your project. 
2. Create a class extending `EntityCatalogConfiguration` 
	1. Implement method `getEntity(catalogCoordinates: CatalogCoordinates): ParallelWriteEntity` to define how to retrieve parallel write entities from catalog 
	2. Implement method `def getEntityTableName(coordinates: CatalogCoordinates): String` to define how to build entity table name to be found in metastore catalog according to metastore catalog table naming convention
3. Add WASP configuration to instantiate via reflection the entity catalog service implementation
```ini
	plugin {  
	  microservice-catalog {  
	    catalog-class: "full.classpath.to.entityCatalogConfiguration"  
	  }  
	}
```

## Configure Pipegraph's ETL sinks to write on parallel write 

An entity should be defined in both entity catalog and metastore catalog by some coordinates:
```scala
case class CatalogCoordinates(
	domain: String, 
	name: String, 
	version: String, 
	dbPrefix: Option[String] = None, 
	overrideDbName: Option[String] = None
)
```

Add a parallel write generic model to Pipegraph's ETL sink, referencing the target entity through its CatalogCoordinates. 

At the moment, two types of Parallel Write entity use cases are implementable. 
- Parallel Write
- Continuous Update

### Parallel Write 
The model is defined as: 
```scala
val parallelWriteModel = GenericModel(  
  name = "parallelwriteExampleModel",  
  value = ParallelWriteModelSerde.serialize(  
		ParallelWriteModel(  
		    ParallelWrite("append"),  
			CatalogCoordinates(  
		        "domain",  
				"entity_name",  
				"v1"
			)  
	    )
	)  
  ),  
 product = GenericProduct("parallelWrite", None)  
)
```

Parallel write constructor requires a saveMode parameter, referring to [Spark savemode](https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/SaveMode.html): 
```scala 
/**  
 * Details needeed by parallel writer 
  @param saveMode spark save mode  
 */
 case class ParallelWrite(saveMode: String) extends WriterDetails
```

### Continuous Update
Continuous update use cases leverages [Delta Lake](https://docs.delta.io/latest/delta-intro.html) for what concerns Cold Parallel Write cases. 

The model is defined as: 
```scala
val continuousUpdateModel = GenericModel(  
  name = "delta-parallelwrite",  
  value = ParallelWriteModelSerde.serialize(  
		ParallelWriteModel(  
        ContinuousUpdate(  
	        List("id"),  
			"number"  
		),  
		CatalogCoordinates(  
			"domain",  
			"name",  
			"v1"  
		)  
	)  
  ),  
 product = GenericProduct("parallelWrite", None)  
)
```

ContinuouUpdate constructor requires two parameters:
- the list of delta table unique keys 
- monotonically increasing select expression to choose upsert candidate

```scala
/**  
 * Details needed by continuous update writer 
 * @param keys delta table unique keys column list  
 * @param orderingExpression monotonically increasing select expression to choose upsert candidate  
 */
case class ContinuousUpdate(keys: List[String], 
							orderingExpression: String
						) extends WriterDetails
```