package it.agilelab.bigdata.wasp.master.web.openapi

/**
  * This class generates product functions variants to autogenerate OpenApiSchema typeclasses for case classes,
  * the output of this class should be copy pasted in [[ProductOpenApi]]
  */
object GenerateProductFunctions {

  def main(args: Array[String]): Unit = {

    Seq
      .range(1, 23)
      .map { index =>
        val typeParametersWithContextBound =
          Seq
            .range(1, index + 1)
            .map(x => s"P$x : ToOpenApiSchema")
            .mkString(",")
        val lambdaTypes =
          Seq.range(1, index + 1).map(x => s"P$x").mkString("(", ",", ")")
        val schemas =
          Seq
            .range(1, index + 1)
            .map(x => s"ToOpenApiSchema[P$x].schema(ctx)")
            .mkString(",")

        s"""
         |def product$index[T: ClassTag, $typeParametersWithContextBound](creator: $lambdaTypes => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
         |    override def schema(ctx:Context): Schema[_] = {
         |      createObjectSchema(ctx, Array($schemas))
         |    }
         |  }
         |""".stripMargin
      }
      .foreach(println)

  }
}
