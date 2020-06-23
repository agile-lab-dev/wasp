package it.agilelab.bigdata.wasp.master.web.openapi

import java.lang.reflect.Modifier

import io.swagger.v3.oas.models.media.{ObjectSchema, Schema, XML}

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait ProductOpenApi extends OpenApiSchemaSupport {

  private def createObjectSchema[T: ClassTag](ctx: Context,
                                              fieldsSchemas: Array[Schema[_]]
                                             ) = {
    val clazz = implicitly[ClassTag[T]].runtimeClass
    val fieldsName = ProductOpenApi.extractFieldNames(implicitly[ClassTag[T]])
    val objectSchema = new ObjectSchema

    val processed = fieldsSchemas.map{ x =>

      shouldBecomeARef(ctx, x)

    }

    fieldsName.zip(processed).foreach {
      case (key, schema) =>
        objectSchema.addProperties(key, schema)
    }

    import scala.collection.JavaConverters._

    val required  = new java.util.ArrayList(objectSchema.getProperties.asScala.flatMap{
      case (property, schema) if schema.getNullable == null || !schema.getNullable => Seq(property)
      case _ => Seq.empty

    }.toList.asJava)

    if(!required.isEmpty){
      objectSchema.required(required)
    }
    objectSchema.xml(new XML().name(clazz.getSimpleName).namespace("java://" + clazz.getPackage.getName)).name(clazz.getSimpleName)
    objectSchema
  }



  def product1[T: ClassTag, P1 : ToOpenApiSchema](creator: (P1) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx)))
    }
  }

  def product2[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema](creator: (P1,P2) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx)))
    }
  }

  def product3[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema](creator: (P1,P2,P3) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx)))
    }
  }

  def product4[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema](creator: (P1,P2,P3,P4) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx)))
    }
  }

  def product5[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx)))
    }
  }

  def product6[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx)))
    }
  }

  def product7[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx)))
    }
  }

  def product8[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx)))
    }
  }

  def product9[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx)))
    }
  }

  def product10[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx)))
    }
  }

  def product11[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx)))
    }
  }

  def product12[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema,P12 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx),ToOpenApiSchema[P12].schema(ctx)))
    }
  }

  def product13[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema,P12 : ToOpenApiSchema,P13 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx),ToOpenApiSchema[P12].schema(ctx),ToOpenApiSchema[P13].schema(ctx)))
    }
  }

  def product14[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema,P12 : ToOpenApiSchema,P13 : ToOpenApiSchema,P14 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx),ToOpenApiSchema[P12].schema(ctx),ToOpenApiSchema[P13].schema(ctx),ToOpenApiSchema[P14].schema(ctx)))
    }
  }

  def product15[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema,P12 : ToOpenApiSchema,P13 : ToOpenApiSchema,P14 : ToOpenApiSchema,P15 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14,P15) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx),ToOpenApiSchema[P12].schema(ctx),ToOpenApiSchema[P13].schema(ctx),ToOpenApiSchema[P14].schema(ctx),ToOpenApiSchema[P15].schema(ctx)))
    }
  }

  def product16[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema,P12 : ToOpenApiSchema,P13 : ToOpenApiSchema,P14 : ToOpenApiSchema,P15 : ToOpenApiSchema,P16 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14,P15,P16) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx),ToOpenApiSchema[P12].schema(ctx),ToOpenApiSchema[P13].schema(ctx),ToOpenApiSchema[P14].schema(ctx),ToOpenApiSchema[P15].schema(ctx),ToOpenApiSchema[P16].schema(ctx)))
    }
  }

  def product17[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema,P12 : ToOpenApiSchema,P13 : ToOpenApiSchema,P14 : ToOpenApiSchema,P15 : ToOpenApiSchema,P16 : ToOpenApiSchema,P17 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14,P15,P16,P17) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx),ToOpenApiSchema[P12].schema(ctx),ToOpenApiSchema[P13].schema(ctx),ToOpenApiSchema[P14].schema(ctx),ToOpenApiSchema[P15].schema(ctx),ToOpenApiSchema[P16].schema(ctx),ToOpenApiSchema[P17].schema(ctx)))
    }
  }

  def product18[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema,P12 : ToOpenApiSchema,P13 : ToOpenApiSchema,P14 : ToOpenApiSchema,P15 : ToOpenApiSchema,P16 : ToOpenApiSchema,P17 : ToOpenApiSchema,P18 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14,P15,P16,P17,P18) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx),ToOpenApiSchema[P12].schema(ctx),ToOpenApiSchema[P13].schema(ctx),ToOpenApiSchema[P14].schema(ctx),ToOpenApiSchema[P15].schema(ctx),ToOpenApiSchema[P16].schema(ctx),ToOpenApiSchema[P17].schema(ctx),ToOpenApiSchema[P18].schema(ctx)))
    }
  }

  def product19[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema,P12 : ToOpenApiSchema,P13 : ToOpenApiSchema,P14 : ToOpenApiSchema,P15 : ToOpenApiSchema,P16 : ToOpenApiSchema,P17 : ToOpenApiSchema,P18 : ToOpenApiSchema,P19 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14,P15,P16,P17,P18,P19) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx),ToOpenApiSchema[P12].schema(ctx),ToOpenApiSchema[P13].schema(ctx),ToOpenApiSchema[P14].schema(ctx),ToOpenApiSchema[P15].schema(ctx),ToOpenApiSchema[P16].schema(ctx),ToOpenApiSchema[P17].schema(ctx),ToOpenApiSchema[P18].schema(ctx),ToOpenApiSchema[P19].schema(ctx)))
    }
  }

  def product20[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema,P12 : ToOpenApiSchema,P13 : ToOpenApiSchema,P14 : ToOpenApiSchema,P15 : ToOpenApiSchema,P16 : ToOpenApiSchema,P17 : ToOpenApiSchema,P18 : ToOpenApiSchema,P19 : ToOpenApiSchema,P20 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14,P15,P16,P17,P18,P19,P20) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx),ToOpenApiSchema[P12].schema(ctx),ToOpenApiSchema[P13].schema(ctx),ToOpenApiSchema[P14].schema(ctx),ToOpenApiSchema[P15].schema(ctx),ToOpenApiSchema[P16].schema(ctx),ToOpenApiSchema[P17].schema(ctx),ToOpenApiSchema[P18].schema(ctx),ToOpenApiSchema[P19].schema(ctx),ToOpenApiSchema[P20].schema(ctx)))
    }
  }

  def product21[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema,P12 : ToOpenApiSchema,P13 : ToOpenApiSchema,P14 : ToOpenApiSchema,P15 : ToOpenApiSchema,P16 : ToOpenApiSchema,P17 : ToOpenApiSchema,P18 : ToOpenApiSchema,P19 : ToOpenApiSchema,P20 : ToOpenApiSchema,P21 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14,P15,P16,P17,P18,P19,P20,P21) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx),ToOpenApiSchema[P12].schema(ctx),ToOpenApiSchema[P13].schema(ctx),ToOpenApiSchema[P14].schema(ctx),ToOpenApiSchema[P15].schema(ctx),ToOpenApiSchema[P16].schema(ctx),ToOpenApiSchema[P17].schema(ctx),ToOpenApiSchema[P18].schema(ctx),ToOpenApiSchema[P19].schema(ctx),ToOpenApiSchema[P20].schema(ctx),ToOpenApiSchema[P21].schema(ctx)))
    }
  }

  def product22[T: ClassTag, P1 : ToOpenApiSchema,P2 : ToOpenApiSchema,P3 : ToOpenApiSchema,P4 : ToOpenApiSchema,P5 : ToOpenApiSchema,P6 : ToOpenApiSchema,P7 : ToOpenApiSchema,P8 : ToOpenApiSchema,P9 : ToOpenApiSchema,P10 : ToOpenApiSchema,P11 : ToOpenApiSchema,P12 : ToOpenApiSchema,P13 : ToOpenApiSchema,P14 : ToOpenApiSchema,P15 : ToOpenApiSchema,P16 : ToOpenApiSchema,P17 : ToOpenApiSchema,P18 : ToOpenApiSchema,P19 : ToOpenApiSchema,P20 : ToOpenApiSchema,P21 : ToOpenApiSchema,P22 : ToOpenApiSchema](creator: (P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14,P15,P16,P17,P18,P19,P20,P21,P22) => T): ToOpenApiSchema[T] = new ToOpenApiSchema[T] {
    override def schema(ctx:Context): Schema[_] = {
      createObjectSchema(ctx, Array(ToOpenApiSchema[P1].schema(ctx),ToOpenApiSchema[P2].schema(ctx),ToOpenApiSchema[P3].schema(ctx),ToOpenApiSchema[P4].schema(ctx),ToOpenApiSchema[P5].schema(ctx),ToOpenApiSchema[P6].schema(ctx),ToOpenApiSchema[P7].schema(ctx),ToOpenApiSchema[P8].schema(ctx),ToOpenApiSchema[P9].schema(ctx),ToOpenApiSchema[P10].schema(ctx),ToOpenApiSchema[P11].schema(ctx),ToOpenApiSchema[P12].schema(ctx),ToOpenApiSchema[P13].schema(ctx),ToOpenApiSchema[P14].schema(ctx),ToOpenApiSchema[P15].schema(ctx),ToOpenApiSchema[P16].schema(ctx),ToOpenApiSchema[P17].schema(ctx),ToOpenApiSchema[P18].schema(ctx),ToOpenApiSchema[P19].schema(ctx),ToOpenApiSchema[P20].schema(ctx),ToOpenApiSchema[P21].schema(ctx),ToOpenApiSchema[P22].schema(ctx)))
    }
  }

}

object ProductOpenApi extends ProductOpenApi {
  private[openapi] def extractFieldNames(classManifest: ClassManifest[_]): Array[String] = {
    val clazz = classManifest.erasure
    try {
      val fields = clazz.getDeclaredFields.filterNot { f =>
        import Modifier._
        (f.getModifiers & (TRANSIENT | STATIC | 0x1000 /* SYNTHETIC*/ )) > 0
      }
      fields.map(f => unmangle(f.getName))
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException(
          "Cannot automatically determine case class field names and order " +
            "for '" + clazz.getName + "', please use the 'productFormat' overload with explicit field name specification",
          ex
        )
    }
  }

  private def unmangle(name: String) = {
    import java.lang.{StringBuilder => JStringBuilder}
    @tailrec def rec(ix: Int, builder: JStringBuilder): String = {
      val rem = name.length - ix
      if (rem > 0) {
        var ch = name.charAt(ix)
        var ni = ix + 1
        val sb = if (ch == '$' && rem > 1) {
          def c(offset: Int, ch: Char) = name.charAt(ix + offset) == ch
          ni = name.charAt(ix + 1) match {
            case 'a' if rem > 3 && c(2, 'm') && c(3, 'p') => {
              ch = '&'; ix + 4
            }
            case 'a' if rem > 2 && c(2, 't') => { ch = '@'; ix + 3 }
            case 'b' if rem > 4 && c(2, 'a') && c(3, 'n') && c(4, 'g') => {
              ch = '!'; ix + 5
            }
            case 'b' if rem > 3 && c(2, 'a') && c(3, 'r') => {
              ch = '|'; ix + 4
            }
            case 'd' if rem > 3 && c(2, 'i') && c(3, 'v') => {
              ch = '/'; ix + 4
            }
            case 'e' if rem > 2 && c(2, 'q') => { ch = '='; ix + 3 }
            case 'g'
              if rem > 7 && c(2, 'r') && c(3, 'e') && c(4, 'a') && c(5, 't') && c(
                6,
                'e'
              ) && c(7, 'r') => { ch = '>'; ix + 8 }
            case 'h' if rem > 4 && c(2, 'a') && c(3, 's') && c(4, 'h') => {
              ch = '#'; ix + 5
            }
            case 'l' if rem > 4 && c(2, 'e') && c(3, 's') && c(4, 's') => {
              ch = '<'; ix + 5
            }
            case 'm'
              if rem > 5 && c(2, 'i') && c(3, 'n') && c(4, 'u') && c(
                5,
                's'
              ) => { ch = '-'; ix + 6 }
            case 'p'
              if rem > 7 && c(2, 'e') && c(3, 'r') && c(4, 'c') && c(5, 'e') && c(
                6,
                'n'
              ) && c(7, 't') => { ch = '%'; ix + 8 }
            case 'p' if rem > 4 && c(2, 'l') && c(3, 'u') && c(4, 's') => {
              ch = '+'; ix + 5
            }
            case 'q'
              if rem > 5 && c(2, 'm') && c(3, 'a') && c(4, 'r') && c(
                5,
                'k'
              ) => { ch = '?'; ix + 6 }
            case 't'
              if rem > 5 && c(2, 'i') && c(3, 'l') && c(4, 'd') && c(
                5,
                'e'
              ) => { ch = '~'; ix + 6 }
            case 't'
              if rem > 5 && c(2, 'i') && c(3, 'm') && c(4, 'e') && c(
                5,
                's'
              )                            => { ch = '*'; ix + 6 }
            case 'u' if rem > 2 && c(2, 'p') => { ch = '^'; ix + 3 }
            case 'u' if rem > 5 =>
              def hexValue(offset: Int): Int = {
                val c = name.charAt(ix + offset)
                if ('0' <= c && c <= '9') c - '0'
                else if ('a' <= c && c <= 'f') c - 87
                else if ('A' <= c && c <= 'F') c - 55
                else -0xFFFF
              }
              val ci = (hexValue(2) << 12) + (hexValue(3) << 8) + (hexValue(4) << 4) + hexValue(
                5
              )
              if (ci >= 0) { ch = ci.toChar; ix + 6 } else ni
            case _ => ni
          }
          if (ni > ix + 1 && builder == null)
            new JStringBuilder(name.substring(0, ix))
          else builder
        } else builder
        rec(ni, if (sb != null) sb.append(ch) else null)
      } else if (builder != null) builder.toString
      else name
    }
    rec(0, null)
  }

}

