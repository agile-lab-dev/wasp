package it.agilelab.bigdata.wasp.master.web.controllers
import akka.http.scaladsl.testkit.ScalatestRouteTest
import it.agilelab.bigdata.wasp.core.utils.FreeCodeCompilerUtils
import it.agilelab.bigdata.wasp.master.web.controllers.FreeCodeDataSupport._
import it.agilelab.bigdata.wasp.models.{CompletionModel, ErrorModel, FreeCodeModel}
import it.agilelab.bigdata.wasp.utils.JsonSupport
import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsonFormat, RootJsonFormat}



class FreeCodeControllerSpec extends FlatSpec with ScalatestRouteTest with Matchers with JsonSupport {

  case class AngularResponse[T](Result: String, data: T)
  case class AngularKoReponse[T](Result : String, ErrorMsg : T)
  implicit def angularResponse[T: JsonFormat]: RootJsonFormat[AngularResponse[T]] = jsonFormat2(AngularResponse.apply[T])

  implicit def angularKoResponseKo[T: JsonFormat]: RootJsonFormat[AngularKoReponse[T]] = jsonFormat2(AngularKoReponse.apply[T])


  it should "test insert correct model" in {

    val serviceDB =  new FreeCodeDBServiceMock
    val freeCodeCompiler = new FreeCodeCompilerUtilsMock(false,false)
    val controller = new FreeCodeController(serviceDB,freeCodeCompiler)

   Post("/freeCode", freeCodeModelCorrect) ~> controller.getRoute ~> check {
     serviceDB.storage.size shouldBe 2
     val output  = serviceDB.storage.find(_.name.equals(freeCodeModelCorrect.name))
       output.isDefined shouldBe true
     output.get shouldBe freeCodeModelCorrect
     responseAs[AngularResponse[String]] shouldEqual AngularResponse("OK", "OK")
   }
 }



  it should "test insert warning model" in {

    val serviceDB =  new FreeCodeDBServiceMock
    val freeCodeCompiler = new FreeCodeCompilerUtilsMock(false,true)
    val controller = new FreeCodeController(serviceDB,freeCodeCompiler)
    serviceDB.storage.size shouldBe 1
    serviceDB.storage.exists(_.name.equals(freeCodeModelWarning.name)) shouldBe false
    Post("/freeCode", freeCodeModelWarning) ~> controller.getRoute ~> check {
      serviceDB.storage.size shouldBe 2
      val output  = serviceDB.storage.find(_.name.equals(freeCodeModelWarning.name))
      output.isDefined shouldBe true
      output.get shouldBe freeCodeModelWarning
      val models = responseAs[AngularResponse[List[ErrorModel]]]
      models.data.size shouldBe 1
      models.data.head.errorType shouldBe "warning"
      models.Result shouldBe "OK"
    }
  }




  it should "test insert wrong model" in {

    val serviceDB =  new FreeCodeDBServiceMock
    val freeCodeCompiler = new FreeCodeCompilerUtilsMock(true,true)
    val controller = new FreeCodeController(serviceDB,freeCodeCompiler)
    serviceDB.storage.size shouldBe 1
    serviceDB.storage.exists(_.name.equals(freeCodeModelError.name)) shouldBe false
    Post("/freeCode", freeCodeModelError) ~> controller.getRoute ~> check {
      serviceDB.storage.size shouldBe 1
      val output  = serviceDB.storage.find(_.name.equals(freeCodeModelError.name))
      output.isDefined shouldBe false
      val models = responseAs[AngularResponse[List[ErrorModel]]]
      models.data.size shouldBe 1
      models.data.head.errorType shouldBe "error"
      models.Result shouldBe "KO"
    }
  }

  it should "test get all" in {
    val serviceDB =  new FreeCodeDBServiceMock
    val freeCodeCompiler = new FreeCodeCompilerUtilsMock(false,false)
    val controller = new FreeCodeController(serviceDB,freeCodeCompiler)
    serviceDB.storage.size shouldBe 1
    serviceDB.storage.exists(_.name.equals(freeCodeModelDefault.name)) shouldBe true

    Get("/freeCode") ~> controller.getRoute ~> check {
      val models = responseAs[AngularResponse[List[FreeCodeModel]]]
      models.data.size shouldBe 1
      models.data.head shouldBe freeCodeModelDefault
      models.Result shouldBe "OK"
    }

    serviceDB.storage = List(freeCodeModelDefault,freeCodeModelError,freeCodeModelWarning,freeCodeModelCorrect)
    Get("/freeCode") ~> controller.getRoute ~> check {
      val models = responseAs[AngularResponse[List[FreeCodeModel]]]
      models.data.size shouldBe 4
      models.data should contain theSameElementsAs  serviceDB.storage
      models.Result shouldBe "OK"
    }


    serviceDB.storage = List.empty
    Get("/freeCode") ~> controller.getRoute ~> check {
      val models = responseAs[AngularResponse[List[FreeCodeModel]]]
      models.data.size shouldBe 0
      models.Result shouldBe "OK"
    }

  }


  it should "test get" in {
    val serviceDB =  new FreeCodeDBServiceMock
    val freeCodeCompiler = new FreeCodeCompilerUtilsMock(false,false)
    val controller = new FreeCodeController(serviceDB,freeCodeCompiler)
    serviceDB.storage.size shouldBe 1
    serviceDB.storage.exists(_.name.equals(freeCodeModelDefault.name)) shouldBe true

    Get(s"/freeCode/instance/${freeCodeModelDefault.name}") ~> controller.getRoute ~> check {
      val models = responseAs[AngularResponse[FreeCodeModel]]
      models.data shouldBe freeCodeModelDefault
    }

    //get a model thant doesn't exist
    Get(s"/freeCode/instance/${freeCodeModelError.name}") ~> controller.getRoute ~> check {
      val models = responseAs[AngularKoReponse[String]]
      models.Result shouldBe "KO"
    }



    serviceDB.storage = List(freeCodeModelDefault,freeCodeModelError,freeCodeModelWarning,freeCodeModelCorrect)
    Get(s"/freeCode/instance/${freeCodeModelDefault.name}") ~> controller.getRoute ~> check {
      val models = responseAs[AngularResponse[FreeCodeModel]]
      models.data shouldBe freeCodeModelDefault
    }

    Get(s"/freeCode/instance/${freeCodeModelError.name}") ~> controller.getRoute ~> check {
      val models = responseAs[AngularResponse[FreeCodeModel]]
      models.data shouldBe freeCodeModelError
    }

    Get(s"/freeCode/instance/${freeCodeModelWarning.name}") ~> controller.getRoute ~> check {
      val models = responseAs[AngularResponse[FreeCodeModel]]
      models.data shouldBe freeCodeModelWarning
    }

    Get(s"/freeCode/instance/${freeCodeModelCorrect.name}") ~> controller.getRoute ~> check {
      val models = responseAs[AngularResponse[FreeCodeModel]]
      models.data shouldBe freeCodeModelCorrect
    }

  }



  it should "test delete" in {
    val serviceDB =  new FreeCodeDBServiceMock
    val freeCodeCompiler = new FreeCodeCompilerUtilsMock(false,false)
    val controller = new FreeCodeController(serviceDB,freeCodeCompiler)
    serviceDB.storage.size shouldBe 1
    serviceDB.storage.exists(_.name.equals(freeCodeModelDefault.name)) shouldBe true

    Delete(s"/freeCode/instance/${freeCodeModelDefault.name}") ~> controller.getRoute ~> check {
      response
      responseAs[AngularResponse[String]] shouldBe AngularResponse("OK", "OK")
      serviceDB.storage.size shouldBe 0
    }

    //delete again
    Delete(s"/freeCode/instance/${freeCodeModelDefault.name}") ~> controller.getRoute ~> check {
      responseAs[AngularKoReponse[String]].Result shouldBe "KO"
      serviceDB.storage.size shouldBe 0
    }



    serviceDB.storage = List(freeCodeModelDefault,freeCodeModelError,freeCodeModelWarning,freeCodeModelCorrect)
    serviceDB.storage.size shouldBe 4

    Delete(s"/freeCode/instance/${freeCodeModelDefault.name}") ~> controller.getRoute ~> check {
      responseAs[AngularResponse[String]] shouldBe AngularResponse("OK", "OK")
      serviceDB.storage.size shouldBe 3
    }

    Delete(s"/freeCode/instance/${freeCodeModelError.name}") ~> controller.getRoute ~> check {
      responseAs[AngularResponse[String]] shouldBe AngularResponse("OK", "OK")
      serviceDB.storage.size shouldBe 2
    }

    Delete(s"/freeCode/instance/${freeCodeModelWarning.name}") ~> controller.getRoute ~> check {
      responseAs[AngularResponse[String]] shouldBe AngularResponse("OK", "OK")
      serviceDB.storage.size shouldBe 1
    }

    Delete(s"/freeCode/instance/${freeCodeModelCorrect.name}") ~> controller.getRoute ~> check {
      responseAs[AngularResponse[String]] shouldBe AngularResponse("OK", "OK")
      serviceDB.storage.size shouldBe 0
    }

  }

  it should "test complete api" in {
    val serviceDB = new FreeCodeDBServiceMock
    val freeCodeCompiler = new FreeCodeCompilerUtilsMock(false, false,true)
    val controller = new FreeCodeController(serviceDB, freeCodeCompiler)

    Post("/freeCode/complete/1", freeCodeModelWarning) ~> controller.getRoute ~> check {
      serviceDB.storage.size shouldBe 1
      val output  = serviceDB.storage.find(_.name.equals(freeCodeModelWarning.name))
      output.isDefined shouldBe false
      val models = responseAs[AngularResponse[List[CompletionModel]]]
      models.data.size shouldBe 1
      models.data.head shouldBe freeCodeCompiler.complete
      models.Result shouldBe "OK"
    }

  }

  it should "test complete api without complete" in {
    val serviceDB = new FreeCodeDBServiceMock
    val freeCodeCompiler = new FreeCodeCompilerUtilsMock(false, false,false)
    val controller = new FreeCodeController(serviceDB, freeCodeCompiler)

    Post("/freeCode/complete/1", freeCodeModelWarning) ~> controller.getRoute ~> check {
      serviceDB.storage.size shouldBe 1
      val output  = serviceDB.storage.find(_.name.equals(freeCodeModelWarning.name))
      output.isDefined shouldBe false
      val models = responseAs[AngularResponse[List[CompletionModel]]]
      models.data.size shouldBe 0
      models.Result shouldBe "OK"
    }

  }

  it should "test validate correct model" in {

    val serviceDB =  new FreeCodeDBServiceMock
    val freeCodeCompiler = new FreeCodeCompilerUtilsMock(false,false)
    val controller = new FreeCodeController(serviceDB,freeCodeCompiler)

    Post("/freeCode/validate", freeCodeModelCorrect) ~> controller.getRoute ~> check {
      responseAs[AngularResponse[String]] shouldEqual AngularResponse("OK", "OK")
    }
  }



  it should "test validate warning model" in {

    val serviceDB =  new FreeCodeDBServiceMock
    val freeCodeCompiler = new FreeCodeCompilerUtilsMock(false,true)
    val controller = new FreeCodeController(serviceDB,freeCodeCompiler)
    Post("/freeCode/validate", freeCodeModelWarning) ~> controller.getRoute ~> check {
      val models = responseAs[AngularResponse[List[ErrorModel]]]
      models.data.size shouldBe 1
      models.data.head.errorType shouldBe "warning"
      models.Result shouldBe "OK"
    }
  }




  it should "test validate wrong model" in {

    val serviceDB =  new FreeCodeDBServiceMock
    val freeCodeCompiler = new FreeCodeCompilerUtilsMock(true,true)
    val controller = new FreeCodeController(serviceDB,freeCodeCompiler)
    Post("/freeCode/validate", freeCodeModelError) ~> controller.getRoute ~> check {
      val models = responseAs[AngularResponse[List[ErrorModel]]]
      models.data.size shouldBe 1
      models.data.head.errorType shouldBe "error"
      models.Result shouldBe "KO"
    }
  }


}

private class FreeCodeCompilerUtilsMock(errors : Boolean, warnings: Boolean,codeToComplete : Boolean=true) extends FreeCodeCompilerUtils{
  private val error = ErrorModel("virtual","1","error","error","","")
  private val warning = ErrorModel("virtual","1","warning","warning","","")
  val complete = CompletionModel("toString","()=>String")

  override def validate(code: String): List[ErrorModel] = {
    if(errors) List(error)
    else if(warnings) List(warning)
    else List.empty
  }

  override def complete(code: String,int : Int): List[CompletionModel] = {
    if(codeToComplete) List(complete)
    else List.empty
  }
}


private class FreeCodeDBServiceMock extends FreeCodeDBService {

  var storage = List(freeCodeModelDefault)

  override def getByName(name: String): Option[FreeCodeModel] = storage.find(_.name.equals(name))

  override def deleteByName(name: String): Unit = {
    storage = storage.filterNot(_.name.equals(name))
  }

  override def getAll: Seq[FreeCodeModel] = storage

  override def insert(freeCodeModel: FreeCodeModel): Unit = {
    storage = storage :+ freeCodeModel
  }

}

private object FreeCodeDataSupport {
  val freeCodeModelDefault : FreeCodeModel = FreeCodeModel("test-default",
    """val a = "test"
      | a.toString """.stripMargin)

  val freeCodeModelCorrect: FreeCodeModel = FreeCodeModel("test-correct",
    """val a = "test"
      | a.toString """.stripMargin)

  val freeCodeModelWarning: FreeCodeModel = FreeCodeModel("test-warn",
    """val a = "test"
      | a
      | a
      | """.stripMargin)

  val freeCodeModelError: FreeCodeModel = FreeCodeModel("test-error",
    """val a = "test"
      | c
      | """.stripMargin)

}
