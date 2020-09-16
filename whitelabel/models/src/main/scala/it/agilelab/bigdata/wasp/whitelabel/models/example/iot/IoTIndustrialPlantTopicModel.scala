package it.agilelab.bigdata.wasp.whitelabel.models.example.iot

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Date, UUID}

import com.sksamuel.avro4s.AvroSchema
import it.agilelab.bigdata.wasp.core.utils.JsonConverter
import it.agilelab.bigdata.wasp.models.TopicModel

import scala.util.Random

object IoTIndustrialPlantTopicModel {

  val iotIndustrialPlantTopicModelName = "industrial-plant"
  lazy val industrialPlantDataSchema = AvroSchema[IndustrialPlantData].toString


  lazy val industrialPlantTopicModel = TopicModel (
    name = TopicModel.name(iotIndustrialPlantTopicModelName),
    creationTime = System.currentTimeMillis,
    partitions = 3,
    replicas = 1,
    topicDataType = "avro",
    keyFieldName = None,
    headersFieldName = None,
    valueFieldsNames = None,
    useAvroSchemaManager = true,
    schema = JsonConverter.fromString(industrialPlantDataSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
  )

}

case class IndustrialPlantData(site: String, plant: String, line: String, machine: String, areadId: String, timestamp: String, kpi: String, kpiValue: Int)
object FakeIndustrialPlantData{

  private val random = new Random()

  val sites = Array("Italy-01","Budapest-01", "Milwaukee-01", "El Paso-01", "Shenzhen-01")
  val plants = Array("AB01", "AB02","BB3","Budapest", "Milwaukee", "El Paso", "Shenzhen")
  val lines = Array("Package-A320","Package-A430","Package-A440-West", "Plastic-B110", "Plastic-B110-East")
  val machines = Array("AK15","BK16","BB22", "BB24", "AB35", "AB36")
  val areadIds = Array("1EST11","1EST12","1WEST11", "1WEST12", "2WEST22", "2WEST23")
  val kpi = Array(("PieceCounterLastMin",50),("MTTR", 6),("MTBF",44), ("OEE", 100), ("OLE", 100), ("OPE", 100))
  val counters = Array(("MachineCounter",102300))

  def getKpiValue(index: Int) = (kpi(index)._2*0.75 +  random.nextInt(kpi(index)._2)*0.25).toInt

  def fromRandom(): IndustrialPlantData = {


    val time = ZonedDateTime.now( ZoneOffset.UTC ).format( DateTimeFormatter.ISO_INSTANT )

    val kpiIndex = random.nextInt(6)
    IndustrialPlantData(sites(random.nextInt(5)), plants(random.nextInt(6)), lines(random.nextInt(5)),
      machines(random.nextInt(6)), areadIds(random.nextInt(6)),time, kpi(kpiIndex)._1,  getKpiValue(kpiIndex)  )
  }
}

