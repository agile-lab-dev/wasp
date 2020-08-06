package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.models.FreeCodeModel

object TestFreeCodeModels {

  val testFreeCode: FreeCodeModel = FreeCodeModel("test-freecode",
    """
      | dataFrames.head._2
      |""".stripMargin)
}
