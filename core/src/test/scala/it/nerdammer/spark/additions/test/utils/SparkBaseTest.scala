package it.nerdammer.spark.additions.test.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * Base class for all Spark tests.
  *
  * @author Nicola Ferraro
  */
class SparkBaseTest extends FlatSpec with BeforeAndAfter {

  var sc: SparkContext = null

  before {
    val conf = new SparkConf()
      .setAppName("BasicIT")
      .set("spark.driver.allowMultipleContexts", "true")
      .setMaster("local[2]")

    sc = new SparkContext(conf)
  }

  after {
    sc.stop()
  }

}
