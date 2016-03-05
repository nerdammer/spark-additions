package com.enterprise.test

import java.sql.{DriverManager}

import it.nerdammer.log4j.PhoenixAppender
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * @author Nicola Ferraro
  */
class SparkJobTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  lazy val sc: SparkContext = {
    val conf = new SparkConf()
      .setAppName("Console Test")
      .setMaster("local")

    new SparkContext(conf)
  }

  override protected def beforeAll(): Unit = sc

  override protected def afterAll(): Unit = sc.stop()


  "the console logger " should "work" in {


    val databaseURL = Logger.getLogger("com.enterprise").getAppender("phoenix").asInstanceOf[PhoenixAppender].getURL
    val conn = DriverManager.getConnection(databaseURL, "any", "any")
    conn.setAutoCommit(true)
    val pstm = conn.prepareStatement("delete from log")
    pstm.execute()
    pstm.close()

    sc.parallelize(1 to 10)
      .map(e => {
        Logger.getLogger(classOf[SparkJobTest]).info("This is a log message")
        e
      })
      .count();

    val pstm2 = conn.prepareStatement("select count(*) from log")
    val rs = pstm2.executeQuery()
    rs.next()
    val count = rs.getInt(1)
    rs.close()
    pstm.close()
    conn.close()

    assert(count == 10)

  }



}
