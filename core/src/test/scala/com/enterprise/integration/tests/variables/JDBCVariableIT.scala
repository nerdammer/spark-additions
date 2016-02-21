package com.enterprise.integration.tests.variables

import java.sql.{Connection, SQLException}


import it.nerdammer.spark.additions.test.utils.SparkBaseTest
import it.nerdammer.spark.additions.variables.{SharedSingleton, SharedVariable}
import org.apache.commons.dbcp2.BasicDataSource
import org.scalatest.{Matchers, FlatSpec}

import scala.util.Try

/**
  *
  * @author Nicola Ferraro
  */
class JDBCVariableIT extends SparkBaseTest with Matchers {

  "this" should "not be serializable" in {

    val trial = Try {


    val ds = new BasicDataSource()
    ds.setDriverClassName("org.driver.Classname")
    ds.setUrl("...")
    // set username, options

    sc.parallelize(1 to 10)
      .map(i => {
        val conn = ds.getConnection
        // do something
        conn.close()
      })
      .count

    }

    assert(trial.isFailure)

  }

  "this" should "be serializable" in {

    val trial = Try {

      sc.parallelize(1 to 10)
        .map(i => {
          val ds = Holder.ds
          // do something
        })
        .count

    }

    assert(trial.isSuccess)

  }

  "this" should "also be serializable" in {

    val dsv = SharedVariable {
      val ds = new BasicDataSource()
      ds.setDriverClassName("org.driver.Classname")
      ds.setUrl("...")
      // set username, options
      ds
    }

    val trial = Try {

      sc.parallelize(1 to 10)
        .map(i => {
          val ds = dsv.get
          // do something
        })
        .count

    }

    assert(trial.isSuccess)

  }

  "singletons" should "also be serializable" in {

    val dsv = SharedSingleton {
      val ds = new BasicDataSource()
      ds.setDriverClassName("org.driver.Classname")
      ds.setUrl("...")
      // set username, options
      ds
    }

    val trial = Try {

      sc.parallelize(1 to 10)
        .map(i => {
          val ds = dsv.get
          // do something
        })
        .count

    }

    assert(trial.isSuccess)

  }

}

object Holder {

  def ds() = {
    val ds = new BasicDataSource()
    ds.setDriverClassName("org.driver.Classname")
    ds.setUrl("...")
    // set username, options
    ds
  }

}
