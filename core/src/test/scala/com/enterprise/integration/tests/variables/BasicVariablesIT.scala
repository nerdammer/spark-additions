package com.enterprise.integration.tests.variables

import it.nerdammer.spark.additions.test.utils.SparkBaseTest
import it.nerdammer.spark.additions.variables.{SharedSingleton, SharedVariable}
import org.apache.commons.lang3.SerializationUtils
import org.scalatest.Matchers

import scala.util.Try

/**
  * Basic tests of the variables.
  *
  * @author Nicola Ferraro
  */
class BasicVariablesIT extends SparkBaseTest with Matchers  {

  "test that some objects" can "not work alone" in {

    val service = new NonSerializable

    val trial = Try {
      sc.parallelize(1 to 10)
        .map(_.toString)
        .map(s => service.hello(s))
        .collect()
    }

    assert(trial.isFailure)

  }

  "test that shared variables" can "work" in {

    val service = SharedVariable{ new NonSerializable }

    val lst = sc.parallelize(1 to 10)
      .map(_.toString)
      .map(s => service.get.hello(s))
      .collect()

    assert(lst.length == 10)

  }

  "test that shared variables" can "be serialized and deserialized correctly" in {

    val service = SharedVariable{ new NonSerializable }

    val lst = for(i <- 1 to 5; clonedService = SerializationUtils.clone(service))
      yield sc.parallelize(1 to 10)
        .map(_.toString)
        .map(s => clonedService.get.hello(s))
        .collect()

    assert(lst.flatten.size == 50)

  }

  "test that shared singletons" can "work" in {

    val service = SharedSingleton{ new NonSerializable }

    val lst = sc.parallelize(1 to 10)
      .map(_.toString)
      .map(s => service.get.hello(s))
      .collect()

    assert(lst.length == 10)

  }

  "test that shared singletons" can "are real singletons" in {

    SharedSingleton.poolClear()

    val service = SharedSingleton{ new NonSerializable }

    for(i <- 1 to 5) {
      val clonedService = SerializationUtils.clone(service)
      sc.parallelize(1 to 10)
        .map(_.toString)
        .map(s => clonedService.get.hello(s))
        .collect()
    }

    assert(SharedSingleton.poolSize == 1)
  }

  "test that shared singletons" can "not collide" in {

    SharedSingleton.poolClear()

    val service = SharedSingleton{ new NonSerializable }
    val service2 = SharedSingleton{ new NonSerializable }

    for(i <- 1 to 5) {
      val clonedService = SerializationUtils.clone(service)
      val clonedService2 = SerializationUtils.clone(service2)
      sc.parallelize(1 to 10)
        .map(_.toString)
        .map(s => clonedService2.get.hello(clonedService.get.hello(s)))
        .collect()
    }

    assert(SharedSingleton.poolSize == 2)
  }


  "test that shared variables and singletons" can "be mixed" in {

    SharedSingleton.poolClear()
    val delegate = SharedVariable { new NonSerializable }
    val service = SharedSingleton{ delegate.get }

    for(i <- 1 to 5) {
      val clonedService = SerializationUtils.clone(service)
      sc.parallelize(1 to 10)
        .map(_.toString)
        .map(s => clonedService.get.hello(s))
        .collect()
    }

    assert(SharedSingleton.poolSize == 1)
  }

}


class NonSerializable {

  def hello(name: String) = s"Hello $name"

}