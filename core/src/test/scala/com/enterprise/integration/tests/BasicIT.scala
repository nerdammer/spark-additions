package com.enterprise.integration.tests

import it.nerdammer.spark.additions.test.utils.SparkBaseTest
import org.scalatest.Matchers

import it.nerdammer.spark.additions._

/**
  * Basic tests of the library.
  *
  * @author Nicola Ferraro
  */
class BasicIT extends SparkBaseTest with Matchers {

  "try functions" should "be available for anyone" in {

    val sum = sc.parallelize(1 to 10)
      .tryMap(i => i+1)
      .tryFlatMap(i => Some(i-1))
      .reduce(_ + _)

    assert(sum == 55)

  }


  "try functions" should "not throw exceptions. It should collect them" in {

    val sum = sc.parallelize(1 to 12)
      .tryMap(i => {
        if(i%2==0)
          i
        else
          throw new RuntimeException("A")
      })
      .tryFlatMap(i => {
        if(i%3==0)
          Some(i)
        else
          throw new Exception("B")
      })
      .reduce(_ + _)

    // only 6 and 12 are ok
    assert(sum == 18)
    assert(sc.accumulatedExceptions().size == 10)
    assert(sc.accumulatedExceptions().map{case (i, e)=> e}.count(_.getMessage=="A") == 6)
    assert(sc.accumulatedExceptions().map{case (i, e)=> e}.count(_.getMessage=="B") == 4)

  }

  "multiple accumulator names " can "be defined" in {

    val sum = sc.parallelize(1 to 12)
      .tryMap(i => {
        if(i%2==0)
          i
        else
          throw new RuntimeException("A")
      }, "acc1")
      .tryFlatMap(i => {
        if(i%3==0)
          Some(i)
        else
          throw new Exception("B")
      }, "acc2")
      .reduce(_ + _)

    // only 6 and 12 are ok
    assert(sum == 18)
    assert(sc.accumulatedExceptions().isEmpty)
    assert(sc.accumulatedExceptions("acc1").size == 6)
    assert(sc.accumulatedExceptions("acc2").size == 4)
    assert(sc.accumulatedExceptions("acc1").map{case (i, e)=> e}.forall(_.getMessage=="A"))
    assert(sc.accumulatedExceptions("acc2").map{case (i, e)=> e}.forall(_.getMessage=="B"))

  }

}
