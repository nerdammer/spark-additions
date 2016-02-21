package it.nerdammer.spark.additions.tryfunctions

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
  * Define new methods to handle exceptions as accumulable objects.
  *
  * @author Nicola Ferraro
  */
private [nerdammer] class TryRDDFunctions[T](rdd: RDD[T]) extends AnyRef with Serializable {

  @transient implicit private val sc = rdd.sparkContext

  /**
    * Retrieves or creates the accumulator object used by this RDD.
    */
  def retrieveAccumulator(accumulatorName: String) = {

    TryRDDAccumulatorHolder.accumulators.synchronized {

      // Initialize the accumulator if needed
      if(TryRDDAccumulatorHolder.getExceptionAccumulator(accumulatorName).isEmpty) {
        val exceptionAccumulator = sc.accumulableCollection(mutable.HashSet[(Any, Throwable)]())
        TryRDDAccumulatorHolder.putExceptionAccumulator(exceptionAccumulator, accumulatorName)
      }
    }

    TryRDDAccumulatorHolder.getExceptionAccumulator(accumulatorName).get
  }


  /**
    * Tries to map every element of the RDD. Elements for which the given transformation function throws a Throwable will be discarded.
    * All exceptions and the related the input data will be collected in the driver node.
    *
    * @param f the map function
    * @param accumulatorName the (optional)exception accumulator name
    * @param ct the implicit class tag
    * @tparam U the destination type of contained elements
    * @return the mapped RDD
    */
  def tryMap[U](f: (T) ⇒ U, accumulatorName: String = TryRDDAccumulatorHolder.ExceptionAccumulatorDefaultName)(implicit ct: ClassTag[U]): RDD[U] = {

    val accumulator = retrieveAccumulator(accumulatorName)

    rdd.flatMap(e => {
      val fe = Try{f(e)}
      val trial = fe match {
        case Failure(t) =>
          accumulator += (e, t)
          fe
        case t: Try[U] => t
      }
      trial.toOption
    })
  }


  /**
    * Tries to execute a flatMap on the RDD. Elements for which the given transformation function throws a Throwable will be discarded.
    * All exceptions and the related the input data will be collected in the driver node.
    *
    * @param f the map function
    * @param accumulatorName the (optional)exception accumulator name
    * @param ct the implicit class tag
    * @tparam U the destination type of contained elements
    * @return the mapped RDD
    */
  def tryFlatMap[U](f: (T) ⇒ TraversableOnce[U], accumulatorName: String = TryRDDAccumulatorHolder.ExceptionAccumulatorDefaultName)(implicit ct: ClassTag[U]): RDD[U] = {

    val accumulator = retrieveAccumulator(accumulatorName)

    rdd.flatMap(e => {
      val fe = Try{f(e)}
      val trial = fe match {
        case Failure(t) =>
          accumulator += (e, t)
          Nil
        case Success(r) => r
      }
      trial
    })
  }

}

