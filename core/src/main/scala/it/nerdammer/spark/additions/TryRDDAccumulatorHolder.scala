package it.nerdammer.spark.additions

import org.apache.spark.{Accumulable, SparkContext}

import scala.collection.concurrent.TrieMap

import scala.collection._

/**
  * Holds accumulator variables that need to be retrieved later, after the execution of some actions.
  *
  * @author Nicola Ferraro
  */
private [nerdammer] object TryRDDAccumulatorHolder {

  /**
    * The default name of the accumulator used to hold the exceptions.
    */
  val ExceptionAccumulatorDefaultName: String = "exceptions"

  /**
    * A map of exception accumulators.
    */
  private [nerdammer] val accumulators = new TrieMap[String, Accumulable[mutable.HashSet[(Any, Throwable)], (Any, Throwable)]]()

  /**
    * Holds an exception accumulator with the given name for the current spark context.
    *
    * @param acc the accumulator
    * @param name the name of the accumulator variable
    * @param sc the current spark context
    */
  def putExceptionAccumulator(acc: Accumulable[mutable.HashSet[(Any, Throwable)], (Any, Throwable)], name: String = ExceptionAccumulatorDefaultName)(implicit sc: SparkContext): Unit =
    accumulators.put(s"${sc.applicationId}-$name", acc)

  /**
    * Retrieves the exception accumulator with the given name.
    *
    * @param name the name of the accumulator variable
    * @param sc the spark context
    * @return an option for the exception accumulator
    */
  def getExceptionAccumulator(name: String = ExceptionAccumulatorDefaultName)(implicit sc: SparkContext): Option[Accumulable[mutable.HashSet[(Any, Throwable)], (Any, Throwable)]] =
    accumulators.get(s"${sc.applicationId}-$name")

}
