package it.nerdammer.spark.additions.tryfunctions

import org.apache.spark.SparkContext

/**
  * Adds new implicit functions into the SparkContext.
  *
  * @param sc the Spark Context
  */
private [nerdammer] class TrySparkContextFunctions(sc: SparkContext) {

  /**
    * Retrieves the accumulated exceptions.
    *
    * @param name the (optional) name of the accumulator variable
    * @return the accumulated exceptions
    */
  def accumulatedExceptions(name: String = TryRDDAccumulatorHolder.ExceptionAccumulatorDefaultName): Iterable[(Any, Throwable)] =
    TryRDDAccumulatorHolder.getExceptionAccumulator(name)(sc).toList.flatMap(exs => exs.value)

}