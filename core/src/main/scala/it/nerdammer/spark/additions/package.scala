package it.nerdammer.spark

import it.nerdammer.spark.additions.tryfunctions.{TrySparkContextFunctions, TryRDDFunctions}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.language.implicitConversions

/**
  * Define all the implicit conversions used in the current library.
  *
  * @author Nicola Ferraro
  */
package object additions {

  implicit def rddToTryRDDFunctions[T](rdd: RDD[T]): TryRDDFunctions[T] = new TryRDDFunctions[T](rdd)

  implicit def sparkContextToTrySparkContextFunctions(sc: SparkContext): TrySparkContextFunctions = new TrySparkContextFunctions(sc)

}
