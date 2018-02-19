package com.lightbend.kafka.scala.streams.typesafe

import com.lightbend.kafka.scala.streams.FunctionConversions._
import com.lightbend.kafka.scala.streams.typesafe.ImplicitConverters._
import org.apache.kafka.streams.kstream.{KGroupedTable, Materialized}

/** Wraps the Java class [[KGroupedTable]] and delegates method calls to the
  * underlying Java object. Makes use of implicit parameters for the
  * [[util.Materialized]] instances.
  *
  * @author Santiago Saavedra (ssaavedra@openshine.com)
  */
class TSKGroupedTable[K, V]
(protected[typesafe] override val unsafe: KGroupedTable[K, V])
  extends AnyVal with TSKType[KGroupedTable, K, V] {
  def count(implicit materialized: Materialized[K, java.lang.Long, kvs])
  : TSKTable[K, Long] =
    unsafe
      .count(materialized)
      .mapValues[scala.Long](
      { l: java.lang.Long => Long2long(l) }.asValueMapper)
      .safe

  def reduce(adder: (V, V) => V,
             subtractor: (V, V) => V)
            (implicit materialized: Materialized[K, V, kvs]): TSKTable[K, V] =
    unsafe
      .reduce(adder.asReducer, subtractor.asReducer, materialized)
      .safe

  def aggregate[VR](initializer: => VR,
                    adder: (K, V, VR) => VR,
                    subtractor: (K, V, VR) => VR)
                   (implicit materialized: Materialized[K, VR, kvs])
  : TSKTable[K, VR] =
    unsafe
      .aggregate(initializer.asInitializer,
        adder.asAggregator,
        subtractor.asAggregator,
        materialized)
      .safe
}
