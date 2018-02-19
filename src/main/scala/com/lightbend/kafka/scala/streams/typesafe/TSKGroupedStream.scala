package com.lightbend.kafka.scala.streams.typesafe

import com.lightbend.kafka.scala.streams.FunctionConversions._
import com.lightbend.kafka.scala.streams.typesafe.implicits._
import org.apache.kafka.streams.kstream._

/** Wraps the Java class [[KGroupedStream]] and delegates method calls to the
  * underlying Java object. Makes use of implicit parameters for the
  * [[util.Materialized]] and [[org.apache.kafka.common.serialization.Serde]]
  * instances.
  *
  * @author Santiago Saavedra (ssaavedra@openshine.com)
  */
class TSKGroupedStream[K, V]
(protected[typesafe] override val unsafe: KGroupedStream[K, V])
  extends AnyVal with TSKType[KGroupedStream, K, V] {

  def aggregate[VR](initializer: => VR, aggregator: (K, V, VR) => VR)
                   (implicit materialized: Materialized[K, VR, kvs])
  : TSKTable[K, VR] =
    unsafe
      .aggregate(initializer.asInitializer, aggregator.asAggregator,
        materialized)
      .safe

  def count(implicit materialized: Materialized[K, java.lang.Long, kvs])
  : TSKTable[K, Long] =
    unsafe
      .count(materialized)
      .mapValues[scala.Long](
      { l: java.lang.Long => Long2long(l) }.asValueMapper)
      .safe

  def reduce(reducer: (V, V) => V)
            (implicit materialized: Materialized[K, V, kvs])
  : TSKTable[K, V] =
    unsafe
      .reduce(reducer.asReducer, materialized)
      .safe

  def windowedBy(windows: SessionWindows)
  : TSSessionWindowedKStream[K, V] =
    unsafe
      .windowedBy(windows)
      .safe

  def windowedBy[W <: Window](windows: Windows[W])
  : TSTimeWindowedKStream[K, V] =
    unsafe
      .windowedBy(windows)
      .safe
}