package com.lightbend.kafka.scala.streams.typesafe

import com.lightbend.kafka.scala.streams.FunctionConversions._
import com.lightbend.kafka.scala.streams.typesafe.implicits._
import org.apache.kafka.streams.kstream.{Materialized,
  SessionWindowedKStream, Windowed}

/**
  * @author Santiago Saavedra (ssaavedra@openshine.com)
  */
class TSSessionWindowedKStream[K, V]
(protected[typesafe] override val unsafe: SessionWindowedKStream[K, V])
  extends AnyVal with TSKType[SessionWindowedKStream, K, V] {

  def aggregate[VR](initializer: => VR,
                    aggregator: (K, V, VR) => VR,
                    merger: (K, VR, VR) => VR)
                   (implicit materialized: Materialized[K, VR, ssb])
  : TSKTable[Windowed[K], VR] = {
    unsafe.aggregate(
      initializer.asInitializer,
      aggregator.asAggregator,
      merger.asMerger,
      materialized)
      .safe
  }

  def count(implicit materialized: Materialized[K, java.lang.Long, ssb])
  : TSKTable[Windowed[K], Long] =
    unsafe
      .count(materialized)
      .mapValues[scala.Long]({
      l: java.lang.Long => Long2long(l)
    }.asValueMapper).safe


  def reduce(reducer: (V, V) => V)
            (implicit materialized: Materialized[K, V, ssb])
  : TSKTable[Windowed[K], V] = {
    unsafe.reduce(reducer.asReducer, materialized).safe
  }

}
