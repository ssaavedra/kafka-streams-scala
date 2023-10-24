package com.openshine.kafka.streams.scala.typesafe

import com.openshine.kafka.streams.scala.typesafe.ImplicitConverters.KTypeSafeProvider
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{KStream, Repartitioned}
import org.apache.kafka.streams.processor.StreamPartitioner


class TSRepartitioned[K, V](override protected[typesafe] val unsafe: Repartitioned[K, V])
  extends AnyVal
    with TSKType[Repartitioned, K, V] {
  def withName(name: String): TSRepartitioned[K, V] =
    unsafe.withName(name).safe

  def withNumberOfPartitions(numberOfPartitions: Int): TSRepartitioned[K, V] =
    unsafe.withNumberOfPartitions(numberOfPartitions).safe

  def withStreamPartitioner(partitioner: StreamPartitioner[K, V]): TSRepartitioned[K, V] =
    unsafe.withStreamPartitioner(partitioner).safe
}

object TSRepartitioned {
  def fromSerde[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): TSRepartitioned[K, V] =
    Repartitioned.`with`(keySerde, valueSerde).safe
}
