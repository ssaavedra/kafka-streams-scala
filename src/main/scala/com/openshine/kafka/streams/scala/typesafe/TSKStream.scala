/*
 * Copyright 2018 OpenShine SL <https://www.openshine.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openshine.kafka.streams.scala.typesafe

import com.openshine.kafka.streams.scala.FunctionConversions._
import com.openshine.kafka.streams.scala.typesafe.ImplicitConverters._
import org.apache.kafka.streams.{KeyValue, StreamsBuilder}
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.api.{FixedKeyProcessorSupplier, Processor, ProcessorSupplier}

import scala.jdk.CollectionConverters._


/**
  * Typesafe KStream implementation. Does not directly allow type-unsafe
  * operations and every serde is implicitly provided.
  */
class TSKStream[K, V](override protected[typesafe] val unsafe: KStream[K, V])
    extends AnyVal
    with TSKType[KStream, K, V] {

  def split(named: Named) = unsafe.split(named).safe

  def split(): TSBranchedKStream[K, V] = unsafe.split().safe

  @deprecated("since 2.8. Use split() instead")
  def branch(predicates: ((K, V) => Boolean)*): Array[TSKStream[K, V]] = {
    unsafe.branch(predicates.map(_.asPredicate): _*).map(_.safe)
  }

  def groupBy[KR](
      selector: (K, V) => KR
  )(
    implicit grouped: Grouped[KR, V]
  ): TSKGroupedStream[KR, V] =
    unsafe
      .groupBy(selector.asKeyValueMapper, grouped)
      .safe

  def groupByKey(
      implicit grouped: Grouped[K, V]
  ): TSKGroupedStream[K, V] =
    unsafe
      .groupByKey(grouped)
      .safe

  def join[VO, VR](
      otherStream: TSKStream[K, VO],
      joiner: (K, V, VO) => VR,
      windows: JoinWindows
  )(implicit joined: StreamJoined[K, V, VO]): TSKStream[K, VR] =
    unsafe
      .join[VO, VR](otherStream.unsafe, joiner.asKeyValueJoiner, windows, joined)
      .safe

  def join[VT, VR](table: TSKTable[K, VT], joiner: (V, VT) => VR)(
      implicit joined: Joined[K, V, VT]
  ): TSKStream[K, VR] =
    unsafe.join[VT, VR](table.unsafe, joiner.asValueJoiner, joined).safe

  def join[GK, GV, RV](
      globalKTable: GlobalKTable[GK, GV],
      keyValueMapper: (K, V) => GK,
      joiner: (V, GV) => RV
  ): TSKStream[K, RV] = {
    val vj: ValueJoiner[V, GV, RV] = joiner(_, _)
    unsafe
      .join[GK, GV, RV](globalKTable, (k: K, v: V) => keyValueMapper(k, v), vj)
      .safe
  }

  def leftJoin[VO, VR](
      otherStream: TSKStream[K, VO],
      joiner: (V, VO) => VR,
      windows: JoinWindows
  )(implicit joined: StreamJoined[K, V, VO]): TSKStream[K, VR] =
    unsafe
      .leftJoin[VO, VR](
        otherStream.unsafe,
        joiner.asValueJoiner,
        windows,
        joined
      )
      .safe

  def leftJoin[VT, VR](table: TSKTable[K, VT], joiner: (V, VT) => VR)(
      implicit joined: Joined[K, V, VT]
  ): TSKStream[K, VR] =
    unsafe
      .leftJoin[VT, VR](table.unsafe, joiner.asValueJoiner, joined)
      .safe

  def leftJoin[GK, GV, RV](
      globalKTable: GlobalKTable[GK, GV],
      keyValueMapper: (K, V) => GK,
      joiner: (V, GV) => RV
  ): TSKStream[K, RV] =
    unsafe
      .leftJoin[GK, GV, RV](
        globalKTable,
        keyValueMapper.asKeyValueMapper,
        joiner.asValueJoiner
      )
      .safe

  def outerJoin[VO, VR](
      otherStream: TSKStream[K, VO],
      joiner: (V, VO) => VR,
      windows: JoinWindows
  )(implicit joined: StreamJoined[K, V, VO]): TSKStream[K, VR] =
    unsafe
      .outerJoin[VO, VR](
        otherStream.unsafe,
        joiner.asValueJoiner,
        windows,
        joined
      )
      .safe

  def merge(stream: TSKStream[K, V]): TSKStream[K, V] =
    unsafe.merge(stream.unsafe).safe

  def peek(action: (K, V) => Unit): TSKStream[K, V] =
    unsafe.peek(action(_, _)).safe

  def peek(action: (K, V) => Unit, named: Named): TSKStream[K, V] =
    unsafe.peek(action(_, _), named).safe

  def split(
      predicate: (K, V) => Boolean
  ): (TSKStream[K, V], TSKStream[K, V]) =
    (filter(predicate), filterNot(predicate))

  def filter(predicate: (K, V) => Boolean): TSKStream[K, V] =
    unsafe.filter(predicate.asPredicate).safe

  def filterNot(predicate: (K, V) => Boolean): TSKStream[K, V] =
    unsafe.filterNot(predicate.asPredicate).safe

  def selectKey[KR](mapper: (K, V) => KR): TSKStream[KR, V] = {
    unsafe.selectKey[KR]((k: K, v: V) => mapper(k, v)).safe
  }

  def map[KR, VR](mapper: (K, V) => (KR, VR)): TSKStream[KR, VR] =
    unsafe.map[KR, VR](mapper.asKeyValueMapper).safe

  def mapValues[VR](mapper: V => VR): TSKStream[K, VR] =
    unsafe.mapValues[VR](mapper.asValueMapper).safe

  def flatMap[KR, VR](
      mapper: (K, V) => Iterable[(KR, VR)]
  ): TSKStream[KR, VR] = {
    val kvMapper = mapper.tupled andThen (
        iter =>
          iter
            .map(t => KeyValue.pair(t._1, t._2))
            .asJava
      )

    unsafe.flatMap[KR, VR]((k: K, v: V) => kvMapper((k, v))).safe
  }

  def flatMapValues[VR](mapper: V => Iterable[VR]): TSKStream[K, VR] =
    unsafe
      .flatMapValues({ v: V =>
        mapper(v).asJava
      }.asValueMapper)
      .safe

  def filterValues(predicate: V => Boolean): TSKStream[K, V] =
    unsafe.filter((k, v) => predicate(v)).safe

  def print(printed: Printed[K, V]): Unit = unsafe.print(printed)

  def foreach(action: (K, V) => Unit): Unit =
    unsafe.foreach((key: K, value: V) => action(key, value))

  def process[KOut, VOut](
      processorSupplier: () => Processor[K, V, KOut, VOut],
      stateStoreNames: String*
  ): TSKStream[KOut, VOut] = {
    val processorSupplierJ: ProcessorSupplier[K, V, KOut, VOut] = () =>
      processorSupplier()
    unsafe
      .process(processorSupplierJ, stateStoreNames: _*)
      .safe
  }

  def processValues[VR](
    processorSupplier: FixedKeyProcessorSupplier[K, V, VR],
    named: Named,
    stateStoreNames: String*
  ): TSKStream[K, VR] = {
    unsafe.processValues(processorSupplier, named, stateStoreNames: _*).safe
  }

  def repartition(
      implicit repartitioned: TSRepartitioned[K, V]
  ): TSKStream[K, V] = {
    unsafe.repartition(repartitioned.unsafe).safe
  }

  def to(topic: String)(implicit produced: Produced[K, V]): Unit = {
    unsafe.to(topic, produced)
  }
}

object TSKStream {

  /** Creates a new TSKStream from a topic, given an implicit StreamsBuilder
    * and the appropriate Serde instances for the types you want to read from
    * the topic.
    *
    * @param topic the topic name you want to read from
    * @param builder the StreamsBuilder you want to use
    * @param consumed the Consumed instance that contains the deserialization
    *                 procedure from the Kafka topic
    * @tparam K the type of keys to be read from the topic
    * @tparam V the type of values to be read from the topic
    * @return
    */
  def apply[K, V](topic: String)(
    implicit builder: StreamsBuilder,
    consumed: Consumed[K, V]
  ): TSKStream[K, V] =
    new TSKStream[K, V](builder.stream(topic, consumed))
}
