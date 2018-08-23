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

package com.lightbend.kafka.scala.streams.typesafe

import com.lightbend.kafka.scala.streams.typesafe.unsafe.ConverterToTypeSafer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._

import scala.language.implicitConversions

/** Conversions to keep the underlying abstraction from leaking. These allow
  * us to always return a TS object instead of the underlying one.
  *
  * These conversions are all Value Classes (extends AnyVal), which means
  * that no new objects get allocated for the `.safe` call (only the TSxx
  * objects get allocated).
  */
object ImplicitConverters {
  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] =
    KeyValue.pair(tuple._1, tuple._2)

  implicit class TSKStreamAuto[K, V]
  (val inner: KStream[K, V])
    extends AnyVal {
    @inline
    def safe: TSKStream[K, V] =
      new TSKStream[K, V](inner)
  }

  implicit class TSKTableAuto[K, V]
  (val inner: KTable[K, V])
    extends AnyVal {
    def safe: TSKTable[K, V] =
      new TSKTable[K, V](inner)
  }

  implicit class TSKGroupedStreamAuto[K, V]
  (val inner: KGroupedStream[K, V])
    extends AnyVal {
    @inline
    def safe: TSKGroupedStream[K, V] =
      new TSKGroupedStream[K, V](inner)
  }

  implicit class TSKGroupedTableAuto[K, V]
  (val inner: KGroupedTable[K, V])
    extends AnyVal {
    @inline
    def safe: TSKGroupedTable[K, V] =
      new TSKGroupedTable[K, V](inner)
  }

  implicit class TSSessionWindowedKStreamAuto[K, V]
  (val inner: SessionWindowedKStream[K, V])
    extends AnyVal {
    @inline
    def safe: TSSessionWindowedKStream[K, V] =
      new TSSessionWindowedKStream[K, V](inner)
  }

  implicit class TSTimeWindowedKStreamAuto[K, V]
  (val inner: TimeWindowedKStream[K, V])
    extends AnyVal {
    @inline
    def safe: TSTimeWindowedKStream[K, V] =
      new TSTimeWindowedKStream[K, V](inner)
  }

  implicit object TSKGroupedStreamAuto
    extends ConverterToTypeSafer[KGroupedStream, TSKGroupedStream] {
    @inline
    override def safe[K, V](src: KGroupedStream[K, V]): TSKGroupedStream[K, V] =
      new TSKGroupedStream(src)
  }

  implicit object TSKStreamAuto
    extends ConverterToTypeSafer[KStream, TSKStream] {
    @inline
    override def safe[K, V](src: KStream[K, V]): TSKStream[K, V] =
      new TSKStream(src)
  }

  implicit object TSKTableAuto
    extends ConverterToTypeSafer[KTable, TSKTable] {
    @inline
    override def safe[K, V](src: KTable[K, V]): TSKTable[K, V] =
      new TSKTable(src)
  }

  implicit object TSKGroupedTableAuto
    extends ConverterToTypeSafer[KGroupedTable, TSKGroupedTable] {
    @inline
    override def safe[K, V](src: KGroupedTable[K, V])
    : TSKGroupedTable[K, V] =
      new TSKGroupedTable(src)
  }

  implicit object TSSessionWindowedKStreamAuto
    extends ConverterToTypeSafer[SessionWindowedKStream,
      TSSessionWindowedKStream] {
    @inline
    override def safe[K, V](src: SessionWindowedKStream[K, V])
    : TSSessionWindowedKStream[K, V] =
      new TSSessionWindowedKStream(src)
  }

  implicit object TSTimeWindowedKStreamAuto
    extends ConverterToTypeSafer[TimeWindowedKStream, TSTimeWindowedKStream] {
    @inline
    override def safe[K, V](src: TimeWindowedKStream[K, V])
    : TSTimeWindowedKStream[K, V] =
      new TSTimeWindowedKStream(src)
  }

}
