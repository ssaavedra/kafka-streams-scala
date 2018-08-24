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

import org.apache.kafka.streams.kstream._

import scala.language.higherKinds

/** Conversions to keep the underlying abstraction from leaking. These allow
  * us to always return a TS object instead of the underlying one.
  *
  * These conversions are all Value Classes (extends AnyVal), which means
  * that no new objects get allocated for the `.safe` call (only the TSxx
  * objects get allocated).
  */
object ImplicitConverters {

  /** The trait for conversions between source org.apache.kafka types and
    *  typesafe items. This only works for Key-Value types (i.e.,
    *  higher-order types of two parameters).
    *
    * @tparam Src the source (unsafe) type
    * @tparam Dst the destination (typesafe) type
    */
  trait ConverterToTypeSafer[-Src[K, V], +Dst[K, V]] {
    def apply[K, V](src: Src[K, V]): Dst[K, V]
  }

  implicit class KTypeSafeProvider[K, V, Src[_, _]](val inner: Src[K, V])
      extends AnyVal {
    @inline
    def safe[Dst[_, _]](
        implicit wrap: ConverterToTypeSafer[Src, Dst]
    ): Dst[K, V] =
      wrap(inner)
  }

  implicit object TSKGroupedStreamAuto
      extends ConverterToTypeSafer[KGroupedStream, TSKGroupedStream] {
    @inline
    override def apply[K, V](
        src: KGroupedStream[K, V]
    ): TSKGroupedStream[K, V] =
      new TSKGroupedStream(src)
  }

  implicit object TSKStreamAuto
      extends ConverterToTypeSafer[KStream, TSKStream] {
    @inline
    override def apply[K, V](src: KStream[K, V]): TSKStream[K, V] =
      new TSKStream(src)
  }

  implicit object TSKTableAuto
      extends ConverterToTypeSafer[KTable, TSKTable] {
    @inline
    override def apply[K, V](src: KTable[K, V]): TSKTable[K, V] =
      new TSKTable(src)
  }

  implicit object TSKGroupedTableAuto
      extends ConverterToTypeSafer[KGroupedTable, TSKGroupedTable] {
    @inline
    override def apply[K, V](
        src: KGroupedTable[K, V]
    ): TSKGroupedTable[K, V] =
      new TSKGroupedTable(src)
  }

  implicit object TSSessionWindowedKStreamAuto
      extends ConverterToTypeSafer[
        SessionWindowedKStream,
        TSSessionWindowedKStream
      ] {
    @inline
    override def apply[K, V](
        src: SessionWindowedKStream[K, V]
    ): TSSessionWindowedKStream[K, V] =
      new TSSessionWindowedKStream(src)
  }

  implicit object TSTimeWindowedKStreamAuto
      extends ConverterToTypeSafer[TimeWindowedKStream, TSTimeWindowedKStream] {
    @inline
    override def apply[K, V](
        src: TimeWindowedKStream[K, V]
    ): TSTimeWindowedKStream[K, V] =
      new TSTimeWindowedKStream(src)
  }

}
