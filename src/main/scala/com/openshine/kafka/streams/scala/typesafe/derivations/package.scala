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

import java.nio.ByteBuffer
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Joined, Materialized, Produced}
import org.apache.kafka.streams.processor.{StateStore, StreamPartitioner}

import scala.collection.JavaConverters._
import java.util.Optional

/** Many default derivations, provided as standalone universal traits that
  * can be mixed in anywhere.
  *
  * Also provides a [[derivations.Default]] object that can be used directly
  * extending all these mix-in traits.
  */
package object derivations {

  trait materialized extends Any {
    implicit def materializedFromSerdes[K, V, S <: StateStore](
        implicit key: Serde[K],
        value: Serde[V]
    ): Materialized[K, V, S] = {
      Materialized.`with`(key, value)
    }
  }

  trait produced extends Any {
    implicit def producedFromSerdes[K, V](
        implicit key: Serde[K],
        value: Serde[V]
    ): Produced[K, V] = {
      Produced.`with`(key, value)
    }
  }

  trait consumed extends Any {
    implicit def consumedFromSerdes[K, V](
        implicit key: Serde[K],
        value: Serde[V],
    ): Consumed[K, V] = {
      Consumed.`with`(key, value)
    }
  }

  trait grouped extends Any {
    implicit def groupedFromSerdes[K, V](
        implicit key: Serde[K],
        value: Serde[V]
    ): Grouped[K, V] = {
      Grouped.`with`(key, value)
    }
  }

  trait joined extends Any {
    implicit def joinedFromSerdes[K, V, VO](
        implicit key: Serde[K],
        value: Serde[V],
        value2: Serde[VO]
    ): Joined[K, V, VO] = {
      Joined.`with`(key, value, value2)
    }
  }

  trait repartitioned extends Any {
    implicit def repartitionedFromSerdes[K, V](
        implicit key: Serde[K],
        value: Serde[V]
    ): TSRepartitioned[K, V] =
      TSRepartitioned.fromSerde(key, value)
  }

  trait streampartitioner extends Any {
    implicit def streamPartitionerFromFunction[K, V](
        implicit f: (String, K, V, Int) => Option[Set[Int]]
    ): StreamPartitioner[K, V] =
      new StreamPartitioner[K, V] {
        override def partition(topic: String, key: K, value: V, numPartitions: Int): Integer =
          f(topic, key, value, numPartitions).flatMap(_.headOption.map(Integer.valueOf)).orNull
        override def partitions(topic: String, key: K, value: V, numPartitions: Int): Optional[java.util.Set[Integer]] = {
          val r = f(topic, key, value, numPartitions)
          r.map(_.map(Integer.valueOf).asJava) match {
            case Some(x) => Optional.of(x)
            case None => Optional.empty()
          }
        }
      }
  }

  trait defaultSerdes {
    implicit val scalaLongSerde: Serde[Long] =
      Serdes.Long().asInstanceOf[Serde[Long]]

    implicit val scalaDoubleSerde: Serde[Double] =
      Serdes.Double().asInstanceOf[Serde[Double]]

    implicit val stringSerde: Serde[String]               = Serdes.String()
    implicit val javaLongSerde: Serde[java.lang.Long]     = Serdes.Long()
    implicit val javaFloatSerde: Serde[java.lang.Float]   = Serdes.Float()
    implicit val javaDoubleSerde: Serde[java.lang.Double] = Serdes.Double()
    implicit val byteArraySerde: Serde[Array[Byte]]       = Serdes.ByteArray()
    implicit val byteBufferSerde: Serde[ByteBuffer]       = Serdes.ByteBuffer()
  }

  trait Default
      extends defaultSerdes
      with produced
      with consumed
      with grouped
      with joined
      with materialized
      with repartitioned

  object Default extends Default

}
