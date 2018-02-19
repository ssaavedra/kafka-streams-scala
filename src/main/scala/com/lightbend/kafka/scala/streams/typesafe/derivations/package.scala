package com.lightbend.kafka.scala.streams.typesafe

import java.nio.ByteBuffer

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.kstream.{Joined, Materialized, Produced,
  Serialized}
import org.apache.kafka.streams.processor.StateStore

/** Many default derivations, provided as standalone universal traits that
  * can be mixed in anywhere.
  *
  * Also provides a [[derivations.Default]] object that can be used directly
  * extending all these mix-in traits.
  *
  * @author Santiago Saavedra (ssaavedra@openshine.com)
  */
package object derivations {

  trait materialized extends Any {
    implicit def materializedFromSerdes[K, V, S <: StateStore]
    (implicit key: Serde[K], value: Serde[V]): Materialized[K, V, S] = {
      util.Materialized[K, V]
    }
  }

  trait serialized extends Any {
    implicit def serializedFromSerdes[K, V](implicit key: Serde[K],
                                            value: Serde[V])
    : Serialized[K, V] = {
      Serialized.`with`(key, value)
    }
  }

  trait produced extends Any {
    implicit def producedFromSerdes[K, V](implicit key: Serde[K],
                                          value: Serde[V])
    : Produced[K, V] = {
      Produced.`with`(key, value)
    }
  }

  trait consumed extends Any {
    implicit def consumedFromSerdes[K, V](implicit key: Serde[K],
                                          value: Serde[V])
    : Consumed[K, V] = {
      Consumed.`with`(key, value)
    }
  }

  trait joined extends Any {
    implicit def joinedFromSerdes[K, V, VO](implicit key: Serde[K],
                                            value: Serde[V], value2: Serde[VO])
    : Joined[K, V, VO] = {
      Joined.`with`(key, value, value2)
    }
  }

  trait defaultSerdes {
    implicit val ScalaLongSerde: Serde[Long] =
      Serdes.Long().asInstanceOf[Serde[Long]]

    implicit val ScalaDoubleSerde: Serde[Double] =
      Serdes.Double().asInstanceOf[Serde[Double]]

    implicit val stringSerde: Serde[String] = Serdes.String()
    implicit val javaLongSerde: Serde[java.lang.Long] = Serdes.Long()
    implicit val javaFloatSerde: Serde[java.lang.Float] = Serdes.Float()
    implicit val javaDoubleSerde: Serde[java.lang.Double] = Serdes.Double()
    implicit val byteArraySerde: Serde[Array[Byte]] = Serdes.ByteArray()
    implicit val byteBufferSerde: Serde[ByteBuffer] = Serdes.ByteBuffer()
  }

  trait Default
    extends serialized
      with produced
      with consumed
      with joined
      with materialized
      with defaultSerdes

  object Default extends Default

}
