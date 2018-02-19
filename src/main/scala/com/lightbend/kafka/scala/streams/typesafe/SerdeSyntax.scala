package com.lightbend.kafka.scala.streams.typesafe

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.language.implicitConversions

/** Adds syntax for defining a Serde as a tuple of Serializer plus
  * Deserializer, as well as defining any of those with a [[Function1]] in
  * the simple case.
  *
  * @author Santiago Saavedra (ssaavedra@openshine.com)
  */
trait SerdeSyntax {

  implicit def tuple2serde[T](t: (Serializer[T], Deserializer[T])): Serde[T] = {
    new Serde[T] {
      override def deserializer(): Deserializer[T] = t._2

      override def serializer(): Serializer[T] = t._1

      override def configure(configs: java.util.Map[String, _],
                             isKey: Boolean): Unit = {
        t._1.configure(configs, isKey)
        t._2.configure(configs, isKey)
      }

      override def close(): Unit = {
        t._1.close()
        t._2.close()
      }
    }
  }

  implicit class FunctionAsSerializer[T](f: T => Array[Byte]) {
    def asSerializer: Serializer[T] = new Serializer[T] {
      override def configure(configs: java.util.Map[String, _], isKey: Boolean)
      : Unit = {}

      override def serialize(topic: String,
                             data: T): Array[Byte] = {
        f(data)
      }

      override def close(): Unit = {}
    }
  }

  implicit class OptionalFunctionAsDeserializer[T](f: Array[Byte] => Option[T])
                                                  (implicit ev: Null <:< T) {
    def asDeserializer: Deserializer[T] = new Deserializer[T] {
      override def configure(configs: java.util.Map[String, _], isKey: Boolean)
      : Unit = {}

      override def close(): Unit = {}

      override def deserialize(topic: String,
                               data: Array[Byte]): T = {
        f(data).orNull
      }
    }
  }

  implicit class FunctionAsDeserializer[T](f: Array[Byte] => T) {
    def asDeserializer: Deserializer[T] = new Deserializer[T] {
      override def configure(configs: java.util.Map[String, _], isKey: Boolean)
      : Unit = {}

      override def close(): Unit = {}

      override def deserialize(topic: String,
                               data: Array[Byte]): T = {
        f(data)
      }
    }
  }

}

object SerdeSyntax extends SerdeSyntax