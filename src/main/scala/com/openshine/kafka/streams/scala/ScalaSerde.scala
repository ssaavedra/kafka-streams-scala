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

package com.openshine.kafka.streams.scala

import java.util

import org.apache.kafka.common.serialization.{Deserializer => JDeserializer, Serde => JSerde, Serializer => JSerializer}

/** A scala.Serde is just a [[JSerde]] with direct methods for serializing
  * and deserializing values.
  *
  * @tparam T the type this serde is able to ser/de
  */
trait Serde[T] extends JSerde[T] {
  def serialize(data: T): Array[Byte]
  def deserialize(data: Array[Byte]): Option[T]

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
    ()

  override def close(): Unit = ()
}

/** An adaptor for Java-based serdes so that they meet the Serde interface
  *
  * @param inner the [[JSerde]] instance to adapt
  * @tparam T the type this serde is able to ser/de
  */
class JavaSerdeWrapper[T >: Null](val inner: JSerde[T]) extends Serde[T] {

  /** Override this value to change the topic name on the
    * serialize/deserialize calls
    */
  def topic: String = "unknown-topic"

  override def configure(
      configs: java.util.Map[String, _],
      isKey: Boolean
  ): Unit =
    inner.configure(configs, isKey)

  override def deserializer(): JDeserializer[T] = inner.deserializer()

  override def serializer(): JSerializer[T] = inner.serializer()

  override def close(): Unit = inner.close()

  override def deserialize(data: Array[Byte]): Option[T] = {
    val d      = inner.deserializer()
    val result = Option(d.deserialize(topic, data))
    d.close()
    result
  }

  override def serialize(data: T): Array[Byte] = {
    val s      = inner.serializer()
    val result = s.serialize(topic, data)
    s.close()
    result
  }
}

trait StatelessScalaSerde[T >: Null] extends Serde[T] { self =>
  override def deserializer(): JDeserializer[T] = new Deserializer[T] {
    override def deserialize(
      data: Array[Byte]
    ): Option[T] = self.deserialize(data)
  }

  override def serializer(): JSerializer[T] = new Serializer[T] {
    override def serialize(data: T): Array[Byte] = self.serialize(data)
  }
}

trait Deserializer[T >: Null] extends JDeserializer[T] {
  override def configure(
      configs: java.util.Map[String, _],
      isKey: Boolean
  ): Unit = ()

  override def close(): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): T =
    Option(data).flatMap(deserialize).orNull

  def deserialize(data: Array[Byte]): Option[T]
}

trait Serializer[T] extends JSerializer[T] {
  override def configure(
      configs: java.util.Map[String, _],
      isKey: Boolean
  ): Unit = ()

  override def close(): Unit = ()

  override def serialize(topic: String, data: T): Array[Byte] =
    Option(data).map(serialize).orNull

  def serialize(data: T): Array[Byte]
}
