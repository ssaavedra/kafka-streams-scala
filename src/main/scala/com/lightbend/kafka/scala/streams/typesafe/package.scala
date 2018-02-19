package com.lightbend.kafka.scala.streams

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.state.{KeyValueStore, SessionStore, WindowStore}

/** Provides a more type-safe interface for Kafka Streams when using Scala.
  *
  * In particular, it always requires a Materialized instance to be provided
  * to aggregating functions that may persist data, so that you won't ever
  * find yourself using the default Serde and thus converting runtime
  * exceptions in compile-time errors.
  *
  * You don't need to create all Materialized (and other persisting helper)
  * objects, as they can be deduced by the Scala type system when you have
  * the implicit resolution rules available at
  * [[com.lightbend.kafka.scala.streams.typesafe.SerdeDerivations]] in scope.
  *
  * @author Santiago Saavedra (ssaavedra@openshine.com)
  */
package object typesafe {
  private[typesafe] type kvs = KeyValueStore[Bytes, Array[Byte]]
  private[typesafe] type ssb = SessionStore[Bytes, Array[Byte]]
  private[typesafe] type wsb = WindowStore[Bytes, Array[Byte]]
}
