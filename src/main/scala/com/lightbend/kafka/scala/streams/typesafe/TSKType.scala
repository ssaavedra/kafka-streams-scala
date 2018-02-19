package com.lightbend.kafka.scala.streams.typesafe

import scala.language.higherKinds

/** A base type for all base stream classes in the typesafe API.
  *
  * @author Santiago Saavedra (ssaavedra@openshine.com)
  */
trait TSKType[T[_, _], K, V] extends Any {
  protected[typesafe] def unsafe: T[K, V]
}
