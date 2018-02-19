package com.lightbend.kafka.scala.streams.typesafe


/** These are implicit serdes that can be mixed-in into your Kafka Streams
  * job, making it easier to work from scala.
  *
  * Objects such as [[util.Materialized]],
  * [[org.apache.kafka.streams.Consumed]],
  * [[org.apache.kafka.streams.kstream.Produced]] or
  * [[org.apache.kafka.streams.kstream.Joined]] need to be created from
  * [[org.apache.kafka.common.serialization.Serde]] instances according to the
  * types you are handling. When including this class, you will automatically
  * generate these objects from available implicit Serde instances.
  *
  * @author Santiago Saavedra (ssaavedra@openshine.com)
  */
trait SerdeDerivations extends derivations.Default

object SerdeDerivations extends SerdeDerivations
