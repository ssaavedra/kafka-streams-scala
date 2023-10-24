package com.openshine.kafka.streams.scala.typesafe

import com.openshine.kafka.streams.scala.typesafe.ImplicitConverters._
import org.apache.kafka.streams.kstream.{Branched, KStream}

class TSBranched[K, V](override protected[typesafe] val unsafe: Branched[K, V])
  extends AnyVal
    with TSKType[Branched, K, V] {
  def withName(name: String) = new TSBranched(unsafe.withName(name))
}

object TSBranched {
  def as[K, V](name: String): TSBranched[K, V] = new TSBranched[K, V](Branched.as(name))

  def withFunction[K, V](chain: (TSKStream[K, V] => TSKStream[K, V]), name: String = null) =
    new TSBranched[K, V](Branched.withFunction((ks: KStream[K, V]) => chain(ks.safe).unsafe, name))

  def withConsumer[K, V](consumer: TSKStream[K, V] => Unit, name: String = null) =
    new TSBranched[K, V](Branched.withConsumer((ks: KStream[K, V]) => consumer(ks.safe), name))

}
