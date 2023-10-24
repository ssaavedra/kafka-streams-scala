package com.openshine.kafka.streams.scala.typesafe

import com.openshine.kafka.streams.scala.FunctionConversions._
import com.openshine.kafka.streams.scala.typesafe.ImplicitConverters._
import org.apache.kafka.streams.kstream.{Branched, BranchedKStream}

import scala.collection.JavaConverters._

class TSBranchedKStream[K, V](override protected[typesafe] val unsafe: BranchedKStream[K, V])
  extends AnyVal
  with TSKType[BranchedKStream, K, V] {
  def branch(predicate: (K, V) => Boolean): TSBranchedKStream[K, V] = {
    unsafe.branch(predicate.asPredicate).safe
  }

  def branch(predicate: (K, V) => Boolean, branched: Branched[K, V]): TSBranchedKStream[K, V] = {
    unsafe.branch(predicate.asPredicate, branched).safe
  }

  def defaultBranch(): Map[String, TSKStream[K, V]] =
    unsafe.defaultBranch().asScala.mapValues(_.safe).toMap

  def defaultBranch(branched: TSBranched[K, V]): Map[String, TSKStream[K, V]] =
    unsafe.defaultBranch(branched.unsafe).asScala.mapValues(_.safe).toMap

  def noDefaultBranch(): Map[String, TSKStream[K, V]] =
    unsafe.noDefaultBranch().asScala.mapValues(_.safe).toMap
}
