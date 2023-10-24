/*
 * Copyright 2018 OpenShine SL <https://www.openshine.com>
 * Copyright 2018 Lightbend Inc. <https://www.lightbend.com>
 * Copyright 2017-2018 Alexis Seigneurin.
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

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._

/**
  * Implicit classes that offer conversions of Scala function literals to
  * SAM (Single Abstract Method) objects in Java. These make the Scala APIs much
  * more expressive, with less boilerplate and more succinct.
  */
object FunctionConversions {

  implicit class PredicateFromFunction[K, V](val test: (K, V) => Boolean)
      extends AnyVal {
    def asPredicate: Predicate[K, V] = test(_, _)
  }

  implicit class MapperFromFunction[T, U, V](val f: (T, U) => V)
      extends AnyVal {
    def asKeyValueMapper: KeyValueMapper[T, U, V] = (k: T, v: U) => f(k, v)
    def asValueJoiner: ValueJoiner[T, U, V]       = (v1, v2) => f(v1, v2)
  }

  implicit class MapperFromFunction3[T, K, U, V](val f: (K, T, U) => V)
      extends AnyVal {
    def asKeyValueJoiner: ValueJoinerWithKey[K, T, U, V] = (k, v1, v2) => f(k, v1, v2)
  }

  implicit class KeyValueMapperFromFunction[K, V, KR, VR](
      val f: (K, V) => (KR, VR)
  ) extends AnyVal {
    def asKeyValueMapper: KeyValueMapper[K, V, KeyValue[KR, VR]] = (k, v) => {
      val (kr, vr) = f(k, v)
      KeyValue.pair(kr, vr)
    }
  }

  implicit class ValueMapperFromFunction[V, VR](val f: V => VR)
      extends AnyVal {
    def asValueMapper: ValueMapper[V, VR] = v => f(v)
  }

  implicit class AggregatorFromFunction[K, V, VR](val f: (K, V, VR) => VR)
      extends AnyVal {
    def asAggregator: Aggregator[K, V, VR] = (k, v, r) => f(k, v, r)
  }

  implicit class MergerFromFunction[K, VR](val f: (K, VR, VR) => VR)
      extends AnyVal {
    def asMerger: Merger[K, VR] = (k, v1, v2) => f(k, v1, v2)
  }

  implicit class ReducerFromFunction[V](val f: (V, V) => V) extends AnyVal {
    def asReducer: Reducer[V] = (v1, v2) => f(v1, v2)
  }

  implicit class InitializerFromNameRef[T](f: => T) {
    def asInitializer: Initializer[T] = () => f
  }

  implicit class InitializerFromFunction[T](val f: () => T) extends AnyVal {
    def asInitializer: Initializer[T] = () => f()
  }

}
