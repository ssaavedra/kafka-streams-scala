# A Scala Interface for the Kafka Streams Java API

[![Build Status](https://secure.travis-ci.org/openshine/kafka-streams-scala.png)](http://travis-ci.org/openshine/kafka-streams-scala)

The library wraps Java APIs in Scala thereby providing:

1. type safety for serializers against data types
1. much better type inference in Scala
2. less boilerplate in application code
3. the usual builder-style composition that developers get with the original Java API

The design of the library was inspired by the work started by Alexis
Seigneurin in [this repository](https://github.com/aseigneurin/kafka-streams-scala) and
later continued by Lightbend in [this repository](http://github.com/lightbend/kafka-streams-scala/).

## Quick Start

`kafka-streams-scala` is published and cross-built for Scala `2.11`, and `2.12`, so you can just add the following to your build:

```scala
val kafka_streams_scala_version = "0.1.2"

libraryDependencies ++= Seq("com.openshine" %%
  "kafka-streams-scala" % kafka_streams_scala_version)
```

> Note: `kafka-streams-scala` supports onwards Kafka Streams `1.0.0`.

The API docs for `kafka-streams-scala` is not yet publicly built at
the moment.


## Running the Tests

The library comes with an embedded Kafka server. To run the tests, simply run `sbt testOnly` and all tests will run on the local embedded server.

> The embedded server is started and stopped for every test and takes quite a bit of resources. Hence it's recommended that you allocate more heap space to `sbt` when running the tests. e.g. `sbt -mem 1500`.

```bash
$ sbt -mem 1500
> +clean
> +test
```

## Type Inference and Composition

Here's a sample code fragment using the Scala wrapper library. Compare this with the Scala code from the same [example](https://github.com/confluentinc/kafka-streams-examples/blob/4.0.0-post/src/test/scala/io/confluent/examples/streams/StreamToTableJoinScalaIntegrationTest.scala) in Confluent's repository.

```scala
// Compute the total per region by summing the individual click counts per region.
val clicksPerRegion: TSKTable[String, Long] = userClicksStream

  // Join the stream against the table.
  .leftJoin(userRegionsTable, (clicks: Long, region: String) => (if (region == null) "UNKNOWN" else region, clicks))

  // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
  .map((_, regionWithClicks) => regionWithClicks)

  // Compute the total per region by summing the individual click counts per region.
  .groupByKey
  .reduce(_ + _)
```

> **Notes:** 
> 
> 1. Note that some methods, like `map`, take a two-argument function,
>    for key-value pairs, rather than the more typical single
>    argument.


## Better Abstraction

The wrapped Scala APIs also incur less boilerplate by taking advantage
of Scala function literals that get converted to Java objects in the
implementation of the API. Hence the surface syntax of the client API
looks simpler and less noisy.

Here's an example of a snippet built using the Java API from Scala ..

```scala
val approximateWordCounts: KStream[String, Long] = textLines
  .flatMapValues(value => value.toLowerCase.split("\\W+").toIterable.asJava)
  .transform(
    new TransformerSupplier[Array[Byte], String, KeyValue[String, Long]] {
      override def get() = new ProbabilisticCounter
    },
    cmsStoreName)
approximateWordCounts.to(outputTopic, Produced.`with`(Serdes.String(), longSerde))
```

And here's the corresponding snippet using the Scala library. Note how
the noise of `TransformerSupplier` has been abstracted out by the
function literal syntax of Scala.

```scala
textLines
  .flatMapValues(value => value.toLowerCase.split("\\W+").toIterable)
  .transform(() => new ProbabilisticCounter, cmsStoreName)
  .to(outputTopic)
```

Also, the explicit conversion `asJava` from a Scala `Iterable` to a
Java `Iterable` is done for you by the Scala library.

## Runtime cost

As any adaptor library, running this wrapper may incur into an
overhead.  However, several techniques were followed to try to
minimize the runtime overhead for using this library.

We design the API so that the most overhead goes to the compiler
system and out from the runtime and from the programmer.

1. All the TSKType instances (i.e., the typesafe variants of the
   streams and tables wrapping the Java API) are Value Classes, and
   actual object creation is mostly always avoided. We avoided using
   the `unsafely` API inside our own construction because that would
   entail allocating a TSKStream object in order to use it as a
   parameter for the UnsafelyWrapper case class.

2. All the conversions between KTypes and TSKTypes are done with scala
   objects (thus, statically allocated) implementing a converter
   trait. The converter trait is public, which allows the user to
   define further conversions in a future API version even if this
   library is not yet updated. The conversions are marked @inline, so
   the compiler may inline the conversions in the user code. Given
   that the conversion is to a value class no actual operations are
   performed, the worst-case result (not inlined) would be equivalent
   to calling `identity`.

3. Using `.safe` on a KType will use the aforementioned converter via
   an intermediate implicit class (also a Value Class)conversion. The
   bytecode generated bytecode is equivalent to calling a function
   that calls identity.


## Implicit Serdes

The Java API has, for many methods, two different interfaces
available: one where you use an explicit Serdes-related type
(Consumed, Produced, Materialized, et al.), and another without such
parameter. In the later case, the default Serdes for keys and values
would be used instead.
 
In Java there is no notion of implicit parameters, and the only sane
way to provide custom-value Serdes are using explicit parameters, thus
making the methods signatures more cumbersome.

Fortunately, in Scala we can leverage implicit parameters to make the
compiler work to find out the appropriate instances for any Serde type
that you have configured in your application.

In the interest of type safety, the Scala API only provides a single
interface, where the appropriate Serdes-related type is filled
implicitly as the last parameter.

As a benefit, your application will not compile if you fail to provide
the compiler an appropriate Serde for every operation where you might
need one.

### Usage

In order to use the packaged Serdes, you can just have in your file a

```scala
import com.openshine.kafka.streams.scala.typesafe.SerdeDerivations._
```

This will bring you into scope Serde instances for the default types
(including scala.Long and java.lang.Long, for compatibility) as well
as default implicit definitions that can construct Serde-derived types
(Consumed, Produced, Materialized, Joined) from a Serde. If you want
to customize the definition of such constructors or disallow some from
being brought into scope, take a look at the typesafe.derivations
package. You can create your own custom derivations by extending the
available traits in that package.

A recommended way of working is having your own SerdeDerivations
extending from ours that includes Serde instances for your
domain-specific data types.


### Customization

If you need to pass a customized certain Materialized instance (for
example) you can always provide an explicit instance for such a case.

### Criticism

This means that you will never use the default serdes that you may
have configured in the StreamsConfig. Given that the resulting
implementation performs equivalently by definition, we seem this
argument as not constructive.


### Examples

1. The example
   [StreamToTableJoinScalaIntegrationTestImplicitSerdes](https://github.com/openshine/kafka-streams-scala/blob/develop/src/test/scala/com/openshine/kafka/streams/scala/StreamToTableJoinScalaIntegrationTestImplicitSerdes.scala)
   demonstrates how to use the technique of implicit `Serde`s
2. The example
   [StreamToTableJoinScalaIntegrationTestImplicitSerialized](https://github.com/openshine/kafka-streams-scala/blob/develop/src/test/scala/com/openshine/kafka/streams/scala/StreamToTableJoinScalaIntegrationTestImplicitSerialized.scala)
   demonstrates how to use the technique of implicit `Serialized`,
   `Consumed` and `Produced`.
3. The example
   [StreamToTableJoinScalaIntegrationTestMixImplicitSerialized](https://github.com/openshine/kafka-streams-scala/blob/develop/src/test/scala/com/openshine/kafka/streams/scala/StreamToTableJoinScalaIntegrationTestMixImplicitSerialized.scala)
   demonstrates how to use the technique of how to use default serdes
   along with implicit `Serialized`, `Consumed` and `Produced`.
