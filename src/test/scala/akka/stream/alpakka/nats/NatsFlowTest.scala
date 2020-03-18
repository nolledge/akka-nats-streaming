package akka.stream.alpakka.nats

import java.time.Duration
import java.util.UUID

import akka.stream.alpakka.nats.scaladsl.{
  NatsStreamingSimpleSink,
  NatsStreamingSimpleSource,
  NatsStreamingSourceWithAck
}
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}

import scala.concurrent.Future
import scala.concurrent.duration._

class NatsFlowTest extends FunctionalTestBase {

  def source(
      subject: String,
      durable: Boolean = false
  ): Source[IncomingMessageWithAck[Array[Byte]], NotUsed] =
    NatsStreamingSourceWithAck(
      connection,
      SubscriptionWithAckSettings(
        subjects = List(subject),
        subscriptionQueue = "testQueue",
        durableSubscriptionName = if (durable) Some("durable") else None,
        startPosition = DeliveryStartPosition.AllAvailable,
        subMaxInFlight = None,
        bufferSize = 100,
        autoRequeueTimeout = Some(Duration.ofSeconds(1)),
        manualAcks = true,
        manualAckTimeout = Duration.ofSeconds(1)
      )
    )

  def sink(subject: String): Sink[OutgoingMessage[Array[Byte]], Future[Done]] =
    NatsStreamingSimpleSink(
      connection,
      PublishingSettings(defaultSubject = subject, parallel = false)
    )

  case class TestStructure(payload: String)

  private def serialize(d: TestStructure): Array[Byte] = d.payload.getBytes
  private def deserialize(d: Array[Byte]): TestStructure =
    TestStructure(new String(d))

  private def subject: String = UUID.randomUUID().toString

  val testData = TestStructure("some data")

  "An event" must {
    "be published by the sink and received by the source" in {
      val sub = subject
      whenReady(for {
        _ <- Source(List(testData))
          .map(serialize)
          .map(d => OutgoingMessage(d, sub))
          .runWith(sink(sub))
        res <- source(sub)
          .map[TestStructure](d => deserialize(d.data))
          .runWith(Sink.head[TestStructure])
      } yield res) { r => r mustBe testData }
    }
  }
  "The NatsSource" must {
    "resume a durable connection" in {
      val sub = subject
      whenReady(for {
        _ <- Source(1.to(10).map(i => TestStructure(i.toString)))
          .map(serialize)
          .map(d => OutgoingMessage(d, sub))
          .runWith(sink(sub))
        _ <- source(sub, durable = true)
          .wireTap(_.ack())
          .take(5)
          .map[TestStructure](d => deserialize(d.data))
          .runWith(Sink.seq[TestStructure])
        res <- source(sub, durable = true)
          .wireTap(_.ack())
          .map[TestStructure](d => deserialize(d.data))
          .takeWithin(2.second)
          .runWith(Sink.seq[TestStructure])
      } yield res) { r => r.size mustBe 5 }
    }
  }

  "The SimpleSource" must {
    "acknowledge incoming messages right away" in {
      val sub = subject
      val simpleSource = NatsStreamingSimpleSource(
        connection,
        SimpleSubscriptionSettings(
          subjects = List(sub),
          subscriptionQueue = "testQueue",
          durableSubscriptionName = None,
          startPosition = DeliveryStartPosition.AllAvailable,
          subMaxInFlight = None,
          bufferSize = 100,
          autoRequeueTimeout = Some(Duration.ofSeconds(1)),
          manualAcks = true
        )
      )
      whenReady(for {
        _ <- Source(1.to(10).map(i => TestStructure(i.toString)))
          .map(serialize)
          .map(d => OutgoingMessage(d, sub))
          .runWith(sink(sub))
        res <- simpleSource
          .map[TestStructure](d => deserialize(d.data))
          .takeWithin(2.second)
          .runWith(Sink.seq)
      } yield res) {
        _.length mustBe 10
      }
    }
  }

}
