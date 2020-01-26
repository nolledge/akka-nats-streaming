package akka.stream.alpakka.nats

import akka.actor.ActorSystem
import akka.stream.alpakka.nats.javadsl.NatsStreamingSimpleSource
import akka.stream.alpakka.nats.scaladsl.NatsStreamingSimpleSink
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class NatsFlowTest extends AnyWordSpec with Matchers {

  implicit val as: ActorSystem = ActorSystem()
  implicit val mat: Materializer = Materializer(as)
  implicit val ec: ExecutionContext = as.dispatcher

  private val connectionBuilder: NatsStreamingConnectionBuilder =
    NatsStreamingConnectionBuilder.fromSettings(
      settings = NatsStreamingConnectionSettings(
        clusterId = "test-cluster",
        clientId = "clientId",
        url = "nats://myhost:4221",
        connectionTimeout = None,
        publishAckTimeout = None,
        publishMaxInFlight = None,
        discoverPrefix = None
      )
    )
  val source = NatsStreamingSimpleSource.create(
    SimpleSubscriptionSettings(
      cp = connectionBuilder,
      subjects = List("test-subject"),
      subscriptionQueue = "testQueue",
      durableSubscriptionName = None,
      startPosition = DeliveryStartPosition.AllAvailable,
      subMaxInFlight = None,
      bufferSize = 100,
      autoRequeueTimeout = None,
      manualAcks = false,
      closeConnectionAfterStop = true
    )
  )

  val sink = NatsStreamingSimpleSink(
    PublishingSettings(
      cp = connectionBuilder,
      defaultSubject = "test-subject",
      parallel = false,
      closeConnectionAfterStop = false
    )
  )

  case class TestStructure(payload: String)

  private def serialize(d: TestStructure): Array[Byte] = d.payload.getBytes
  private def deserialize(d: Array[Byte]): TestStructure = TestStructure(new String(d))

  "An emitted event" must {
    "be received by the source" in {
      for {
        _ <- Source(List(TestStructure("TestEvent")))
          .map(serialize)
          .map(d => OutgoingMessage(d))
          .runWith(sink)
        _ <- source
            .map[TestStructure](d => deserialize(d.data))
            .runWith(Sink.head[TestStructure])
      } yield ???
    }
  }
}
