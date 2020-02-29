package akka.stream.alpakka.nats

import java.time.Duration
import java.util.UUID

import scala.concurrent.duration._
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.nats.scaladsl.NatsStreamingSimpleSink
import akka.stream.Materializer
import akka.stream.alpakka.nats.scaladsl.NatsStreamingSourceWithAck
import akka.stream.scaladsl.{Sink, Source}
import io.nats.client.{Connection, ConnectionListener, Consumer, ErrorListener}
import io.nats.streaming.StreamingConnection
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}

class NatsFlowTest
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit val as: ActorSystem = ActorSystem()
  implicit val mat: Materializer = Materializer(as)
  implicit val ec: ExecutionContext = as.dispatcher

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 10.seconds, interval = 100.milliseconds)

  private val connection: StreamingConnection =
    NatsStreamingConnectionBuilder
      .fromSettings(
        settings = NatsStreamingConnectionSettings(
          clusterId = "test-cluster",
          clientId = "clientId",
          url = "nats://localhost:4222",
          connectionTimeout = None,
          publishAckTimeout = None,
          publishMaxInFlight = None,
          discoverPrefix = None
        )
      )
      .connection(
        (_: Connection, _: ConnectionListener.Events) => (),
        new ErrorListener {
          override def errorOccurred(conn: Connection, error: String): Unit = ()

          override def exceptionOccurred(
              conn: Connection,
              exp: Exception
          ): Unit = ()

          override def slowConsumerDetected(
              conn: Connection,
              consumer: Consumer
          ): Unit = ()
        }
      )

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
      val result = for {
        _ <- Source(List(testData))
          .map(serialize)
          .map(d => OutgoingMessage(d, sub))
          .runWith(sink(sub))
        res <- source(sub)
          .map[TestStructure](d => deserialize(d.data))
          .runWith(Sink.head[TestStructure])
      } yield res
      whenReady(result) { r => r mustBe testData }
    }
  }
  "The NatsSource" must {
    "resume a durable connection" in {
      val sub = subject
      val result = for {
        _ <- Source(1.to(10).map(i => TestStructure(i.toString)))
          .map(serialize)
          .map(d => OutgoingMessage(d, sub))
          .runWith(sink(sub))
        first <- source(sub, durable = true)
          .wireTap(_.ack())
          .take(5)
          .map[TestStructure](d => deserialize(d.data))
          .runWith(Sink.seq[TestStructure])
        res <- source(sub, durable = true)
          .wireTap(_.ack())
          .map[TestStructure](d => deserialize(d.data))
          .takeWithin(2.second)
          .runWith(Sink.seq[TestStructure])
        _ = println(first)
        _ = println(res)
      } yield res
      whenReady(result) { r => r.size mustBe 5 }
    }
  }

  override def afterAll() = {
    super.afterAll()
    connection.close()
  }
}
