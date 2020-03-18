package akka.stream.alpakka.nats

import akka.actor.ActorSystem
import akka.stream.Materializer
import io.nats.client.{Connection, ConnectionListener, Consumer, ErrorListener}
import io.nats.streaming.StreamingConnection
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext

class FunctionalTestBase
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit val as: ActorSystem = ActorSystem()
  implicit val mat: Materializer = Materializer(as)
  implicit val ec: ExecutionContext = as.dispatcher

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 10.seconds, interval = 100.milliseconds)

  val clientId = "client" + System.currentTimeMillis()

  val connection: StreamingConnection =
    NatsStreamingConnectionBuilder
      .fromSettings(
        settings = NatsStreamingConnectionSettings(
          clusterId = "test-cluster",
          clientId = clientId,
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

  override def afterAll() = {
    super.afterAll()
    connection.close()
  }

}
