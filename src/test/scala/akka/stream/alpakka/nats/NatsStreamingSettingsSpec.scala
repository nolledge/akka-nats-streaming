package akka.stream.alpakka.nats

import java.time.Duration

import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class NatsStreamingSettingsSpec extends AnyWordSpec with Matchers {

  val clusterId = "clusterId"
  val clientId = "clientId"
  val durableName = "durableName"
  val url = "url"
  val urlHostPort = "nats://host:4200"
  val connectionTimeout = Some(Duration.ofSeconds(30))
  val publishAckTimeout = Some(Duration.ofSeconds(1))
  val publishMaxInFlight = Some(50)
  val discoverPrefix = Some("discoverPrefix")

  val connectionSettings = NatsStreamingConnectionSettings(
    clusterId = clusterId,
    clientId = clientId,
    url = url,
    connectionTimeout = connectionTimeout,
    publishAckTimeout = publishAckTimeout,
    publishMaxInFlight = publishMaxInFlight,
    discoverPrefix = discoverPrefix
  )

  "NatsStreamingSettings" should {
    "be loaded from config as expected" in {
      NatsStreamingConnectionSettings.fromConfig(
        ConfigFactory.parseResources("connection.conf")
      ) shouldBe connectionSettings
    }
    "be loaded from config as expected with host and port configured" in {
      NatsStreamingConnectionSettings.fromConfig(
        ConfigFactory.parseResources("connection_host_port.conf")
      ) shouldBe connectionSettings.copy(url = urlHostPort)
    }
  }

}
