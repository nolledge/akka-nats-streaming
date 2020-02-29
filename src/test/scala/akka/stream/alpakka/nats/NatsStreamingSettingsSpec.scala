package akka.stream.alpakka.nats

import java.time.{Duration, Instant}

import akka.stream.alpakka.nats.DeliveryStartPosition.{
  AfterSequenceNumber,
  AfterTime,
  AllAvailable,
  LastReceived,
  OnlyNew
}
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
  val simpleSubSettings = SimpleSubscriptionSettings(
    subjects = List("sub1", "sub2"),
    subscriptionQueue = "subQueue",
    durableSubscriptionName = Some("durableName"),
    startPosition = AfterSequenceNumber(10),
    subMaxInFlight = Some(100),
    bufferSize = 100,
    autoRequeueTimeout = Some(Duration.ofSeconds(30)),
    manualAcks = true
  )
  val connectionConf = ConfigFactory.parseResources("connection.conf")
  val subscriptionConf = ConfigFactory.parseResources("subscription.conf")

  "NatsStreamingConnectionSettings" should {
    "be loaded from config as expected" in {
      NatsStreamingConnectionSettings.fromConfig(
        connectionConf.getConfig("connection-url")
      ) shouldBe connectionSettings
    }
    "be loaded from config as expected with host and port configured" in {
      NatsStreamingConnectionSettings.fromConfig(
        connectionConf.getConfig("connection-host-port")
      ) shouldBe connectionSettings.copy(url = urlHostPort)
    }
  }

  "SimpleSubscriptionSettings" should {
    "be loaded from config as expected with after seq nr. delivery" in {
      SimpleSubscriptionSettings.fromConfig(
        subscriptionConf.getConfig("after-seq")
      ) shouldBe simpleSubSettings
    }
    "be loaded from config as expected with after millis" in {
      SimpleSubscriptionSettings.fromConfig(
        subscriptionConf.getConfig("deliver-after-epoch")
      ) shouldBe simpleSubSettings.copy(
        startPosition = AfterTime(Instant.ofEpochMilli(100))
      )
    }
    "be loaded from config as expected deliver only new" in {
      SimpleSubscriptionSettings.fromConfig(
        subscriptionConf.getConfig("deliver-only-new")
      ) shouldBe simpleSubSettings.copy(
        startPosition = OnlyNew
      )
    }
    "be loaded from config as expected deliver all available" in {
      SimpleSubscriptionSettings.fromConfig(
        subscriptionConf.getConfig("deliver-all-available")
      ) shouldBe simpleSubSettings.copy(
        startPosition = AllAvailable
      )
    }
    "be loaded from config as expected deliver last received" in {
      SimpleSubscriptionSettings.fromConfig(
        subscriptionConf.getConfig("deliver-last-received")
      ) shouldBe simpleSubSettings.copy(
        startPosition = LastReceived
      )
    }
  }

}
