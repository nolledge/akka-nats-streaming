package akka.stream.alpakka.nats.scaladsl

import akka.Done
import akka.stream.alpakka.nats.{
  NatsStreamingSimpleSinkStage,
  OutgoingMessage,
  PublishingSettings
}
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config
import io.nats.streaming.StreamingConnection

import scala.concurrent.Future

object NatsStreamingSimpleSink {
  def apply(
      connection: StreamingConnection,
      settings: PublishingSettings
  ): Sink[OutgoingMessage[Array[Byte]], Future[Done]] =
    Sink.fromGraph(new NatsStreamingSimpleSinkStage(connection, settings))

  def apply(
      connection: StreamingConnection,
      config: Config
  ): Sink[OutgoingMessage[Array[Byte]], Future[Done]] =
    apply(connection, PublishingSettings.fromConfig(config))
}
