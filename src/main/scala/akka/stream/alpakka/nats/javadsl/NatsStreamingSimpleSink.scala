package akka.stream.alpakka.nats.javadsl

import akka.Done
import akka.stream.alpakka.nats.{
  NatsStreamingSimpleSinkStage,
  OutgoingMessage,
  PublishingSettings
}
import akka.stream.javadsl.Sink
import com.typesafe.config.Config
import io.nats.streaming.StreamingConnection

import scala.concurrent.Future

object NatsStreamingSimpleSink {
  def create(
      connection: StreamingConnection,
      settings: PublishingSettings
  ): Sink[OutgoingMessage[Array[Byte]], Future[Done]] =
    Sink.fromGraph(new NatsStreamingSimpleSinkStage(connection, settings))

  def create(
      connection: StreamingConnection,
      config: Config
  ): Sink[OutgoingMessage[Array[Byte]], Future[Done]] =
    create(connection, PublishingSettings.fromConfig(config))
}
