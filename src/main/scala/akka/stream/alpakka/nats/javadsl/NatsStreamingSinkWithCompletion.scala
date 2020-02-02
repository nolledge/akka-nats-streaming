package akka.stream.alpakka.nats.javadsl

import akka.Done
import akka.stream.alpakka.nats.{
  NatsStreamingSinkWithCompletionStage,
  OutgoingMessageWithCompletion,
  PublishingSettings
}
import akka.stream.javadsl.Sink
import com.typesafe.config.Config
import io.nats.streaming.StreamingConnection

import scala.concurrent.Future

object NatsStreamingSinkWithCompletion {
  def create(
      connection: StreamingConnection,
      settings: PublishingSettings
  ): Sink[OutgoingMessageWithCompletion[Array[Byte]], Future[Done]] =
    Sink.fromGraph(
      new NatsStreamingSinkWithCompletionStage(connection, settings)
    )

  def create(
      connection: StreamingConnection,
      config: Config
  ): Sink[OutgoingMessageWithCompletion[Array[Byte]], Future[Done]] =
    create(connection, PublishingSettings.fromConfig(config))
}
