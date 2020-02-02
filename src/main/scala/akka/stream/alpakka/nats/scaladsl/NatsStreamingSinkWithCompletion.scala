package akka.stream.alpakka.nats.scaladsl

import akka.Done
import akka.stream.alpakka.nats.{
  NatsStreamingSinkWithCompletionStage,
  OutgoingMessageWithCompletion,
  PublishingSettings
}
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config
import io.nats.streaming.StreamingConnection

import scala.concurrent.Future

object NatsStreamingSinkWithCompletion {
  def apply(
      connection: StreamingConnection,
      settings: PublishingSettings
  ): Sink[OutgoingMessageWithCompletion[Array[Byte]], Future[Done]] =
    Sink.fromGraph(
      new NatsStreamingSinkWithCompletionStage(connection, settings)
    )

  def apply(
      connection: StreamingConnection,
      config: Config
  ): Sink[OutgoingMessageWithCompletion[Array[Byte]], Future[Done]] =
    apply(connection, PublishingSettings.fromConfig(config))
}
