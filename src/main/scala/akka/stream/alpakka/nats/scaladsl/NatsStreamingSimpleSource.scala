package akka.stream.alpakka.nats.scaladsl

import akka.NotUsed
import akka.stream.alpakka.nats.{
  IncomingMessage,
  NatsStreamingSimpleSourceStage,
  SimpleSubscriptionSettings
}
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import io.nats.streaming.StreamingConnection

object NatsStreamingSimpleSource {
  def apply(
      connection: StreamingConnection,
      settings: SimpleSubscriptionSettings
  ): Source[IncomingMessage[Array[Byte]], NotUsed] =
    Source.fromGraph(new NatsStreamingSimpleSourceStage(connection, settings))

  def apply(
      connection: StreamingConnection,
      config: Config
  ): Source[IncomingMessage[Array[Byte]], NotUsed] =
    apply(connection, SimpleSubscriptionSettings.fromConfig(config))
}
