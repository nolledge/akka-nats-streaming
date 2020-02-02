package akka.stream.alpakka.nats.javadsl

import akka.NotUsed
import akka.stream.alpakka.nats.{
  IncomingMessage,
  NatsStreamingSimpleSourceStage,
  SimpleSubscriptionSettings
}
import akka.stream.javadsl.Source
import com.typesafe.config.Config
import io.nats.streaming.StreamingConnection

object NatsStreamingSimpleSource {
  def create(
      connection: StreamingConnection,
      settings: SimpleSubscriptionSettings
  ): Source[IncomingMessage[Array[Byte]], NotUsed] =
    Source.fromGraph(new NatsStreamingSimpleSourceStage(connection, settings))

  def create(
      connection: StreamingConnection,
      config: Config
  ): Source[IncomingMessage[Array[Byte]], NotUsed] =
    create(connection, SimpleSubscriptionSettings.fromConfig(config))
}
