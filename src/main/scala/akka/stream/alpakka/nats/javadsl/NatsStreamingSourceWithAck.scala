package akka.stream.alpakka.nats.javadsl

import akka.NotUsed
import akka.stream.alpakka.nats.{
  IncomingMessageWithAck,
  NatsStreamingSourceWithAckStage,
  SubscriptionWithAckSettings
}
import akka.stream.javadsl.Source
import com.typesafe.config.Config
import io.nats.streaming.StreamingConnection

object NatsStreamingSourceWithAck {
  def create(
      connection: StreamingConnection,
      settings: SubscriptionWithAckSettings
  ): Source[IncomingMessageWithAck[Array[Byte]], NotUsed] =
    Source.fromGraph(new NatsStreamingSourceWithAckStage(connection, settings))

  def create(
      connection: StreamingConnection,
      config: Config
  ): Source[IncomingMessageWithAck[Array[Byte]], NotUsed] =
    create(connection, SubscriptionWithAckSettings.fromConfig(config))

}
