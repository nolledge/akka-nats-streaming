package akka.stream.alpakka.nats.scaladsl

import akka.NotUsed
import akka.stream.alpakka.nats.{
  IncomingMessageWithAck,
  NatsStreamingSourceWithAckStage,
  SubscriptionWithAckSettings
}
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import io.nats.streaming.StreamingConnection

object NatsStreamingSourceWithAck {
  def apply(
      connection: StreamingConnection,
      settings: SubscriptionWithAckSettings
  ): Source[IncomingMessageWithAck[Array[Byte]], NotUsed] =
    Source.fromGraph(new NatsStreamingSourceWithAckStage(connection, settings))

  def apply(
      connection: StreamingConnection,
      config: Config
  ): Source[IncomingMessageWithAck[Array[Byte]], NotUsed] =
    apply(connection, SubscriptionWithAckSettings.fromConfig(config))

}
