package akka.stream.alpakka.nats

import akka.Done

import scala.concurrent.{Future, Promise}

sealed trait NatsStreamingMessage[D] {
  def data: D
  def subject: String
}

case class IncomingMessage[D](
    data: D,
    seqNumber: Long,
    isRedelivered: Boolean,
    subject: String
) extends NatsStreamingMessage[D]

case class IncomingMessageWithAck[D](
    data: D,
    seqNumber: Long,
    isRedelivered: Boolean,
    subject: String,
    ack: () => Unit
) extends NatsStreamingMessage[D]

object IncomingMessageWithAck {
  private[nats] def apply[T](
      data: T,
      seqNumber: Long,
      isRedelivered: Boolean,
      subject: String,
      ack: () => Unit
  ): IncomingMessageWithAck[T] =
    new IncomingMessageWithAck(data, seqNumber, isRedelivered, subject, ack)
}

case class OutgoingMessage[T](data: T, subject: String)
    extends NatsStreamingMessage[T]

case class OutgoingMessageWithCompletion[T](data: T, subject: String)
    extends NatsStreamingMessage[T] {
  private[nats] val promise = Promise[Done]
  def completion: Future[Done] =
    promise.future
}

object OutgoingMessageWithCompletion {
  private[nats] def apply[T](
      data: T,
      subject: String,
      p: Promise[Done]
  ): OutgoingMessageWithCompletion[T] =
    new OutgoingMessageWithCompletion(data, subject) {
      override val promise: Promise[Done] = p
    }
}
