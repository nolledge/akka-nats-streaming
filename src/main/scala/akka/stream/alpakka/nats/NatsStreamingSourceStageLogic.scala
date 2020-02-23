package akka.stream.alpakka.nats

import java.util.concurrent.ConcurrentLinkedQueue

import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import io.nats.streaming.{Message, MessageHandler, StreamingConnection}

import scala.util.control.NonFatal

private[nats] abstract class NatsStreamingSourceStageLogic[T](
    connection: StreamingConnection,
    settings: NatsStreamingSubscriptionSettings,
    shape: SourceShape[T],
    out: Outlet[T]
) extends GraphStageLogic(shape)
    with OutHandler
    with StageLogging {

  protected final var unackedMsg = 0
  private final var downstreamWaiting = false
  private final var subscriptions: Seq[io.nats.streaming.Subscription] =
    Seq.empty

  val messageQueue = new ConcurrentLinkedQueue[T]()
  protected final var processingLogic: AsyncCallback[Unit] = _
  protected val messageHandler: MessageHandler

  private final def handleFailure(e: Throwable): Unit = {
    log.error(e, "Caught Exception. Failing stage...")
    failStage(e)
  }

  private def push(e: T): Unit = {
    downstreamWaiting = false
    unackedMsg += 1
    log.debug("Pushing message {}", e)
    push(out, e)
  }

  private final def process(u: Unit): Unit = {
    if (downstreamWaiting) {
      Option(messageQueue.poll()).foreach(push)
    }
    u
  }

  override def preStart(): Unit =
    try {
      log.debug("Initializing source")
      processingLogic = getAsyncCallback(process)
      subscriptions = settings.subjects.map { s =>
        connection.subscribe(
          s,
          settings.subscriptionQueue,
          messageHandler,
          settings.subscriptionOptions
        )
      }
      processingLogic.invoke(())
      super.preStart()
    } catch {
      case NonFatal(e) =>
        handleFailure(e)
    }

  override def postStop(): Unit = {
    try {
      log.debug("Stopping source. Closing subscriptions.")
      subscriptions.foreach(_.close())
    } catch {
      case NonFatal(e) =>
        log.error(e, "Exception during cleanup")
    }
    super.postStop()
  }

  override def onPull(): Unit = {
    log.debug("Downstream in demand trying to push element")
    if (messageQueue.isEmpty) {
      downstreamWaiting = true
    } else {
      Option(messageQueue.poll())
        .fold {
          downstreamWaiting = true
        }(push)
    }
  }
  override def onDownstreamFinish(cause: Throwable): Unit = {
    if (unackedMsg == 0) super.onDownstreamFinish(cause)
    else {
      setKeepGoing(true)
      log.debug("Awaiting {} acks before finishing.", unackedMsg)
    }
  }

  setHandler(out, this)
}

private[nats] class NatsStreamingSimpleSourceStageLogic(
    connection: StreamingConnection,
    settings: SimpleSubscriptionSettings,
    shape: SourceShape[IncomingMessage[Array[Byte]]],
    out: Outlet[IncomingMessage[Array[Byte]]]
) extends NatsStreamingSourceStageLogic(connection, settings, shape, out) {
  val messageHandler: MessageHandler = (msg: Message) => {
    messageQueue.offer(IncomingMessage(msg.getData, msg.getSubject))
    if (settings.manualAcks) msg.ack()
    processingLogic.invoke(())
  }
}

private[nats] class NatsStreamingSourceWithAckStageLogic(
    connection: StreamingConnection,
    settings: SubscriptionWithAckSettings,
    shape: SourceShape[IncomingMessageWithAck[Array[Byte]]],
    out: Outlet[IncomingMessageWithAck[Array[Byte]]]
) extends NatsStreamingSourceStageLogic(connection, settings, shape, out) {
  val ackCallaback = getAsyncCallback[Message] { msg =>
    log.debug("Message {} ackknowledged", msg.getSequence)
    msg.ack()
    unackedMsg -= 1
    if (unackedMsg == 0 && isClosed(out)) completeStage()
  }
  val messageHandler: MessageHandler = (msg: Message) => {
    log.debug(
      "Incoming message with id {}. Putting into queue.",
      msg.getSequence
    )
    if (msg.isRedelivered) {
      log.warning("message {} has been redelivered", msg)
    }
    def ack(): Unit = ackCallaback.invoke(msg)
    messageQueue.offer(IncomingMessageWithAck(msg.getData, msg.getSubject, ack))
    processingLogic.invoke(())
  }
}

class NatsStreamingSimpleSourceStage(
    connection: StreamingConnection,
    settings: SimpleSubscriptionSettings
) extends GraphStage[SourceShape[IncomingMessage[Array[Byte]]]] {
  val out: Outlet[IncomingMessage[Array[Byte]]] = Outlet(
    "NatsStreamingSimpleSource.out"
  )
  val shape: SourceShape[IncomingMessage[Array[Byte]]] = SourceShape(out)
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new NatsStreamingSimpleSourceStageLogic(connection, settings, shape, out)
}

class NatsStreamingSourceWithAckStage(
    connection: StreamingConnection,
    settings: SubscriptionWithAckSettings
) extends GraphStage[SourceShape[IncomingMessageWithAck[Array[Byte]]]] {
  require(
    settings.manualAckTimeout.compareTo(settings.autoRequeueTimeout.get) <= 0
  )
  val out: Outlet[IncomingMessageWithAck[Array[Byte]]] = Outlet(
    "NatsStreamingSourceWithAck.out"
  )
  val shape: SourceShape[IncomingMessageWithAck[Array[Byte]]] = SourceShape(out)
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new NatsStreamingSourceWithAckStageLogic(connection, settings, shape, out)
}
