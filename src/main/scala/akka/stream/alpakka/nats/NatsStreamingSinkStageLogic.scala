package akka.stream.alpakka.nats

import akka.Done
import akka.stream.stage._
import akka.stream.{Attributes, Inlet, SinkShape}
import io.nats.streaming.{AckHandler, StreamingConnection}

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

private[nats] abstract class NatsStreamingSinkStageLogic[
    T <: NatsStreamingOutgoing[Array[Byte]]
](
    connection: StreamingConnection,
    settings: PublishingSettings,
    promise: Promise[Done],
    shape: SinkShape[T],
    in: Inlet[T]
) extends GraphStageLogic(shape)
    with StageLogging {
  protected val successCallback: AsyncCallback[String] = getAsyncCallback(
    handleSuccess
  )
  protected val failureCallback: AsyncCallback[Throwable] = getAsyncCallback(
    handleFailure
  )
  def ah(m: T): AckHandler

  override def preStart(): Unit =
    try {
      pull(in)
      super.preStart()
    } catch {
      case NonFatal(e) =>
        failureCallback.invoke(e)
    }

  override def postStop(): Unit = {
    promise.tryFailure(new RuntimeException("stage stopped unexpectedly"))
    super.postStop()
  }

  def handleFailure(ex: Throwable): Unit = {
    log.error(ex, "Caught Exception. Failing stage...")
    promise.tryFailure(ex)
    failStage(ex)
  }

  def handleSuccess(nuid: String): Unit = {
    log.debug("Successfully pushed {}", nuid)
    if (settings.parallel) () else pull(in)
  }

  setHandler(
    in,
    new InHandler {
      override def onUpstreamFailure(ex: Throwable): Unit = {
        promise.tryFailure(ex)
        super.onUpstreamFailure(ex)
      }

      override def onUpstreamFinish(): Unit = {
        promise.trySuccess(Done)
        super.onUpstreamFinish()
      }
      override def onPush(): Unit = {
        val m = grab(in)
        connection
          .publish(m.subject.getOrElse(settings.defaultSubject), m.data, ah(m))
        if (settings.parallel) pull(in) else ()
      }
    }
  )
}

private[nats] class NatsStreamingSimpleSinkStageLogic(
    connection: StreamingConnection,
    settings: PublishingSettings,
    promise: Promise[Done],
    shape: SinkShape[OutgoingMessage[Array[Byte]]],
    in: Inlet[OutgoingMessage[Array[Byte]]]
) extends NatsStreamingSinkStageLogic(
      connection,
      settings,
      promise,
      shape,
      in
    ) {
  def ah(m: OutgoingMessage[Array[Byte]]): AckHandler =
    (nuid: String, ex: Exception) =>
      if (Option(ex).isDefined) failureCallback.invoke(ex)
      else successCallback.invoke(nuid)
}

private[nats] class NatsStreamingSinkWithCompletionStageLogic(
    connection: StreamingConnection,
    settings: PublishingSettings,
    promise: Promise[Done],
    shape: SinkShape[OutgoingMessageWithCompletion[Array[Byte]]],
    in: Inlet[OutgoingMessageWithCompletion[Array[Byte]]]
) extends NatsStreamingSinkStageLogic(
      connection,
      settings,
      promise,
      shape,
      in
    ) {
  def ah(m: OutgoingMessageWithCompletion[Array[Byte]]): AckHandler =
    (nuid: String, ex: Exception) =>
      if (Option(ex).isDefined) {
        m.promise.tryFailure(ex)
        failureCallback.invoke(ex)
      } else {
        m.promise.trySuccess(Done)
        successCallback.invoke(nuid)
      }
}

private[nats] class NatsStreamingSimpleSinkStage(
    connection: StreamingConnection,
    settings: PublishingSettings
) extends GraphStageWithMaterializedValue[SinkShape[
      OutgoingMessage[Array[Byte]]
    ], Future[Done]] {
  val in: Inlet[OutgoingMessage[Array[Byte]]] = Inlet(
    "NatsStreamingSimpleSink.in"
  )
  val shape: SinkShape[OutgoingMessage[Array[Byte]]] = SinkShape(in)
  def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]
    val logic =
      new NatsStreamingSimpleSinkStageLogic(
        connection,
        settings,
        promise,
        shape,
        in
      )
    (logic, promise.future)
  }
}

private[nats] class NatsStreamingSinkWithCompletionStage(
    connection: StreamingConnection,
    settings: PublishingSettings
) extends GraphStageWithMaterializedValue[SinkShape[
      OutgoingMessageWithCompletion[Array[Byte]]
    ], Future[Done]] {
  val in: Inlet[OutgoingMessageWithCompletion[Array[Byte]]] = Inlet(
    "NatsStreamingSinkWithComplete.in"
  )
  val shape: SinkShape[OutgoingMessageWithCompletion[Array[Byte]]] = SinkShape(
    in
  )
  def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]
    val logic = new NatsStreamingSinkWithCompletionStageLogic(
      connection,
      settings,
      promise,
      shape,
      in
    )
    (logic, promise.future)
  }
}
