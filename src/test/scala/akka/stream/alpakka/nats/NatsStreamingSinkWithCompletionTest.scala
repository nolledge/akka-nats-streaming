package akka.stream.alpakka.nats

import akka.Done
import akka.stream.alpakka.nats.scaladsl.NatsStreamingSinkWithCompletion
import akka.stream.scaladsl.Source

class NatsStreamingSinkWithCompletionTest extends FunctionalTestBase {

  "The NatsStreamingSinkWithCompletionStage" must {
    "execute callback when message is ack by nats" in {
      val sink = NatsStreamingSinkWithCompletion(
        connection,
        PublishingSettings("test.subject", parallel = false)
      )
      val msg = OutgoingMessageWithCompletion(Array.empty[Byte], "test.subject")
      whenReady(
        Source(List(msg))
          .runWith(sink)
          .flatMap(_ => msg.completion)
      ) { _ mustBe a[Done] }
    }
  }
}
