package gpubsub

import com.google.api.gax.core.{CredentialsProvider, NoCredentialsProvider}
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.{FixedTransportChannelProvider, TransportChannelProvider}
import com.google.auth.Credentials
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.PubsubMessage
import gpubsub.CreateTopic
import io.grpc.ManagedChannelBuilder
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.slf4j.{Logger, LoggerFactory}

import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.sys.process.*
import org.scalatest.BeforeAndAfterEach

class GooglePubSubTest
  extends AnyFreeSpec
    with MockitoSugar
    with Matchers
    with ScalaCheckDrivenPropertyChecks 
    with BeforeAndAfterEach :
  that =>

  val topicId = "topic-test-marou"
  val subscriptionId = "topic-test-marou-sub"
  val projectId = "revio-staging-320500"
  val port = 8086
  val host = "localhost:" + port

  override def afterEach(): Unit = 
    super.afterEach()
    Seq("ksh", "-c", s"kill $$(lsof -i:$port | grep LISTEN | awk '{print $$2}')") !

  val pubsubProcess = s"gcloud beta emulators pubsub start --host-port=$host".run()
  val channel = ManagedChannelBuilder.forTarget(host).usePlaintext.build
  val channelProvider = Some(FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)));
  val credentialsProvider = NoCredentialsProvider.create()

  val topicOpt = CreateTopic.createTopic(projectId, topicId, credentialsProvider, channelProvider.get)
  val subOpt = CreateTopic.createSubscription(projectId, topicId, subscriptionId, credentialsProvider, channelProvider.get)

  val publisher = new GooglePublisherWrapper[String] {
    val projectId = that.projectId
    val topicId = that.topicId
    val credentialsProvider = that.credentialsProvider
    val channelProvider = that.channelProvider
  }

  val messagesReceived = ArrayBuffer.empty[String]

  val subscriberWrapper = new GoogleSubscriberWrapper[Unit] :
    val projectId = that.projectId
    val subscriptionId = that.subscriptionId
    val credentialsProvider = that.credentialsProvider
    val channelProvider = that.channelProvider

    override def handle(message: PubsubMessage): Future[Unit] =
      val data = message.getData.toString
      messagesReceived += data.substring(data.indexOf('"')+1,data.lastIndexOf('"'))
      logger.info(s"I handled a message with data: $data")
      Future.unit

  
  val subFuture: Future[Subscriber] = subscriberWrapper.start()
  val sub = Await.result(subFuture, Duration.Inf)

  "Pubsub" - {
    "should publish and handle messages" in {
      val messagesPublished = Array.fill(4)(UUID.randomUUID().toString)
      for (message <- messagesPublished) {
        publisher.publishMessage(message)
        Thread.sleep(messagesPublished.length * 200)
      }

      Await.ready(publisher.stop(), Duration.Inf)
      Await.ready(subscriberWrapper.stopClient(sub), Duration.Inf)

      messagesPublished should contain theSameElementsAs messagesReceived
    }
  }
