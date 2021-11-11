package gpubsub

import com.google.api.gax.core.{CredentialsProvider, NoCredentialsProvider}
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.{FixedTransportChannelProvider, TransportChannelProvider}
import com.google.auth.Credentials
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.pubsub.v1.{Subscriber, SubscriptionAdminSettings}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import gpubsub.*
import gpubsub.PubsubLocalTest.{channelProvider, credentialsProvider}
import io.grpc.ManagedChannelBuilder
import org.slf4j.{Logger, LoggerFactory}

import java.util.UUID
import scala.collection.JavaConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.sys.process.*
import scala.util.{ChainingSyntax, Failure, Success, Try}

object PubsubLocalTest extends App with ChainingSyntax {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val topicId = "topic-test-marou"
  val subscriptionId = "topic-test-marou-sub"
  val projectId = "revio-staging-320500"
  val port = 8092

  val useFakePubSub = false

  val (channelProvider, credentialsProvider) =
    if (useFakePubSub) {
      val host = "localhost:" + port
      val pubsubProcess = s"gcloud beta emulators pubsub start --host-port=$host".run()
      val channel = ManagedChannelBuilder.forTarget(host).usePlaintext.build
      val channelProvider: TransportChannelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
      val credentialsProvider: CredentialsProvider = NoCredentialsProvider.create()

      logger.info(s"Topic $topicId is going to be created")
      val topicOpt = CreateTopic.createTopic(projectId, topicId, credentialsProvider, channelProvider)
      logger.info(s"Subscription $subscriptionId is going to be created")
      val subOpt = CreateTopic.createSubscription(projectId, topicId, subscriptionId, credentialsProvider, channelProvider)

      (Some(channelProvider), credentialsProvider)
    }
    else {
      val credentialsProvider: CredentialsProvider = new CredentialsProvider {
        override def getCredentials: Credentials = ServiceAccountCredentials
          .fromStream(getClass.getResourceAsStream("/revio-staging-320500-db7f307386d9.json"))
          .createScoped()
      }
      (None, credentialsProvider)
    }

  val publisher = new GooglePublisherWrapper[String] {
    val projectId = PubsubLocalTest.projectId
    val topicId = PubsubLocalTest.topicId
    val credentialsProvider = PubsubLocalTest.credentialsProvider
    val channelProvider = PubsubLocalTest.channelProvider
  }

  val subscriberWrapper = new GoogleSubscriberWrapper[Unit] {
    val projectId = PubsubLocalTest.projectId
    val subscriptionId = PubsubLocalTest.subscriptionId
    val credentialsProvider = PubsubLocalTest.credentialsProvider
    val channelProvider = PubsubLocalTest.channelProvider

    override def handle(message: PubsubMessage): Future[Unit] = {
      val st = message.getData.toString
      this.logger.info(s"I receive message : $st")
      Future.unit
    }
  }

  val subFuture: Future[Subscriber] = subscriberWrapper.start()
  val sub = Await.result(subFuture, Duration.Inf)

  val numbeMessages = 3
  for (i <- 1 to numbeMessages) {
    publisher.publishMessage(UUID.randomUUID().toString)
    Thread.sleep(numbeMessages * 2000)
  }

  Await.ready(publisher.stop(), Duration.Inf)
  Await.ready(subscriberWrapper.stopClient(sub), Duration.Inf)

  Seq("ksh","-c",s"kill $$(lsof -i:8092 | grep LISTEN | awk '{print $$2}')") !
}
