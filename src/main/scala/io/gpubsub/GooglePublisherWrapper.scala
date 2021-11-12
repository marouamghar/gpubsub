package io.gpubsub

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.Publisher as GooglePublisherClient
import com.google.pubsub.v1.{ProjectTopicName, PubsubMessage, TopicName}
import gpubsub.*
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

abstract class GooglePublisherWrapper[M](implicit transform: M => PubsubMessage, ec: ExecutionContext)
  extends Publisher[M, String, Future] {
  val projectId: String
  val topicId: String
  val credentialsProvider: CredentialsProvider
  val channelProvider: Option[TransportChannelProvider]

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  lazy val topic: TopicName = ProjectTopicName.of(projectId, topicId)
  lazy val client: GooglePublisherClient = {
    val builder = GooglePublisherClient.newBuilder(topic).setCredentialsProvider(credentialsProvider)
    if (channelProvider.nonEmpty) builder.setChannelProvider(channelProvider.get)
    builder.build()
  }

  //TODO: why doesn't it detect the implicit method?
  override def publishMessage(message: M): Future[String] = publish(transform(message))

  def publish(message: PubsubMessage): Future[String] = client.publish(message).asFuture.andThen {
    case Success(value) => logger.info(s"message $value was well published")
    case Failure(err) => logger.error(s"message was not published", err)
  }

  def stop(): Future[Unit] = Future {
    Try(client.shutdown()) match {
      case Success(_) => logger.info(s"Publisher $topic has been shut down")
      case Failure(ex) => logger.error("Error during publisher shutdown", ex)
    }
  }
}
