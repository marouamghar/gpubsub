package io.gpubsub


import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.{AlreadyExistsException, TransportChannelProvider}
import com.google.cloud.pubsub.v1.{SubscriptionAdminClient, SubscriptionAdminSettings, TopicAdminClient, TopicAdminSettings}
import com.google.pubsub.v1.{PushConfig, Subscription, SubscriptionName, Topic, TopicName}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{ChainingSyntax, Failure, Success, Try}

object CreateTopic extends ChainingSyntax {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def createTopic(projectId: String, topicId: String, credentialsProvider: CredentialsProvider, channelProvider: Option[TransportChannelProvider])= {
    val topicName: TopicName = TopicName.of(projectId, topicId)
    val builder = TopicAdminSettings.newBuilder.setCredentialsProvider(credentialsProvider)
    if (channelProvider.nonEmpty) builder.setTransportChannelProvider(channelProvider.get)
    val topicAdminClient = TopicAdminClient.create(builder.build)

    Try(topicAdminClient.getTopic(topicName)) match {
      case Success(topic) =>
        logger.info(s"Topic ${topic.getName} already exists")
        Some(topic)
      case Failure(_) =>
        logger.info(s"Topic $topicId doesn't exist. Creating it now ")
        Try(topicAdminClient.createTopic(topicName)) match {
          case Success(topic) =>
            logger.info("Created topic: " + topic.getName())
            Some(topic)
          case Failure(ex) =>
            logger.error("Topic creation failed.", ex)
            None
        }
    }
  }

  def createSubscription(projectId: String, topicId: String, subscriptionId: String, credentialsProvider: CredentialsProvider, channelProvider: Option[TransportChannelProvider]): Option[Subscription] = {
    val subName = SubscriptionName.of(projectId, subscriptionId).toString
    val topicName = TopicName.of(projectId, topicId)
    val pushConfig = PushConfig.newBuilder.build
    val ackDeadlineSeconds = 5

    val builder = SubscriptionAdminSettings.newBuilder.setCredentialsProvider(credentialsProvider)
    if (channelProvider.nonEmpty) builder.setTransportChannelProvider(channelProvider.get)
    val subAdminClient = SubscriptionAdminClient.create(builder.build())

    Try(subAdminClient.getSubscription(subName)) match {
      case Success(sub) =>
        logger.info(s"Subscription ${sub.getName} already exists")
        Some(sub)
      case Failure(_) =>
        logger.info(s"Subscription $subscriptionId doesn't exist. Creating it now")
        Try(subAdminClient.createSubscription(subName, topicName, pushConfig, ackDeadlineSeconds))match {
          case Success(sub) =>
            logger.info("Created subscription: " + sub.getName())
            Some(sub)
          //TODO: this failure does not capture the error properly. For repro, put ackDeadlineSeconds = 2135351438
          case Failure(ex) =>
            logger.error("Subscription creation failed.", ex)
            None
        }
    }
  }
}
