package gpubsub

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber as GoogleSubscriberClient}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import gpubsub.*
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.{ChainingSyntax, Failure, Success, Try}

abstract class GoogleSubscriberWrapper[R](implicit ec: ExecutionContext) extends Subscriber[PubsubMessage, R, Future] with ChainingSyntax {
  val projectId: String
  val subscriptionId: String
  val credentialsProvider: CredentialsProvider
  //set it to none if connecting to a real PubSub instance
  val channelProvider: Option[TransportChannelProvider]
  
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  lazy val subscription = ProjectSubscriptionName.of(projectId, subscriptionId)

  def receiver: MessageReceiver = {
    (message, consumer) =>
      handle(message)
      consumer.ack()
  }

  private def startClient(subscription: ProjectSubscriptionName, receiver: MessageReceiver, credentialsProvider: CredentialsProvider, retries: Int = 10): Future[GoogleSubscriberClient] = {
    if (retries == 0)
      logger.error(s"The client was restarted too many times. Killing the process")
      Future.failed(new RuntimeException("The client was restarted too many times. Killing the process"))
    else
      val builder = GoogleSubscriberClient.newBuilder(subscription, receiver)
      builder.setCredentialsProvider(credentialsProvider)
      if (channelProvider.nonEmpty) builder.setChannelProvider(channelProvider.get)
      builder.build().pipe { client =>
        Try(client.startAsync().awaitRunning()) match {
          case Success(_) =>
            logger.info(s"Subscriber $subscription started successfully!")
            Future.successful(client.tap(sub => awaitClient(sub, retries)))

          case Failure(ex) =>
            logger.error("Error starting the subscriber", ex)
            Try(client.stopAsync().awaitTerminated()).recover {
              case ex =>
                logger.error("Error stopping the subscriber", ex)
            }
            startClient(subscription, receiver, credentialsProvider, retries - 1)
        }
      }
  }

  //TODO: figure out how to have 1 client per wrapper, so these methods wouldn't take a client, or move them to an object
  def stopClient(client: GoogleSubscriberClient): Future[Unit] = Future {
    Try(client.stopAsync().awaitTerminated()) match {
      case Success(_) => logger.info(s"Subscriber $subscription stopped successfully!")
      case Failure(e) => logger.error("Error stopping the subscriber", e)

    }
  }

  def awaitClient(client: GoogleSubscriberClient, retries: Int): Future[Unit] =
    Future {
      //TODO: this piece of code doesn't log all the errors happening during the process. I can mess with the channel to repro it, it fails to handle the message, but it logs nothing
      Try(client.awaitTerminated()) recover {
        case ex: Throwable => logger.error("Error during the subscriber working. Restarting it.", ex)
          stopClient(client).flatMap(_ => startClient(subscription, receiver, credentialsProvider, retries - 1))
      }
    }

  def start(): Future[GoogleSubscriberClient] = startClient(subscription, receiver, credentialsProvider)
}
//
//abstract class GoogleSubscriberWrapper[R, RT](implicit ec: ExecutionContext) extends Subscriber[PubsubMessage, R, Future] with ChainingSyntax with App {
//
//  val projectId: String
//  val subscriptionId: String
//  val credentialsProvider: CredentialsProvider
//
//  val subscription = ProjectSubscriptionName.of(projectId, subscriptionId)
//
//  def receiver: MessageReceiver = {
//    (message, consumer) =>
//      handle(message)
////      println("I handled the message")
//    //      consumer.ack()
//  }
//
//  val client = GoogleSubscriberClient.newBuilder(subscription, receiver).setCredentialsProvider(credentialsProvider).build()
//
//  def startClient(): Unit = {
//    client.startAsync().awaitRunning()
//  }
//
//  def awaitClient(): Unit = client.awaitTerminated()
//
//  //  val startClientZio = effectBlocking(ZIO.effect(startClient()).tapError(logger.errorZIO).retry(Schedule.recurs(5)))
//  val startClientZio = ZIO.effect(startClient()).tapError(logger.errorZIO).retry(Schedule.recurs(5))
//  val awaitClientZio = effectBlocking(awaitClient())
//
//  val run = for {
//    _ <- startClientZio
//    _ <- awaitClientZio
//  } yield ()
//
//  println(s"listening to messages on subscription: $subscriptionId")
//
//  override def run(args: List[String]) = run.exitCode
//
//  def logger.infoZIO(line: String): UIO[Unit] = ZIO.succeed(println(line))
//
//  def logger.errorZIO(err: Throwable): UIO[Unit] = logger.infoZIO(s"error message is ${err.getMessage}")
//}
