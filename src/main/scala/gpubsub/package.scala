package object gpubsub {
  import com.google.api.core.ApiFuture
  import com.google.protobuf.ByteString
  import com.google.pubsub.v1.PubsubMessage

  import scala.language.implicitConversions
  import scala.concurrent.{ExecutionContext, Future}

  implicit class TransformApiFuture[T](future: ApiFuture[T])(implicit ec: ExecutionContext):
    def asFuture: Future[T] = Future {
      future.get()
    }

  implicit def transformString2PubsubMessage(s: String): PubsubMessage = {
    val data = ByteString.copyFromUtf8(s)
    PubsubMessage.newBuilder().setData(data).build()
  }
}
