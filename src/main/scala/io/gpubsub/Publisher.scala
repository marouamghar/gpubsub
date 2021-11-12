package io.gpubsub

import scala.concurrent.Future

trait Publisher[M, R, F[_]] {
  def publishMessage(message: M): F[R]
}
