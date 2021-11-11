package gpubsub

trait Subscriber[M, R, F[_]] {
  def handle(message: M): F[R]
}
