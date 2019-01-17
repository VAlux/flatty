package protocol

import cats.effect.{Resource, Sync}

trait IOProtocol[I, O, A] {
  def input[F[_] : Sync](resource: Resource[F, I]): F[A]

  def output[F[_] : Sync](entity: A, resource: Resource[F, O]): F[Long]
}

object IOProtocol {
  def apply[I, O, A](implicit instance: IOProtocol[I, O, A]): IOProtocol[I, O, A] = instance
}