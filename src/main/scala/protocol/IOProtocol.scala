package protocol

import cats.effect.{Resource, Sync}

trait IOProtocol[I, O] {
  def input[F[_] : Sync, A: SerializationProtocol](resource: Resource[F, I]): F[A]

  def output[F[_] : Sync, A: SerializationProtocol](entity: A, resource: Resource[F, O]): F[Long]
}

object IOProtocol {
  def apply[I, O](implicit instance: IOProtocol[I, O]): IOProtocol[I, O] = instance
}