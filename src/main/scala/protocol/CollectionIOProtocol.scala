package protocol

import cats.effect.{Concurrent, Resource}

trait CollectionIOProtocol[I, O] {
  def load[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol]
  (resource: Resource[F, I]): F[Collection[F, K, V]]

  def save[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol]
  (resource: Resource[F, O], element: Collection[F, K, V]): F[Long]
}

object CollectionIOProtocol {
  def apply[I, O](implicit instance: CollectionIOProtocol[I, O]): CollectionIOProtocol[I, O] = instance
}