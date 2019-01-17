package collection

import cats.effect.Concurrent
import cats.effect.concurrent.MVar
import cats.syntax.flatMap._
import protocol.SerializationProtocol

final case class CollectionElement[K: SerializationProtocol, V: SerializationProtocol](key: K, value: V)

class Collection[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol](val data: MVar[F, Map[K, V]]) {
  def get(key: K): F[V] =
    data.take.flatMap(elems => Concurrent[F].delay(elems(key)))

  def put(key: K, value: V): F[Unit] =
    data.take.flatMap(elems => Concurrent[F].delay(data.put(elems + (key -> value))))

  def remove(key: K): F[Unit] =
    data.take.flatMap(elems => Concurrent[F].delay(data.put(elems - key)))

  def isDefined(key: K): F[Boolean] =
    data.take.flatMap(elems => Concurrent[F].delay(elems.isDefinedAt(key)))
}
