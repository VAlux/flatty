package collection

import cats.effect.Concurrent
import cats.effect.concurrent.MVar
import cats.syntax.flatMap._
import cats.syntax.functor._
import protocol.SerializationProtocol

final case class Collection[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol](data: MVar[F, Map[K, V]]) {
  def get(key: K): F[Option[V]] =
    data.take.flatMap(elems => Concurrent[F].delay(elems.get(key)))

  def put(key: K, value: V): F[Boolean] =
    data.take.flatMap(map => data.tryPut(map + (key -> value)))

  def remove(key: K): F[Boolean] =
    data.take.flatMap(map => data.tryPut(map - key))

  def isDefined(key: K): F[Boolean] =
    data.take.flatMap(map => Concurrent[F].delay(map.isDefinedAt(key)))

  def asString: F[String] =
    data.take.flatMap(map => Concurrent[F].delay(map.toString()))
}

object Collection {
  def apply[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol]
  (data: Iterable[(K, V)]): F[Collection[F, K, V]] = for {
    map <- Concurrent[F].delay(data.map(element => element._1 -> element._2).toMap)
    mv <- MVar.of[F, Map[K, V]](map)
    collection <- Concurrent[F].delay(new Collection[F, K, V](mv))
  } yield collection

  def empty[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol]: F[Collection[F, K, V]] = for {
    data <- MVar.of[F, Map[K, V]](Map.empty[K, V])
    collection <- Concurrent[F].pure(new Collection[F, K, V](data))
  } yield collection
}
