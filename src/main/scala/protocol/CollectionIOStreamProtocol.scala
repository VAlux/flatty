package protocol

import java.io.{InputStream, OutputStream}

import cats.effect.{Concurrent, Resource}
import cats.syntax.all._

final class CollectionIOStreamProtocol[I <: InputStream, O <: OutputStream](ioProtocol: IOProtocol[I, O])
  extends CollectionIOProtocol[I, O] {

  override def load[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol]
  (resource: Resource[F, I]): F[Collection[F, K, V]] = ???

  override def save[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol]
  (resource: Resource[F, O], collection: Collection[F, K, V]): F[Long] = {
    collection.data.take.flatMap { elemsMap =>
      val test = for {
        (key, value) <- elemsMap
        keyAmount = ioProtocol.output[F, K](key, resource)
        valueAmount = ioProtocol.output[F, V](value, resource)
      } yield (keyAmount, valueAmount)

      Concurrent[F].delay(1)
    }
  }
}
