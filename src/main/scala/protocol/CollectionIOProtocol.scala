package protocol

import java.io.{FileInputStream, FileOutputStream}

import cats.effect.concurrent.MVar
import cats.effect.{Concurrent, Resource}
import collection.Collection

trait CollectionIOProtocol[I, O] {
  def load[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol]
  (resource: Resource[F, I]): F[Collection[F, K, V]]

  def save[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol]
  (resource: Resource[F, O], element: Collection[F, K, V]): F[Long]
}

object CollectionIOProtocol {
  def apply[I, O](implicit instance: CollectionIOProtocol[I, O]): CollectionIOProtocol[I, O] = instance
}

//object CollectionIOProtocolInstances {
//
//  import IOStreamProtocolInstances._
//  import cats.implicits._
//
//  implicit val fileStreamCollectionIOProtocol: CollectionIOProtocol[FileInputStream, FileOutputStream] =
//    new CollectionIOProtocol[FileInputStream, FileOutputStream] {
//
//      override def load[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol]
//      (resource: Resource[F, FileInputStream]): F[Collection[F, K, V]] = for {
//        key <- IOProtocol[FileInputStream, FileOutputStream].input[F, K](resource)
//        value <- IOProtocol[FileInputStream, FileOutputStream].input[F, V](resource)
//        map <- MVar.of[F, Map[K, V]](Map(key -> value))
//      } yield new Collection[F, K, V](map)
//
//      override def save[F[_] : Concurrent, K: SerializationProtocol, V: SerializationProtocol]
//      (resource: Resource[F, FileOutputStream], collection: Collection[F, K, V]): F[Long] =
//        collection.data.take.flatMap { elems =>
//          Concurrent[F].delay(
//            //          val written = for {
//            //            (key, value) <- elems
//            //            amount <- IOProtocol[FileInputStream, FileOutputStream].output[F, K](key, resource)
//            //          } yield amount
//
//            1L
//          )
//        }
//    }
//}