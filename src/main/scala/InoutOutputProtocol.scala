import java.io._

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, Resource, Sync}
import cats.syntax.flatMap._
import cats.syntax.functor._

sealed trait InoutOutputProtocol[EntityType, InputResourceType, OutputResourceType] {
  def output[F[_] : Sync](entity: EntityType, stream: Resource[F, OutputResourceType])
                         (implicit serializationProtocol: SerializationProtocol[EntityType]): F[Long]

  def input[F[_] : Sync](stream: Resource[F, InputResourceType])
                        (implicit serializationProtocol: SerializationProtocol[EntityType]): F[EntityType]
}

object InputOutputProtocolInstances {
  implicit def inputOutputStreamProtocol[A]: InoutOutputProtocol[A, InputStream, OutputStream] =
    new InoutOutputProtocol[A, InputStream, OutputStream] {

      private def transmitTo[F[_] : Sync](payload: Array[Byte], destination: OutputStream): F[Long] =
        Sync[F].delay {
          destination.write(payload.length)
          destination.write(payload)
          payload.length + 8
        }

      private def transmitFrom[F[_] : Sync](source: InputStream): F[Array[Byte]] =
        for {
          dataStream <- Sync[F].delay(new DataInputStream(source))
          size <- Sync[F].delay(dataStream.readInt())
          entityBuffer <- Sync[F].delay(new Array[Byte](size))
          _ <- Sync[F].delay(dataStream.read(entityBuffer, 0, size))
        } yield entityBuffer

      override def output[F[_] : Sync](entity: A, resource: Resource[F, OutputStream])
                                      (implicit serializationProtocol: SerializationProtocol[A]): F[Long] =
        for {
          payload <- serializationProtocol.serialize(entity)
          total <- resource.use(outputStream => transmitTo(payload, outputStream))
        } yield total

      override def input[F[_] : Sync](resource: Resource[F, InputStream])
                                     (implicit serializationProtocol: SerializationProtocol[A]): F[A] =
        for {
          payload <- resource.use(inputStream => transmitFrom(inputStream))
          entity <- serializationProtocol.deserialize(payload)
        } yield entity
    }
}

object InoutOutputProtocolSyntax {

  import InputOutputProtocolInstances._

  implicit class InputOutputProtocolOps[A: SerializationProtocol, F[_] : Sync](entity: A) {
    def out(resource: Resource[F, OutputStream]): F[Long] =
      implicitly[InoutOutputProtocol[A, _, OutputStream]].output(entity, resource)
  }

}

object Test extends App {

  import InputOutputProtocolInstances._
  import cats.implicits._

  def inputStream[F[_] : Sync](f: File, guard: Semaphore[F]): Resource[F, FileInputStream] =
    Resource.make {
      Sync[F].delay(new FileInputStream(f))
    } { inStream =>
      guard.withPermit {
        Sync[F].delay(inStream.close()).handleErrorWith(_ => Sync[F].unit)
      }
    }

  def outputStream[F[_] : Sync](f: File, guard: Semaphore[F]): Resource[F, FileOutputStream] =
    Resource.make {
      Sync[F].delay(new FileOutputStream(f))
    } { outStream =>
      guard.withPermit {
        Sync[F].delay(outStream.close()).handleErrorWith(_ => Sync[F].unit)
      }
    }


  def out[F[_] : Concurrent, A: SerializationProtocol](entity: A, resource: Resource[F, OutputStream]): F[Long] =
    implicitly[InoutOutputProtocol[A, InputStream, OutputStream]].output(entity, resource)
}