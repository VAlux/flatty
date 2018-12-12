import java.io.{DataInputStream, InputStream, OutputStream}

import cats.effect.{Resource, Sync}
import cats.syntax.functor._
import cats.syntax.flatMap._

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

      private def transmitFrom[F[_] : Sync](source: InputStream)
                                           (implicit serializationProtocol: SerializationProtocol[A]): F[A] =
        for {
          dataStream <- Sync[F].delay(new DataInputStream(source))
          size <- Sync[F].delay(dataStream.readInt())
          entityBuffer <- Sync[F].delay(new Array[Byte](size))
          _ <- Sync[F].delay(dataStream.read(entityBuffer, 0, size))
          entity <- serializationProtocol.deserialize(entityBuffer)
        } yield entity

      override def output[F[_] : Sync](entity: A, resource: Resource[F, OutputStream])
                                      (implicit serializationProtocol: SerializationProtocol[A]): F[Long] =
        for {
          payload <- serializationProtocol.serialize(entity)
          total <- resource.use(outputStream => transmitTo(payload, outputStream))
        } yield total

      override def input[F[_] : Sync](resource: Resource[F, InputStream])
                                     (implicit serializationProtocol: SerializationProtocol[A]): F[A] =
        for {
          entity <- resource.use(inputStream => transmitFrom(inputStream))
        } yield entity
    }
}