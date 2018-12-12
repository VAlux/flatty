import java.io.{InputStream, OutputStream}

import cats.effect.{Resource, Sync}

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
                                           (implicit serializationProtocol: SerializationProtocol[A]): F[A] = {
        import SerializationProtocolInstances._
        val sizeDeserializer: SerializationProtocol[Long] = implicitly[SerializationProtocol[Long]]
        val sizeBuffer = new Array[Byte](8)
        for {
          _ <- source.read(sizeBuffer, 0, 8)
          size <- sizeDeserializer.deserialize(sizeBuffer)
          entityBuffer = new Array[Byte](size)
          _ <- source.read(entityBuffer, 0, size)
          entity: A <- serializationProtocol.deserialize(entityBuffer)
        } yield entity
      }

      override def output[F[_] : Sync](entity: A, stream: Resource[F, OutputStream])
                                      (implicit serializationProtocol: SerializationProtocol[A]): F[Long] = for {
        payload <- serializationProtocol.serialize(entity)
        total <- stream.use(outputStream => transmitTo(payload, outputStream))
      } yield total

      override def input[F[_] : Sync](stream: Resource[F, InputStream])
                                     (implicit serializationProtocol: SerializationProtocol[A]): F[A] = for {
        entity: A <- stream.use(inputStream => transmitFrom(inputStream))
      } yield entity
    }
}