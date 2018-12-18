import java.io._

import cats.effect.{Resource, Sync}
import cats.syntax.flatMap._
import cats.syntax.functor._

class IOStreamProtocol[I <: InputStream, O <: OutputStream] extends IOProtocol[I, O] {

  import SerializationProtocolInstances._

  private def transmitTo[F[_] : Sync](payload: Array[Byte], destination: OutputStream): F[Long] = for {
    length <- SerializationProtocol[Int].serialize(payload.length)
    _ <- Sync[F].delay(destination.write(length))
    _ <- Sync[F].delay(destination.write(payload))
  } yield payload.length + Integer.BYTES

  private def transmitFrom[F[_] : Sync](source: InputStream): F[Array[Byte]] = for {
    dataStream <- Sync[F].delay(new DataInputStream(source))
    size <- Sync[F].delay(dataStream.readInt())
    entityBuffer <- Sync[F].delay(new Array[Byte](size))
    _ <- Sync[F].delay(dataStream.read(entityBuffer, 0, size))
  } yield entityBuffer

  override def input[F[_] : Sync, A: SerializationProtocol](resource: Resource[F, I]): F[A] = for {
    payload <- resource.use(inputStream => transmitFrom(inputStream))
    entity <- SerializationProtocol[A].deserialize(payload)
  } yield entity

  override def output[F[_] : Sync, A: SerializationProtocol](entity: A, resource: Resource[F, O]): F[Long] = for {
    payload <- SerializationProtocol[A].serialize(entity)
    total <- resource.use(outputStream => transmitTo(payload, outputStream))
  } yield total
}

object IOStreamProtocolInstances {
  implicit val fileIOStreamProtocol: IOProtocol[FileInputStream, FileOutputStream] =
    new IOStreamProtocol[FileInputStream, FileOutputStream]

  implicit val byteArrayIOStreamProtocol: IOProtocol[ByteArrayInputStream, ByteArrayOutputStream] =
    new IOStreamProtocol[ByteArrayInputStream, ByteArrayOutputStream]
}
