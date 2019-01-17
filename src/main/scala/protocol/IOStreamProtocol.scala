package protocol

import java.io._

import cats.effect.{Resource, Sync}

final class IOStreamProtocol[I <: InputStream, O <: OutputStream, A: SerializationProtocol]
  extends IOProtocol[I, O, Iterable[A]] {

  import SerializationProtocolInstances._
  import cats.syntax.flatMap._
  import cats.syntax.functor._

  private def transmitTo[F[_] : Sync](payload: Array[Byte], destination: O): F[Long] = for {
    payloadLength <- SerializationProtocol[Int].serialize(payload.length)
    _ <- Sync[F].delay(destination.write(payloadLength))
    _ <- Sync[F].delay(destination.write(payload))
  } yield payload.length + Integer.BYTES

  private def transmitFrom[F[_] : Sync](source: I): F[Array[Byte]] = {
    def read(inputStream: InputStream, payloads: Array[Byte] = Array.empty): F[Array[Byte]] = for {
      dataStream <- Sync[F].delay(new DataInputStream(inputStream)) // TODO: think, how this can be done more efficiently, without wrapper data stream
      entitySize <- Sync[F].delay(dataStream.readInt())
      entityBuffer <- Sync[F].delay(new Array[Byte](entitySize))
      size <- Sync[F].delay(dataStream.read(entityBuffer))
      _ <- if (size > -1) read(inputStream, payloads ++ entityBuffer)
      else Sync[F].pure(payloads)
    } yield payloads

    read(source)
  }

  override def input[F[_] : Sync](resource: Resource[F, I]): F[Iterable[A]] = for {
    payloads <- resource.use(inputStream => transmitFrom(inputStream))
    entities <- SerializationProtocol[Iterable[A]].deserialize(payloads)
  } yield entities

  override def output[F[_] : Sync](data: Iterable[A], resource: Resource[F, O]): F[Long] = for {
    payload <- SerializationProtocol[Iterable[A]].serialize(data)
    total <- resource.use(outputStream => transmitTo(payload, outputStream))
  } yield total
}

//TODO: think about how this can be generated automatically... maybe shapeless/magnolia/macros/etc?
object IOStreamProtocolInstances {
  implicit def fileIOStreamProtocol[A: SerializationProtocol]: IOProtocol[FileInputStream, FileOutputStream, Iterable[A]] =
    new IOStreamProtocol[FileInputStream, FileOutputStream, A]

  implicit def byteArrayIOStreamProtocol[A: SerializationProtocol]: IOProtocol[ByteArrayInputStream, ByteArrayOutputStream, Iterable[A]] =
    new IOStreamProtocol[ByteArrayInputStream, ByteArrayOutputStream, A]
}
