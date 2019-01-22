package protocol

import java.io._

import cats.effect.{Resource, Sync}

final class IOStreamProtocol[I <: InputStream, O <: OutputStream, A: SerializationProtocol]
  extends IOProtocol[I, O, Iterable[A]] {

  import SerializationProtocolInstances._
  import cats.syntax.flatMap._
  import cats.syntax.functor._

  val sizeBuffer = new Array[Byte](Integer.BYTES)

  private def transmitTo[F[_] : Sync](payload: Array[Byte], destination: O): F[Long] = for {
    _ <- Sync[F].delay(destination.write(payload))
  } yield payload.length

  private def transmitFrom[F[_] : Sync](source: I): F[Array[Byte]] = {

    def read(inputStream: InputStream, payloads: Array[Byte] = Array.empty): F[Array[Byte]] = for {
      entitySizeBytesAmount <- Sync[F].delay(inputStream.read(sizeBuffer))
      entitySize <- if (entitySizeBytesAmount > -1) SerializationProtocol[Int].deserialize(sizeBuffer) else Sync[F].pure(-1)
      entityBuffer <- if (entitySize > -1) Sync[F].delay(new Array[Byte](entitySize)) else Sync[F].pure(Array.empty[Byte])
      size <- if (entityBuffer.length > 0) Sync[F].delay(inputStream.read(entityBuffer)) else Sync[F].pure(-1)
      newPayloads <- Sync[F].delay(payloads ++ sizeBuffer ++ entityBuffer)
      result <- if (size > -1) read(inputStream, newPayloads)
                else Sync[F].pure(payloads)
    } yield result

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

object IOStreamProtocolInstances {
  // For any input/output stream we need only one generic implicit instance
  implicit def ioStream[I <: InputStream, O <: OutputStream, A: SerializationProtocol]: IOProtocol[I, O, Iterable[A]] =
    new IOStreamProtocol[I, O, A]
}
