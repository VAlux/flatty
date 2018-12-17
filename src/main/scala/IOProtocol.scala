import java.io._

import cats.effect.concurrent.Semaphore
import cats.effect.{ExitCode, IOApp, Resource, Sync}
import cats.syntax.flatMap._
import cats.syntax.functor._

sealed trait IOProtocol[InputResourceType, OutputResourceType] {
  def input[F[_] : Sync, A: SerializationProtocol](resource: Resource[F, InputResourceType]): F[A]

  def output[F[_] : Sync, A: SerializationProtocol](entity: A, resource: Resource[F, OutputResourceType]): F[Long]
}

class IOStreamProtocol[I <: InputStream, O <: OutputStream] extends IOProtocol[I, O] {

  import SerializationProtocolInstances._

  private def transmitTo[F[_] : Sync](payload: Array[Byte], destination: OutputStream)
                                     (implicit serializer: SerializationProtocol[Int]): F[Long] = for {
    length <- serializer.serialize(payload.length)
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
    entity <- implicitly[SerializationProtocol[A]].deserialize(payload)
  } yield entity

  override def output[F[_] : Sync, A: SerializationProtocol](entity: A, resource: Resource[F, O]): F[Long] = for {
    payload <- implicitly[SerializationProtocol[A]].serialize(entity)
    total <- resource.use(outputStream => transmitTo(payload, outputStream))
  } yield total
}

object IOProtocolInstances {
  implicit val fileIOStreamProtocol: IOProtocol[FileInputStream, FileOutputStream] =
    new IOStreamProtocol[FileInputStream, FileOutputStream]

  implicit val byteArrayIOStreamProtocol: IOProtocol[ByteArrayInputStream, ByteArrayOutputStream] =
    new IOStreamProtocol[ByteArrayInputStream, ByteArrayOutputStream]
}

object Test extends IOApp {

  import IOProtocolInstances._
  import SerializationProtocolInstances._
  import cats.effect.{Concurrent, IO}
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

  val ioProtocol: IOProtocol[FileInputStream, FileOutputStream] =
    implicitly[IOProtocol[FileInputStream, FileOutputStream]]

  def fromFile[A: SerializationProtocol, F[_] : Concurrent](file: File): F[A] =
    for {
      guard <- Semaphore[F](1)
      entity <- ioProtocol.input[F, A](inputStream(file, guard))
    } yield entity

  def outToFile[A: SerializationProtocol, F[_] : Concurrent](entity: A, file: File): F[Long] =
    for {
      guard <- Semaphore[F](1)
      amount <- ioProtocol.output(entity, outputStream(file, guard))
    } yield amount

  private def writeToFile = for {
    entity <- IO(9)
    file <- IO(new File("test.dat"))
    written <- outToFile[Int, IO](entity, file)
    _ <- IO(println(s"$written bytes written to test.dat"))
  } yield ExitCode.Success

  private def readFromFile = for {
    file <- IO(new File("test.dat"))
    entity <- fromFile[Int, IO](file)
    _ <- IO(println(s"Payload: [$entity] loaded from the test.dat"))
  } yield ExitCode.Success

  override def run(args: List[String]): IO[ExitCode] = {
    readFromFile
    writeToFile
  }
}