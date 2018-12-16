import java.io._

import cats.effect.concurrent.Semaphore
import cats.effect.{ExitCode, IOApp, Resource, Sync}
import cats.syntax.flatMap._
import cats.syntax.functor._

sealed trait IOProtocol[EntityType, InputResourceType, OutputResourceType] {
  def output[F[_] : Sync](entity: EntityType, resource: Resource[F, OutputResourceType]): F[Long]

  def input[F[_] : Sync](resource: Resource[F, InputResourceType]): F[EntityType]
}

object IOProtocolInstances {
  implicit def ioStreamProtocol[A: SerializationProtocol]: IOProtocol[A, FileInputStream, FileOutputStream] =
    new IOProtocol[A, FileInputStream, FileOutputStream] {

      import SerializationProtocolInstances._

      private def transmitTo[F[_] : Sync](payload: Array[Byte], destination: FileOutputStream)
                                         (implicit serializer: SerializationProtocol[Int]): F[Long] =
        for {
          length <- serializer.serialize(payload.length)
          _ <- Sync[F].delay(destination.write(length))
          _ <- Sync[F].delay(destination.write(payload))
        } yield payload.length + 8

      private def transmitFrom[F[_] : Sync](source: FileInputStream): F[Array[Byte]] =
        for {
          dataStream <- Sync[F].delay(new DataInputStream(source))
          size <- Sync[F].delay(dataStream.readInt())
          _ <- Sync[F].delay(println(s"the size is: $size"))
          entityBuffer <- Sync[F].delay(new Array[Byte](size))
          _ <- Sync[F].delay(dataStream.read(entityBuffer, 0, size))
        } yield entityBuffer

      override def output[F[_] : Sync](entity: A, resource: Resource[F, FileOutputStream]): F[Long] =
        for {
          payload <- implicitly[SerializationProtocol[A]].serialize(entity)
          total <- resource.use(outputStream => transmitTo(payload, outputStream))
        } yield total

      override def input[F[_] : Sync](resource: Resource[F, FileInputStream]): F[A] =
        for {
          payload <- resource.use(inputStream => transmitFrom(inputStream))
          entity <- implicitly[SerializationProtocol[A]].deserialize(payload)
        } yield entity
    }

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

  def outToFile[A: SerializationProtocol, F[_] : Concurrent](entity: A, file: File): F[Long] =
    for {
      guard <- Semaphore[F](1)
      amount <- implicitly[IOProtocol[A, FileInputStream, FileOutputStream]].output(entity, outputStream(file, guard))
    } yield amount

  def fromFile[A: SerializationProtocol, F[_] : Concurrent](file: File): F[A] =
    for {
      guard <- Semaphore[F](1)
      entity <- implicitly[IOProtocol[A, FileInputStream, FileOutputStream]].input(inputStream(file, guard))
    } yield entity

  override def run(args: List[String]): IO[ExitCode] = {
    readFromFile
//    writeToFile
  }

  private def writeToFile = for {
    entity <- IO("TEST")
    file <- IO(new File("test.dat"))
    written <- outToFile[String, IO](entity, file)
    _ <- IO(println(s"$written bytes written to test.dat"))
  } yield ExitCode.Success

  private def readFromFile = for {
    file <- IO(new File("test.dat"))
    entity <- fromFile[String, IO](file)
    _ <- IO(println(s"[$entity] loaded from the test.dat"))
  } yield ExitCode.Success
}