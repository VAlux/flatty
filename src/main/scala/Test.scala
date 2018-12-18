import java.io.{File, FileInputStream, FileOutputStream}

import cats.effect.{ExitCode, IOApp, Resource, Sync}
import cats.effect.concurrent.Semaphore

object Test extends IOApp {

  import IOStreamProtocolInstances._
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

  def fromFile[A: SerializationProtocol, F[_] : Concurrent](file: File): F[A] =
    for {
      guard <- Semaphore[F](1)
      entity <- IOProtocol[FileInputStream, FileOutputStream].input[F, A](inputStream(file, guard))
    } yield entity

  def outToFile[A: SerializationProtocol, F[_] : Concurrent](entity: A, file: File): F[Long] =
    for {
      guard <- Semaphore[F](1)
      amount <- IOProtocol[FileInputStream, FileOutputStream].output(entity, outputStream(file, guard))
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
    writeToFile
    readFromFile
  }
}