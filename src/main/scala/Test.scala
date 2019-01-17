import java.io.{File, FileInputStream, FileOutputStream}

import cats.effect.concurrent.Semaphore
import cats.effect.{ExitCode, IOApp, Resource, Sync}
import protocol.{IOProtocol, SerializationProtocol}

//TODO this will be deleted, once it is not needed
// this is just an illustration, experimentation and testing of main concepts.
object Test extends IOApp {

  import cats.effect.{Concurrent, IO}
  import cats.implicits._
  import protocol.IOStreamProtocolInstances._
  import protocol.SerializationProtocolInstances._

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

  def fromFile[A: SerializationProtocol, F[_] : Concurrent](file: File): F[Iterable[A]] =
    for {
      guard <- Semaphore[F](1)
      entity <- IOProtocol[FileInputStream, FileOutputStream, Iterable[A]].input(inputStream(file, guard))
    } yield entity

  def toFile[A: SerializationProtocol, F[_] : Concurrent](entity: Iterable[A], file: File): F[Long] =
    for {
      guard <- Semaphore[F](1)
      amount <- IOProtocol[FileInputStream, FileOutputStream, Iterable[A]].output(entity, outputStream(file, guard))
    } yield amount

  val file = new File("test.dat")

  private def writeToFile: IO[ExitCode] = for {
    entity <- IO(List(10f, 20f, 30f, 40f))
    file <- IO(file)
    written <- toFile[Float, IO](entity, file)
    _ <- IO(println(s"$written bytes written to test.dat"))
  } yield ExitCode.Success

  private def readFromFile: IO[ExitCode] = for {
    file <- IO(file)
    entities <- fromFile[Float, IO](file)
    _ <- IO(println(s"Payload: [$entities] loaded from the test.dat"))
  } yield ExitCode.Success

  override def run(args: List[String]): IO[ExitCode] = {
    writeToFile >> readFromFile
  }
}