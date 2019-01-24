import java.io.{File, FileInputStream, FileOutputStream}

import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.implicits._
import collection.Collection
import protocol.{IOProtocol, SerializationProtocol}

//TODO this will be deleted, once it is not needed
// this is just an illustration, experimentation and testing of main concepts.
object Test extends IOApp {

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

  def fileIO[A: SerializationProtocol]: IOProtocol[FileInputStream, FileOutputStream, Iterable[A]] =
    IOProtocol[FileInputStream, FileOutputStream, Iterable[A]]

  def fromFile[A: SerializationProtocol, F[_] : Concurrent](file: File): F[Iterable[A]] = for {
    guard <- Semaphore[F](1)
    entity <- fileIO[A].input(inputStream(file, guard))
  } yield entity

  def toFile[A: SerializationProtocol, F[_] : Concurrent](entity: Iterable[A], file: File): F[Long] = for {
    guard <- Semaphore[F](1)
    amount <- fileIO[A].output(entity, outputStream(file, guard))
  } yield amount

  val file = new File("test.dat")

  private def writeCollectionToFile[K: SerializationProtocol, V: SerializationProtocol]
  (collection: Collection[IO, K, V]): IO[ExitCode] = for {
    data <- collection.data.take
    written <- toFile[(K, V), IO](data, file)
    _ <- IO(println(s"$written bytes written to test.dat"))
  } yield ExitCode.Success

  private def createTestCollection: IO[Collection[IO, Int, String]] = for {
    collection <- Collection.empty[IO, Int, String]
    result1 <- collection.put(1, "this")
    result2 <- collection.put(2, "is")
    result3 <- collection.put(3, "a")
    result4 <- collection.put(4, "test")
    result5 <- collection.put(5, "collection")
    result6 <- collection.put(6, "from")
    result7 <- collection.put(7, "alvo")
  } yield collection

  private def writeToFile: IO[ExitCode] = for {
    entity <- IO(List((1, "test"), (2, "alvo"), (3, "serialization")))
    file <- IO(file)
    written <- toFile[(Int, String), IO](entity, file)
    _ <- IO(println(s"$written bytes written to test.dat"))
  } yield ExitCode.Success

  private def readFromFile: IO[ExitCode] = for {
    file <- IO(file)
    entities <- fromFile[(Int, String), IO](file)
    collection <- Collection[IO, Int, String](entities)
    collectionContents <- collection.asString
    _ <- IO(println(s"Payload: [$entities] entities loaded from the test.dat"))
    _ <- IO(println(s"Collection created: [$collectionContents]"))
  } yield ExitCode.Success

  override def run(args: List[String]): IO[ExitCode] = {
    createTestCollection.flatMap(collection => writeCollectionToFile(collection)) >> readFromFile
  }
}