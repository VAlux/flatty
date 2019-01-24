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

  private def createComplexTestCollection: IO[Collection[IO, Int, (Int, String)]] = {
    def create(collection: Collection[IO, Int, (Int, String)], count: Int = 0): IO[Collection[IO, Int, (Int, String)]] = for {
      _ <- collection.put(count, (count, count + "value"))
      result <- if (count < 100000) create(collection, count + 1)
                else IO(collection)
    } yield result

    Collection.empty[IO, Int, (Int, String)].flatMap(coll => create(coll))
  }

  private def writeCollectionToFile[K: SerializationProtocol, V: SerializationProtocol]
  (collection: Collection[IO, K, V]): IO[ExitCode] = for {
    data <- collection.data.take
    written <- toFile[(K, V), IO](data, file)
    _ <- IO(println(s"$written bytes written to test.dat"))
  } yield ExitCode.Success

  private def readFromFile: IO[ExitCode] = for {
    file <- IO(file)
    entities <- fromFile[(Int, (Int, String)), IO](file)
    collection <- Collection[IO, Int, (Int, String)](entities)
    collectionContents <- collection.asString
    _ <- IO(println(s"Payload of [${entities.size}] entities loaded from the ${file.getName}"))
  } yield ExitCode.Success

  override def run(args: List[String]): IO[ExitCode] = {
    createComplexTestCollection.flatMap(collection => writeCollectionToFile(collection)) >> readFromFile
  }
}