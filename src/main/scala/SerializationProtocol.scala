import java.io._
import java.nio.{ByteBuffer, ByteOrder}

import cats.effect.{IO, Resource, Sync}
import cats.implicits._

sealed trait SerializationProtocol[A] {
  def serialize(entity: A): IO[Array[Byte]]

  def deserialize(array: Array[Byte]): A
}

//Just for testing...
final case class User(id: Long, name: String, email: String)

object SerializationProtocolInstances {
  implicit val serializeBoolean: SerializationProtocol[Boolean] = new SerializationProtocol[Boolean] {
    override def serialize(entity: Boolean): IO[Array[Byte]] =
      if (entity) IO(Array[Byte](1)) else IO(Array[Byte](0))

    override def deserialize(array: Array[Byte]): Boolean = array(0) == 1
  }
  implicit val serializeByte: SerializationProtocol[Byte] = new SerializationProtocol[Byte] {
    override def serialize(entity: Byte) = IO(Array(entity))

    override def deserialize(array: Array[Byte]): Byte = array(0)
  }
  implicit val serializeShort: SerializationProtocol[Short] = new SerializationProtocol[Short] {
    private[this] val buffer: ByteBuffer = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN)

    override def serialize(entity: Short): IO[Array[Byte]] = IO(Array(entity.toByte))

    override def deserialize(array: Array[Byte]): Short = {
      def combineTo(first: Byte, second: Byte): Short =
        buffer
          .put(first)
          .put(second)
          .getShort()

      array.grouped(2).map {
        case Array(a) => combineTo(a, 0)
        case Array(a, b) => combineTo(a, b)
        case _ => 0.toShort
      }.next()
    }
  }
  implicit val serializeInt: SerializationProtocol[Int] = new SerializationProtocol[Int] {
    private[this] val buffer: ByteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)

    override def serialize(entity: Int): IO[Array[Byte]] = IO(Array(entity.toByte))

    override def deserialize(array: Array[Byte]): Int = {
      def combineTo(first: Byte, second: Byte, third: Byte, fourth: Byte): Int =
        buffer
          .put(first)
          .put(second)
          .put(third)
          .put(fourth)
          .getInt()

      array.grouped(4).map {
        case Array(a) => combineTo(a, 0, 0, 0)
        case Array(a, b) => combineTo(a, b, 0, 0)
        case Array(a, b, c) => combineTo(a, b, c, 0)
        case Array(a, b, c, d) => combineTo(a, b, c, d)
        case _ => 0
      }.next()
    }
  }
  implicit val serializeLong: SerializationProtocol[Long] = new SerializationProtocol[Long] {
    private[this] val buffer: ByteBuffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)

    override def serialize(entity: Long) = IO(Array(entity.toByte))

    override def deserialize(array: Array[Byte]): Long = {
      def combineTo(first: Byte, second: Byte, third: Byte, fourth: Byte,
                    fifth: Byte, sixth: Byte, seventh: Byte, eighth: Byte): Long =
        buffer
          .put(first)
          .put(second)
          .put(third)
          .put(fourth)
          .put(fifth)
          .put(sixth)
          .put(seventh)
          .put(eighth)
          .getLong()

      array.grouped(8).map {
        case Array(a) => combineTo(a, 0, 0, 0, 0, 0, 0, 0)
        case Array(a, b) => combineTo(a, b, 0, 0, 0, 0, 0, 0)
        case Array(a, b, c) => combineTo(a, b, c, 0, 0, 0, 0, 0)
        case Array(a, b, c, d) => combineTo(a, b, c, d, 0, 0, 0, 0)
        case Array(a, b, c, d, e) => combineTo(a, b, c, d, e, 0, 0, 0)
        case Array(a, b, c, d, e, f) => combineTo(a, b, c, d, e, f, 0, 0)
        case Array(a, b, c, d, e, f, g) => combineTo(a, b, c, d, e, f, g, 0)
        case Array(a, b, c, d, e, f, g, h) => combineTo(a, b, c, d, e, f, g, h)
        case _ => 0.toLong
      }.next()
    }
  }

  implicit val serializeString: SerializationProtocol[String] = new SerializationProtocol[String] {
    override def serialize(entity: String) = IO(entity.getBytes)

    override def deserialize(array: Array[Byte]): String = new String(array)
  }
  implicit val serializeUser: SerializationProtocol[User] = new SerializationProtocol[User] {

    import SerializationProtocolSyntax._

    override def serialize(user: User): IO[Array[Byte]] = IO {
      user.id ->> user.name
    }

    override def deserialize(array: Array[Byte]): User = {
      val id = decode[Long](array)
      val name = decode[String](array)
      val email = decode[String](array)

      User(id, name, email)
    }
  }
  implicit val byteArraySerializer: SerializationProtocol[Array[Byte]] = new SerializationProtocol[Array[Byte]] {
    override def serialize(entity: Array[Byte]) = IO(entity)

    override def deserialize(array: Array[Byte]): Array[Byte] = array
  }
}

object SerializationProtocolSyntax {

  implicit class SerializationProtocolOperations[A: SerializationProtocol](entity: A) {
    private val serializer: SerializationProtocol[A] = implicitly[SerializationProtocol[A]]

    def ->>(entity2: A): IO[Array[Byte]] = combine(entity2)

    def ->>[B: SerializationProtocol](entity2: B): IO[Array[Byte]] = combine(entity2)

    def combine(entity2: A): IO[Array[Byte]] =
      serializer.serialize(entity) >> serializer.serialize(entity2)

    def combine[B: SerializationProtocol](entity2: B): IO[Array[Byte]] =
      serializer.serialize(entity) >> implicitly[SerializationProtocol[B]].serialize(entity2)
  }

  def decode[A: SerializationProtocol](array: Array[Byte]): A =
    implicitly[SerializationProtocol[A]].deserialize(array)

  def decode[A: SerializationProtocol](stream: InputStream): A =
    decode(stream.readAllBytes())
}

sealed trait OutputProtocol[EntityType, ResourceType] {
  def output[F[_] : Sync](entity: EntityType, stream: Resource[F, ResourceType])
                         (implicit serializationProtocol: SerializationProtocol[EntityType]): F[Long]
}

object OutputProtocolInstances {
  implicit def outputStreamProtocol[A]: OutputProtocol[A, OutputStream] = new OutputProtocol[A, OutputStream] {
    def transmit[F[_] : Sync](payload: Array[Byte], destination: OutputStream): F[Long] =
      Sync[F].delay {
        destination.write(payload.length)
        destination.write(payload)
        payload.length
      }

    override def output[F[_] : Sync](entity: A, stream: Resource[F, OutputStream])
                                    (implicit serializationProtocol: SerializationProtocol[A]): F[Long] = for {
      payload <- serializationProtocol.serialize(entity)
      total <- stream.use(outputStream => transmit(payload, outputStream))
    } yield total
  }
}

