import java.io._
import java.nio.{ByteBuffer, ByteOrder}

import cats.effect.{Resource, Sync}
import cats.implicits._

sealed trait SerializationProtocol[EntityType] {
  def serialize[F[_] : Sync](entity: EntityType): F[Array[Byte]]

  def deserialize[F[_] : Sync](array: Array[Byte]): F[EntityType]
}

//Just for testing...
final case class User(id: Long, name: String, email: String)

object SerializationProtocolInstances {
  implicit val serializeBoolean: SerializationProtocol[Boolean] = new SerializationProtocol[Boolean] {
    override def serialize[F[_] : Sync](entity: Boolean): F[Array[Byte]] =
      if (entity) Sync[F].delay(Array[Byte](1))
      else Sync[F].delay(Array[Byte](0))

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Boolean] =
      Sync[F].delay(array(0) == 1)
  }
  implicit val serializeByte: SerializationProtocol[Byte] = new SerializationProtocol[Byte] {
    override def serialize[F[_] : Sync](entity: Byte): F[Array[Byte]] =
      Sync[F].delay(Array(entity))

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Byte] =
      Sync[F].delay(array(0))
  }
  implicit val serializeShort: SerializationProtocol[Short] = new SerializationProtocol[Short] {
    private[this] val buffer: ByteBuffer = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN)

    override def serialize[F[_] : Sync](entity: Short): F[Array[Byte]] =
      Sync[F].delay(Array(entity.toByte))

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Short] = {
      def combineTo(first: Byte, second: Byte): Short =
        buffer
          .put(first)
          .put(second)
          .getShort()

      Sync[F].delay {
        array.grouped(2).map {
          case Array(a) => combineTo(a, 0)
          case Array(a, b) => combineTo(a, b)
          case _ => 0.toShort
        }.next()
      }
    }
  }
  implicit val serializeInt: SerializationProtocol[Int] = new SerializationProtocol[Int] {
    private[this] val buffer: ByteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)

    override def serialize[F[_] : Sync](entity: Int): F[Array[Byte]] =
      Sync[F].delay(Array(entity.toByte))

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Int] = {
      def combineTo(first: Byte, second: Byte, third: Byte, fourth: Byte): Int =
        buffer
          .put(first)
          .put(second)
          .put(third)
          .put(fourth)
          .getInt()

      Sync[F].delay {
        array.grouped(4).map {
          case Array(a) => combineTo(a, 0, 0, 0)
          case Array(a, b) => combineTo(a, b, 0, 0)
          case Array(a, b, c) => combineTo(a, b, c, 0)
          case Array(a, b, c, d) => combineTo(a, b, c, d)
          case _ => 0
        }.next()
      }
    }
  }
  implicit val serializeLong: SerializationProtocol[Long] = new SerializationProtocol[Long] {
    private[this] val buffer: ByteBuffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)

    override def serialize[F[_] : Sync](entity: Long): F[Array[Byte]] =
      Sync[F].delay(Array(entity.toByte))

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Long] = {
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

      Sync[F].delay {
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
  }

  implicit val serializeString: SerializationProtocol[String] = new SerializationProtocol[String] {
    override def serialize[F[_] : Sync](entity: String): F[Array[Byte]] =
      Sync[F].delay(entity.getBytes)

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[String] =
      Sync[F].delay(new String(array))
  }
  implicit val byteArraySerializer: SerializationProtocol[Array[Byte]] = new SerializationProtocol[Array[Byte]] {
    override def serialize[F[_] : Sync](entity: Array[Byte]): F[Array[Byte]] =
      Sync[F].delay(entity)

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Array[Byte]] = Sync[F].delay(array)
  }
}

object SerializationProtocolSyntax {

  implicit class SerializationProtocolOperations[A: SerializationProtocol, F[_] : Sync](entity: A) {
    private val serializer: SerializationProtocol[A] = implicitly[SerializationProtocol[A]]

    def ->>(entity2: A): F[Array[Byte]] = combine(entity2)

    def ->>[B: SerializationProtocol](entity2: B): F[Array[Byte]] = combine(entity2)

    def combine(entity2: A): F[Array[Byte]] =
      serializer.serialize(entity) >> serializer.serialize(entity2)

    def combine[B: SerializationProtocol](entity2: B): F[Array[Byte]] =
      serializer.serialize(entity) >> implicitly[SerializationProtocol[B]].serialize(entity2)
  }

  def decode[A: SerializationProtocol, F[_] : Sync](array: Array[Byte]): F[A] =
    implicitly[SerializationProtocol[A]].deserialize(array)

  def encode[A: SerializationProtocol, F[_] : Sync](entity: A): F[Array[Byte]] =
    implicitly[SerializationProtocol[A]].serialize(entity)
}
