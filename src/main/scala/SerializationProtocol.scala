import java.nio.ByteBuffer

import cats.effect.Sync
import cats.implicits._

sealed trait SerializationProtocol[EntityType] {
  def serialize[F[_] : Sync](entity: EntityType): F[Array[Byte]]

  def deserialize[F[_] : Sync](array: Array[Byte]): F[EntityType]
}

object SerializationProtocolInstances {
  implicit val serializeBoolean: SerializationProtocol[Boolean] = new SerializationProtocol[Boolean] {
    override def serialize[F[_] : Sync](entity: Boolean): F[Array[Byte]] =
      if (entity) Sync[F].delay(Array[Byte](1))
      else Sync[F].delay(Array[Byte](0))

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Boolean] =
      Sync[F].delay(array.headOption.getOrElse(0) == 1)
  }
  implicit val serializeByte: SerializationProtocol[Byte] = new SerializationProtocol[Byte] {
    override def serialize[F[_] : Sync](entity: Byte): F[Array[Byte]] =
      Sync[F].delay(Array(entity))

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Byte] =
      Sync[F].delay(array.headOption.getOrElse(0))
  }
  implicit val serializeShort: SerializationProtocol[Short] = new SerializationProtocol[Short] {
    private val buffer: ByteBuffer = ByteBuffer.allocate(2)

    override def serialize[F[_] : Sync](entity: Short): F[Array[Byte]] =
      Sync[F].delay(Array(entity.toByte))

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Short] =
      Sync[F].delay {
        buffer.put(array)
        buffer.flip()
        buffer.getShort()
      }
  }
  implicit val serializeInt: SerializationProtocol[Int] = new SerializationProtocol[Int] {
    private val buffer: ByteBuffer = ByteBuffer.allocate(4)

    override def serialize[F[_] : Sync](entity: Int): F[Array[Byte]] =
      Sync[F].delay(Array(entity.toByte))

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Int] =
      Sync[F].delay {
        buffer.put(array)
        buffer.flip()
        buffer.getInt
      }
  }

  implicit val serializeLong: SerializationProtocol[Long] = new SerializationProtocol[Long] {
    private val buffer: ByteBuffer = ByteBuffer.allocate(8)

    override def serialize[F[_] : Sync](entity: Long): F[Array[Byte]] =
      Sync[F].delay(Array(entity.toByte))

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Long] =
      Sync[F].delay {
        buffer.put(array)
        buffer.flip()
        buffer.getLong()
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
