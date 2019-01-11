package protocol

import java.nio.ByteBuffer

import cats.effect.Sync
import cats.implicits._

sealed trait SerializationProtocol[A] {
  def serialize[F[_] : Sync](entity: A): F[Array[Byte]]

  def deserialize[F[_] : Sync](array: Array[Byte]): F[A]
}

object SerializationProtocol {
  def apply[A](implicit instance: SerializationProtocol[A]): SerializationProtocol[A] = instance
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
    private val buffer: ByteBuffer = ByteBuffer.allocate(java.lang.Short.BYTES)

    override def serialize[F[_] : Sync](entity: Short): F[Array[Byte]] = for {
      cleanBuffer <- Sync[F].delay(buffer.clear())
      payload <- Sync[F].delay(cleanBuffer.putShort(entity).array().clone())
    } yield payload

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Short] = for {
      cleanBuffer <- Sync[F].delay(buffer.clear())
      result <- Sync[F].delay {
        cleanBuffer.put(array)
        cleanBuffer.flip()
        cleanBuffer.getShort()
      }
    } yield result
  }

  implicit val serializeInt: SerializationProtocol[Int] = new SerializationProtocol[Int] {
    private val buffer: ByteBuffer = ByteBuffer.allocate(java.lang.Integer.BYTES)

    override def serialize[F[_] : Sync](entity: Int): F[Array[Byte]] = for {
      cleanBuffer <- Sync[F].delay(buffer.clear())
      payload <- Sync[F].delay(cleanBuffer.putInt(entity).array().clone())
    } yield payload

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Int] = for {
      cleanBuffer <- Sync[F].delay(buffer.clear())
      result <- Sync[F].delay {
        cleanBuffer.put(array)
        cleanBuffer.flip()
        cleanBuffer.getInt
      }
    } yield result
  }

  implicit val serializeLong: SerializationProtocol[Long] = new SerializationProtocol[Long] {
    private val buffer: ByteBuffer = ByteBuffer.allocate(java.lang.Long.BYTES)

    override def serialize[F[_] : Sync](entity: Long): F[Array[Byte]] = for {
      cleanBuffer <- Sync[F].delay(buffer.clear())
      payload <- Sync[F].delay(cleanBuffer.putLong(entity).array().clone())
    } yield payload

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Long] = for {
      cleanBuffer <- Sync[F].delay(buffer.clear())
      result <- Sync[F].delay {
        cleanBuffer.put(array)
        cleanBuffer.flip()
        cleanBuffer.getLong
      }
    } yield result
  }

  implicit val serializeFloat: SerializationProtocol[Float] = new SerializationProtocol[Float] {
    private val buffer: ByteBuffer = ByteBuffer.allocate(java.lang.Float.BYTES)

    override def serialize[F[_] : Sync](entity: Float): F[Array[Byte]] = for {
      cleanBuffer <- Sync[F].delay(buffer.clear())
      payload <- Sync[F].delay(cleanBuffer.putFloat(entity).array().clone())
    } yield payload

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Float] = for {
      cleanBuffer <- Sync[F].delay(buffer.clear())
      result <- Sync[F].delay {
        cleanBuffer.put(array)
        cleanBuffer.flip()
        cleanBuffer.getFloat
      }
    } yield result
  }

  implicit val serializeDouble: SerializationProtocol[Double] = new SerializationProtocol[Double] {
    private val buffer: ByteBuffer = ByteBuffer.allocate(java.lang.Double.BYTES)

    override def serialize[F[_] : Sync](entity: Double): F[Array[Byte]] = for {
      cleanBuffer <- Sync[F].delay(buffer.clear())
      payload <- Sync[F].delay(cleanBuffer.putDouble(entity).array().clone())
    } yield payload

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Double] = for {
      cleanBuffer <- Sync[F].delay(buffer.clear())
      result <- Sync[F].delay {
        cleanBuffer.put(array)
        cleanBuffer.flip()
        cleanBuffer.getDouble
      }
    } yield result
  }

  implicit val serializeString: SerializationProtocol[String] = new SerializationProtocol[String] {
    override def serialize[F[_] : Sync](entity: String): F[Array[Byte]] =
      Sync[F].delay(entity.getBytes)

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[String] =
      Sync[F].delay(new String(array))
  }

  implicit val serializeByteArray: SerializationProtocol[Array[Byte]] = new SerializationProtocol[Array[Byte]] {
    override def serialize[F[_] : Sync](entity: Array[Byte]): F[Array[Byte]] =
      Sync[F].delay(entity)

    override def deserialize[F[_] : Sync](array: Array[Byte]): F[Array[Byte]] =
      Sync[F].delay(array)
  }
}