import java.io._
import java.nio.{ByteBuffer, ByteOrder}

/*sealed trait CollectionElement[A] {
  def value: A

  def size: Long
}

final case class ValueCollectionElement[A](value: A, size: Long) extends CollectionElement[A]

final case class KeyValueCollectionElement[K, A](key: K, value: A, size: Long) extends CollectionElement[A]

sealed trait CollectionElementBuilder[A] {
  def build(entity: A): CollectionElement[A]
}

object CollectionElementBuilderInstances {
  implicit val buildBoolean: CollectionElementBuilder[Boolean] = new CollectionElementBuilder[Boolean] {
    override def build(entity: Boolean): CollectionElement[Boolean] = ValueCollectionElement(entity, 1)
  }
}*/

sealed trait SerializationProtocol[A] {
  def serialize(entity: A): Array[Byte]

  def deserialize(array: Array[Byte]): A
}

//Just for testing...
final case class User(id: Long, name: String, email: String)

object SerializationProtocolInstances {
  implicit val serializeBoolean: SerializationProtocol[Boolean] = new SerializationProtocol[Boolean] {
    override def serialize(entity: Boolean): Array[Byte] = if (entity) Array(1) else Array(0)

    override def deserialize(array: Array[Byte]): Boolean = array(0) == 1
  }
  implicit val serializeByte: SerializationProtocol[Byte] = new SerializationProtocol[Byte] {
    override def serialize(entity: Byte): Array[Byte] = Array(entity)

    override def deserialize(array: Array[Byte]): Byte = array(0)
  }
  implicit val serializeShort: SerializationProtocol[Short] = new SerializationProtocol[Short] {
    private[this] val buffer: ByteBuffer = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN)

    override def serialize(entity: Short): Array[Byte] = Array(entity.toByte)

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

    override def serialize(entity: Int): Array[Byte] = Array(entity.toByte)

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

    override def serialize(entity: Long): Array[Byte] = Array(entity.toByte)

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
    override def serialize(entity: String): Array[Byte] = entity.getBytes

    override def deserialize(array: Array[Byte]): String = new String(array)
  }
  implicit val serializeUser: SerializationProtocol[User] = new SerializationProtocol[User] {

    import SerializationProtocolSyntax._

    override def serialize(user: User): Array[Byte] = {
      user.id ->> user.name ->> user.email
    }

    override def deserialize(array: Array[Byte]): User = {
      val id = decode[Long](array)
      val name = decode[String](array)
      val email = decode[String](array)

      User(id, name, email)
    }
  }
  implicit val byteArraySerializer: SerializationProtocol[Array[Byte]] = new SerializationProtocol[Array[Byte]] {
    override def serialize(entity: Array[Byte]): Array[Byte] = entity

    override def deserialize(array: Array[Byte]): Array[Byte] = array
  }
}

object SerializationProtocolSyntax {

  implicit class SerializationProtocolOperations[A: SerializationProtocol](entity: A) {
    private val serializer: SerializationProtocol[A] = implicitly[SerializationProtocol[A]]

    def >>-(stream: OutputStream): Unit = serialize(stream)

    def ->>(entity2: A): Array[Byte] = combine(entity2)

    def ->>[B: SerializationProtocol](entity2: B): Array[Byte] = combine(entity2)

    def combine(entity2: A): Array[Byte] = serializer.serialize(entity) ++ serializer.serialize(entity2)

    def combine[B: SerializationProtocol](entity2: B): Array[Byte] =
      serializer.serialize(entity) ++ implicitly[SerializationProtocol[B]].serialize(entity2)

    def serialize(stream: OutputStream): Unit = {
      val payload = serializer.serialize(entity)
      stream.write(payload.length)
      stream.write(payload)
    }
  }

  def decode[A: SerializationProtocol](array: Array[Byte]): A =
    implicitly[SerializationProtocol[A]].deserialize(array)

  def decode[A: SerializationProtocol](stream: InputStream): A =
    decode(stream.readAllBytes())
}
