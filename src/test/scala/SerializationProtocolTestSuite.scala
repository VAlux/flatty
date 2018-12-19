import SerializationProtocolInstances._
import cats.effect.IO
import org.scalatest.Assertion

class SerializationProtocolTestSuite extends FlattyBaseTestSuite {
  def assertSerializationIsCorrect[A: SerializationProtocol](entity: A, expected: Array[Byte]): Assertion = {
    val actual: IO[Array[Byte]] = for {
      test <- IO(entity)
      payload <- SerializationProtocol[A].serialize[IO](test)
    } yield payload

    actual.unsafeRunSync() shouldEqual expected
  }

  def assertDeserializationIsCorrect[A: SerializationProtocol](array: Array[Byte], expected: A): Assertion = {
    val actual: IO[A] = for {
      test <- IO(array)
      entity <- SerializationProtocol[A].deserialize[IO](test)
    } yield entity

    actual.unsafeRunSync() shouldEqual expected
  }

  test("Can correctly serialize and deserialize Boolean") {
    assertSerializationIsCorrect(true, Array[Byte](1))
    assertSerializationIsCorrect(false, Array[Byte](0))

    assertDeserializationIsCorrect(Array[Byte](1), true)
    assertDeserializationIsCorrect(Array[Byte](0), false)
  }

  test("Can correctly serialize and deserialize Byte") {
    assertSerializationIsCorrect(1.toByte, Array[Byte](1))
    assertSerializationIsCorrect(0.toByte, Array[Byte](0))
    assertSerializationIsCorrect(-1.toByte, Array[Byte](-1))
    assertSerializationIsCorrect(java.lang.Byte.MAX_VALUE, Array[Byte](java.lang.Byte.MAX_VALUE))
    assertSerializationIsCorrect(java.lang.Byte.MIN_VALUE, Array[Byte](java.lang.Byte.MIN_VALUE))

    assertDeserializationIsCorrect(Array[Byte](1), 1.toByte)
    assertDeserializationIsCorrect(Array[Byte](0), 0.toByte)
    assertDeserializationIsCorrect(Array[Byte](-1), -1.toByte)
    assertDeserializationIsCorrect(Array[Byte](java.lang.Byte.MAX_VALUE), java.lang.Byte.MAX_VALUE)
    assertDeserializationIsCorrect(Array[Byte](java.lang.Byte.MIN_VALUE), java.lang.Byte.MIN_VALUE)
  }
  
  test("Can correctly serialize and deserialize Short") {
    assertSerializationIsCorrect(1.toShort, Array[Byte](0, 1))
    assertSerializationIsCorrect(0.toShort, Array[Byte](0, 0))
    assertSerializationIsCorrect(-1.toShort, Array[Byte](-1, -1))
    assertSerializationIsCorrect(java.lang.Short.MAX_VALUE, Array[Byte](0x7F.toByte, 0xFF.toByte))
    assertSerializationIsCorrect(java.lang.Short.MIN_VALUE, Array[Byte](0x80.toByte, 0x00.toByte))

    assertDeserializationIsCorrect(Array[Byte](0, 1), 1.toShort)
    assertDeserializationIsCorrect(Array[Byte](0, 0), 0.toShort)
    assertDeserializationIsCorrect(Array[Byte](-1, -1), -1.toShort)
    assertDeserializationIsCorrect(Array[Byte](0x7F.toByte, 0xFF.toByte), java.lang.Short.MAX_VALUE)
    assertDeserializationIsCorrect(Array[Byte](0x80.toByte, 0x00.toByte), java.lang.Short.MIN_VALUE)
  }

  test("Can correctly serialize and deserialize Int") {
    assertSerializationIsCorrect(1, Array[Byte](0, 0, 0, 1))
    assertSerializationIsCorrect(0, Array[Byte](0, 0, 0, 0))
    assertSerializationIsCorrect(-1, Array[Byte](-1, -1, -1, -1))
    assertSerializationIsCorrect(java.lang.Integer.MAX_VALUE, Array[Byte](0x7F.toByte, 0xFF.toByte, 0xFF.toByte, 0xFF.toByte))
    assertSerializationIsCorrect(java.lang.Integer.MIN_VALUE, Array[Byte](0x80.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte))

    assertDeserializationIsCorrect(Array[Byte](0, 0, 0, 1), 1)
    assertDeserializationIsCorrect(Array[Byte](0, 0, 0, 0), 0)
    assertDeserializationIsCorrect(Array[Byte](-1, -1, -1, -1), -1)
    assertDeserializationIsCorrect(Array[Byte](0x7F.toByte, 0xFF.toByte, 0xFF.toByte, 0xFF.toByte), java.lang.Integer.MAX_VALUE)
    assertDeserializationIsCorrect(Array[Byte](0x80.toByte, 0x00.toByte, 0x00.toByte, 0x00.toByte), java.lang.Integer.MIN_VALUE)
  }
}
