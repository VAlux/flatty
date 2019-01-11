package collection

import protocol.SerializationProtocol

sealed trait CollectionElement[A] {
  def payload: A
}

final case class SimpleCollectionElement[A: SerializationProtocol](payload: A)
  extends CollectionElement[A]

final case class KeyBasedCollectionElement[K: SerializationProtocol, A: SerializationProtocol](key: K, payload: A)
  extends CollectionElement[A]