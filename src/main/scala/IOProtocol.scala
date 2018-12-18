import cats.effect.{Resource, Sync}

trait IOProtocol[InputResourceType, OutputResourceType] {
  def input[F[_] : Sync, A: SerializationProtocol](resource: Resource[F, InputResourceType]): F[A]

  def output[F[_] : Sync, A: SerializationProtocol](entity: A, resource: Resource[F, OutputResourceType]): F[Long]
}

object IOProtocol {
  def apply[I, O](implicit instance: IOProtocol[I, O]): IOProtocol[I, O] = instance
}