import scala.concurrent.Future

trait Collection[A] extends Traversable[A] {
  def isDynamic: Boolean
}

//TODO: probably move to another file...
class HashBasedCollection[A](override val isDynamic: Boolean) extends Collection[A] {
  private[this] var storage: Map[String, A] = Map.empty

  def put(key: String, value: A): Unit =
    storage.get(key)
      .map(_ => Future.failed(new Exception("Already exists")))
      .getOrElse(storage += key -> value)

  def get(key: String): Future[A] =
    storage.get(key)
      .map(Future.successful)
      .getOrElse(Future.failed(new Exception(s"No value found for key $key")))

  def exists(key: String): Boolean = storage.get(key).isDefined

  override def foreach[U](f: A => U): Unit = storage.values.foreach(f)
}
