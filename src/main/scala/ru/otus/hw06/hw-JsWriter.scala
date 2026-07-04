package hw06


sealed trait JsValue

object JsValue {
  case class JsObject(get: Map[String, JsValue]) extends JsValue
  case class JsString(get: String) extends JsValue
  case class JsNumber(get: Double) extends JsValue
  case object JsNull extends JsValue
}


trait JsonWriter[T] {
  def toJson(v: T): JsValue
}

object JsonWriter {

  def apply[T](implicit w: JsonWriter[T]): JsonWriter[T] = w

  def from[T](f: T => JsValue): JsonWriter[T] =
    new JsonWriter[T] {
      def toJson(v: T): JsValue = f(v)
    }

  implicit val stringWriter: JsonWriter[String] = from(JsValue.JsString)

  implicit val intWriter: JsonWriter[Int] = from(i => JsValue.JsNumber(i.toDouble))

  implicit def optionWriter[T](implicit w: JsonWriter[T]): JsonWriter[Option[T]] = from {
      case Some(v) => w.toJson(v)
      case None    => JsValue.JsNull
    }

  def toJson[T](v: T)(implicit w: JsonWriter[T]): JsValue = w.toJson(v)
}


object Main {
  def main(args: Array[String]): Unit = {
    import JsonWriter._

    println(toJson("hello"))
    println(toJson(1))
  }
}