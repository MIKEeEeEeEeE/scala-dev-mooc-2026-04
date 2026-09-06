import cats.effect.{IO, IOApp, Ref}
import com.comcast.ip4s.{Host, Port}
import fs2.{Chunk, Stream}
import io.circe.Json
import org.http4s.{HttpRoutes, Request}
import org.http4s.circe.*
import org.http4s.dsl.io.*
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Router
import org.http4s.client.Client

import scala.concurrent.duration.*
import cats.effect.unsafe.implicits.global
import org.http4s.implicits.uri

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import fs2.Stream
import io.circe.Json
import org.http4s.*
import org.http4s.circe.*
import org.http4s.client.Client
import org.http4s.dsl.io.*
import org.http4s.implicits.*
import scala.concurrent.duration.*


object HttpClient extends IOApp.Simple {

  val numbers: Stream[IO, Byte] = Stream.constant('1'.toByte)

  object PosInt {
    def unapply(str: String): Option[Int] =
      str.toIntOption.filter(_ > 0)
  }

  def counterRoute(ref: Ref[IO, Int]): HttpRoutes[IO] = HttpRoutes.of {
    case GET -> Root =>
      for {
        value    <- ref.updateAndGet(_ + 1)
        response <- Ok(Json.obj("value" -> Json.fromInt(value)))
      } yield response
  }

  val streamResponse: HttpRoutes[IO] = HttpRoutes.of {
    case GET -> Root / PosInt(chunk) / PosInt(total) / PosInt(time) =>
      val resultStream = numbers
        .take(total.toLong)
        .chunkN(chunk)
        .zipLeft(Stream.awakeEvery[IO](time.seconds))
        .flatMap(Stream.chunk)

      Ok(resultStream)

    case GET -> Root / _ / _ / _ =>
      BadRequest("Bad Request")
  }

  override val run: IO[Unit] = for {
    ref <- Ref[IO].of(0)

    router = Router(
      "/counter" -> counterRoute(ref),
      "/show"    -> streamResponse
    )

    _ <- EmberServerBuilder
      .default[IO]
      .withHost(Host.fromString("localhost").get)
      .withPort(Port.fromInt(8081).get)
      .withHttpApp(router.orNotFound)
      .build
      .use(_ => IO.never)
  } yield ()


  val testSuite: IO[Unit] = for {
    ref <- Ref[IO].of(99)

    router = Router(
      "/counter" -> counterRoute(ref),
      "/show"    -> streamResponse
    )

    client = Client.fromHttpApp(router.orNotFound)

    res1 <- client.expect[Json](uri"/counter")
    _    <- IO.println(res1)

    res2 <- client.expect[Json](uri"/counter")
    _    <- IO.println(res2)

    bytes = client.stream(Request[IO](Method.GET, uri"/show/2/2/1")).flatMap(_.body).compile.toList
    _     <- IO.println(bytes)

    status <- client.status(Request[IO](Method.GET, uri"/show/abc/10/1"))
    _      <- IO.println(status)

  } yield ()

}


HttpClient.testSuite.unsafeRunSync()
