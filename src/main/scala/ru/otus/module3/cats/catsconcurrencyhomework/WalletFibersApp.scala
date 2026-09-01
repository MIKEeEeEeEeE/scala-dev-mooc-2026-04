package ru.otus.module3.cats.catsconcurrencyhomework

import cats.effect.{IO, IOApp}
import cats.implicits._
import scala.concurrent.duration._

// Поиграемся с кошельками на файлах и файберами.

// Нужно написать программу где инициализируются три разных кошелька и для каждого из них работает фоновый процесс,
// который регулярно пополняет кошелек на 100 рублей раз в определенный промежуток времени. Промежуток надо сделать разный, чтобы легче было наблюдать разницу.
// Для определенности: первый кошелек пополняем раз в 100ms, второй каждые 500ms и третий каждые 2000ms.
// Помимо этих трёх фоновых процессов (подсказка - это файберы), нужен четвертый, который раз в одну секунду будет выводить балансы всех трех кошельков в консоль.
// Основной процесс программы должен просто ждать ввода пользователя (IO.readline) и завершить программу (включая все фоновые процессы) когда ввод будет получен.
// Итого у нас 5 процессов: 3 фоновых процесса регулярного пополнения кошельков, 1 фоновый процесс регулярного вывода балансов на экран и 1 основной процесс просто ждущий ввода пользователя.

// Можно делать всё на IO, tagless final тут не нужен.

// Подсказка: чтобы сделать бесконечный цикл на IO достаточно сделать рекурсивный вызов через flatMap:
// def loop(): IO[Unit] = IO.println("hello").flatMap(_ => loop())
object WalletFibersApp extends IOApp.Simple {

  def run: IO[Unit] =
    for {
      _ <- IO.println("Press any key to stop...")
      wallet1 <- Wallet.fileWallet[IO]("6")
      wallet2 <- Wallet.fileWallet[IO]("7")
      wallet3 <- Wallet.fileWallet[IO]("8")
      // todo: запустить все файберы и ждать ввода от пользователя чтобы завершить работу
      fiber1 <- (wallet1.topup(100) *> IO.sleep(100.millis)).foreverM.start
      fiber2 <- (wallet2.topup(100) *> IO.sleep(500.millis)).foreverM.start
      fiber3 <- (wallet3.topup(100) *> IO.sleep(2000.millis)).foreverM.start
      fiber4 <- (
        for {
          b1 <- wallet1.balance
          b2 <- wallet2.balance
          b3 <- wallet3.balance
          _  <- IO.println(s"Wallet 1: $b1 | Wallet 2: $b2 | Wallet 3: $b3")
          _  <- IO.sleep(1000.millis)
        } yield ()
        ).foreverM.start
      _ <- IO.readLine
      _ <- fiber1.cancel
      _ <- fiber2.cancel
      _ <- fiber3.cancel
      _ <- fiber4.cancel
    } yield ()

}