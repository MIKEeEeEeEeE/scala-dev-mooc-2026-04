package ru.otus.module3.cats.catsconcurrencyhomework


import cats.effect.Sync
import cats.implicits.*
import Wallet.*

import java.nio.file.{Files, Path, Paths, StandardOpenOption}

// DSL управления электронным кошельком
trait Wallet[F[_]] {
  // возвращает текущий баланс
  def balance: F[BigDecimal]
  // пополняет баланс на указанную сумму
  def topup(amount: BigDecimal): F[Unit]
  // списывает указанную сумму с баланса (ошибка если средств недостаточно)
  def withdraw(amount: BigDecimal): F[Either[WalletError, Unit]]
}

// Игрушечный кошелек который сохраняет свой баланс в файл
// todo: реализовать используя java.nio.file._
// Насчёт безопасного конкуррентного доступа и производительности не заморачиваемся, делаем максимально простую рабочую имплементацию. (Подсказка - можно читать и сохранять файл на каждую операцию).
// Важно аккуратно и правильно завернуть в IO все возможные побочные эффекты.
//
// функции которые пригодятся:
// - java.nio.file.Files.write
// - java.nio.file.Files.readString
// - java.nio.file.Files.exists
// - java.nio.file.Paths.get
final class FileWallet[F[_]: Sync](id: WalletId) extends Wallet[F] {

  val walletDir: Path = Paths.get("wallets")
  val walletPath: Path = Paths.get(walletDir.toString, s"$id.txt")

  val initFile: F[Unit] = Sync[F].delay {
    if (!Files.exists(walletDir)) {
      Files.createDirectories(walletDir)
    }
    if (!Files.exists(walletPath)) {
      Files.createFile(walletPath)
      Files.writeString(walletPath, "0")
      print("New wallet")
    }
    ()
  }

  def balance: F[BigDecimal] = initFile *> Sync[F].delay(Files.readString(walletPath))
    .map(content => BigDecimal(content))
    .adaptError { case e =>
      new RuntimeException(s"CRITICAL WALLET ERROR: ${e.getMessage}", e)
    }
  def topup(amount: BigDecimal): F[Unit] = balance.flatMap { current =>
    val new_balance = current + amount
    Sync[F].delay(Files.writeString(walletPath, new_balance.toString))
  }
  def withdraw(amount: BigDecimal): F[Either[WalletError, Unit]] = balance.flatMap { current =>
    if (current >= amount) topup(current - amount).as(Right(()))
    else Sync[F].pure(Left(BalanceTooLow))
  }
}

object Wallet {

  // todo: реализовать конструктор
  // внимание на сигнатуру результата - инициализация кошелька имеет сайд-эффекты
  // Здесь нужно использовать обобщенную версию уже пройденного вами метода IO.delay,
  // вызывается она так: Sync[F].delay(...)
  // Тайпкласс Sync из cats-effect описывает возможность заворачивания сайд-эффектов
  def fileWallet[F[_]: Sync](id: WalletId): F[Wallet[F]] = Sync[F].delay(new FileWallet(id))

  type WalletId = String

  sealed trait WalletError
  case object BalanceTooLow extends WalletError
}