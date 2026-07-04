package ru.otus.module2

import ru.otus.module2.catsHomework.*
import ru.otus.module2.catsHomework.given

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


package object catsHomework {

  /**
   * Простое бинарное дерево
   * @tparam A
   */
  sealed trait Tree[+A]
  final case class Branch[A](left: Tree[A], right: Tree[A])
    extends Tree[A]
  final case class Leaf[A](value: A) extends Tree[A]

  /**
   * Напишите instance Functor для объявленного выше бинарного дерева.
   * Проверьте, что код работает корректно для Branch и Leaf
   */

  trait Functor[F[_]] {
    def map[A,B](fa: F[A])(f: A => B): F[B]
  }

  given Functor[Tree] with {
    override def map[A, B](fa: Tree[A])(f: A => B): Tree[B] = fa match {
      case Branch(l, r) => Branch(map(l)(f), map(r)(f))
      case Leaf(value) => Leaf(f(value))
    }
  }


  /**
   * Monad абстракция для последовательной
   * комбинации вычислений в контексте F
   * @tparam F
   */
  trait Monad[F[_]]{
    def flatMap[A,B](fa: F[A])(f: A => F[B]): F[B]
    def pure[A](v: A): F[A]
  }


  /**
   * MonadError расширяет возможность Monad
   * кроме последовательного применения функций, позволяет обрабатывать ошибки
   * @tparam F
   * @tparam E
   */
  trait MonadError[F[_], E] extends Monad[F]{
    // Поднимаем ошибку в контекст `F`:
    def raiseError[A](e: E): F[A]

    // Обработка ошибки, потенциальное восстановление:
    def handleErrorWith[A](fa: F[A])(f: E => F[A]): F[A]

    // Обработка ошибок, восстановление от них:
    def handleError[A](fa: F[A])(f: E => A): F[A]

    // Test an instance of `F`,
    // failing if the predicate is not satisfied:
    def ensure[A](fa: F[A])(e: E)(f: A => Boolean): F[A]
  }

  /**
   * Напишите instance MonadError для Try
   */

  given MonadError[Try, Throwable] with {

    override def raiseError[A](e: Throwable): Try[A] = Failure(e)

    override def handleErrorWith[A](fa: Try[A])(f: Throwable => Try[A]): Try[A] = fa match {
      case Success(a) => Success(a)
      case Failure(a) => f(a)
    }

    override def handleError[A](fa: Try[A])(f: Throwable => A): Try[A] = fa match {
      case Success(a) => Success(a)
      case Failure(a) => Try(f(a))
    }

    override def ensure[A](fa: Try[A])(e: Throwable)(f: A => Boolean): Try[A] = fa match {
      case Success(a) => if (f(a)) Success(a) else Failure(e)
      case Failure(a) => Failure(a)
    }

    override def flatMap[A, B](fa: Try[A])(f: A => Try[B]): Try[B] = fa match {
      case Success(a) => f(a)
      case Failure(a) => Failure(a)
    }

    override def pure[A](v: A): Try[A] = Try(v)
  }

  /**
   * Напишите instance MonadError для Either,
   * где в качестве типа ошибки будет String
   */
  given MonadError[[A] =>> Either[String, A], String] with {

    override def raiseError[A](e: String): Either[String, A] = Left(e)

    override def handleErrorWith[A](fa: Either[String, A])(f: String => Either[String, A]): Either[String, A] = fa match {
      case Left(a) => f(a)
      case Right(a) => Right(a)
    }

    override def handleError[A](fa: Either[String, A])(f: String => A): Either[String, A] = fa match {
      case Left(a) => Right(f(a))
      case Right(a) => Right(a)
    }

    override def ensure[A](fa: Either[String, A])(e: String)(f: A => Boolean): Either[String, A] = fa match {
      case Right(a) => if (f(a)) Right(a) else Left(e)
      case Left(a) => Left(a)
    }

    override def flatMap[A, B](fa: Either[String, A])(f: A => Either[String, B]): Either[String, B] = fa match {
      case Right(a) => f(a)
      case Left(a) => Left(a)
    }

    override def pure[A](v: A): Either[String, A] = Right(v)
  }
}

@main
def main2 = {

  val tree: Tree[Int] = Branch(Branch(Leaf(0), Leaf(1)), Branch(Leaf(2), Branch(Leaf(3), Leaf(4))))

  val result = summon[Functor[Tree]].map(tree)(_ * 10)

  println(result) // Branch(Branch(Leaf(0),Leaf(10)),Branch(Leaf(20),Branch(Leaf(30),Leaf(40))))

  val me = summon[MonadError[Try, Throwable]].pure(10) // Success(1)

  val meSuccess = summon[MonadError[Try, Throwable]].flatMap(me)(v => Try(v/2))
  val meFailure = summon[MonadError[Try, Throwable]].flatMap(me)(v => Try(v/0))

  println(meSuccess) // Success(5)
  println(meFailure) // Failure(java.lang.ArithmeticException: / by zero)

  val meRaiseError = summon[MonadError[Try, Throwable]].raiseError(ArithmeticException("Do not divide by zero!"))

  println(meRaiseError) // Failure(java.lang.ArithmeticException: Do not divide by zero!)

  val meHandleError = summon[MonadError[Try, Throwable]].handleError(meFailure){case _: Exception => "Oops!"}

  println(meHandleError) // Success(Oops!)

  val meHandleErrorWith = summon[MonadError[Try, Throwable]].handleErrorWith(meFailure) { error =>  Failure(Exception("Oops!")) }

  println(meHandleErrorWith) // Failure(java.lang.Exception: Oops!)

  val meEnsure = summon[MonadError[Try, Throwable]].ensure(meSuccess)(Exception("Should be larger than 5"))(_ > 5)

  println(meEnsure) // Failure(java.lang.Exception: Should be larger than 5)

}
