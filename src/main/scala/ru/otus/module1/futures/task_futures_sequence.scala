package ru.otus.module1.futures

import ru.otus.module1.futures.HomeworksUtils.task

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import scala.util.Try

object task_futures_sequence {

  /**
   * В данном задании Вам предлагается реализовать функцию fullSequence,
   * похожую на Future.sequence, но в отличие от нее,
   * возвращающую все успешные и не успешные результаты.
   * Возвращаемое тип функции - кортеж из двух списков,
   * в левом хранятся результаты успешных выполнений,
   * в правовой результаты неуспешных выполнений.
   * Не допускается использование методов объекта Await и мутабельных переменных var
   */
  /**
   * @param futures список асинхронных задач
   * @return асинхронную задачу с кортежом из двух списков
   */

   def fullSequence[A](futures: List[Future[A]])
                     (implicit ex: ExecutionContext): Future[(List[A], List[Throwable])] =
     futures.foldLeft(Future.successful((List.empty[A], List.empty[Throwable]))) {
       (accF, nextF) =>
         for {
           case (s, f) <- accF
           t <- toTry(nextF)
         } yield t match {
           case scala.util.Success(v) => (s :+ v, f)
           case scala.util.Failure(e) => (s, f :+ e)
         }
     }

  def toTry[A](f: Future[A])(implicit ec: ExecutionContext): Future[Try[A]] =
    f.map(Success(_)).recover { case e => scala.util.Failure(e) }

}
