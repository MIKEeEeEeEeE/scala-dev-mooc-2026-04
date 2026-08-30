package ru.otus.module3

import zio._
import zio.Clock
import zio.Console
import zio.Random

import scala.language.postfixOps
import ru.otus.module3.zio_homework.config.{AppConfig, Configuration}

import java.util.concurrent.TimeUnit

package object zio_homework {
  /**
   * 1.
   * Используя сервисы Random и Console, напишите консольную ZIO программу которая будет предлагать пользователю угадать число от 1 до 3
   * и печатать в консоль угадал или нет. Подумайте, на какие наиболее простые эффекты ее можно декомпозировать.
   */

  val guessProgram: ZIO[Any, Throwable, Unit] = for {
    _     <- Console.printLine("Guess between 1 and 3:")
    target <- Random.nextIntBetween(1, 4)
    input  <- Console.readLine
    _      <- input.toIntOption match {
      case Some(guess) if guess == target =>
        Console.printLine("You guessed!")
      case Some(_) =>
        Console.printLine(s"Wrong! It was $target!")
      case None =>
        Console.printLine("That was not a valid number!")
    }
  } yield ()

  /**
   * 2. реализовать функцию doWhile (общего назначения), которая будет выполнять эффект до тех пор, пока его значение в условии не даст true
   * 
   */

  def doWhile[R, E](effect: ZIO[R, E, Boolean]): ZIO[R, E, Boolean] =
    effect.repeatUntil(_ == true)

  /**
   * 3. Реализовать метод, который безопасно прочитает конфиг из переменных окружения, а в случае ошибки вернет дефолтный конфиг
   * и выведет его в консоль
   * Используйте эффект "Configuration.config" из пакета config
   */

  def loadConfigOrDefault = {
    Configuration.config.foldZIO(
      err => Console.printLine(s"Ошибка загрузки конфига: $err").as(AppConfig("127.0.0.1", "8000")),
      cfg => Console.printLine(s"Конфиг успешно загружен: $cfg").as(cfg)
    )
  }

  /**
   * 4. Следуйте инструкциям ниже для написания 2-х ZIO программ,
   * обратите внимание на сигнатуры эффектов, которые будут у вас получаться,
   * на изменение этих сигнатур
   */


  /**
   * 4.1 Создайте эффект, который будет возвращать случайным образом выбранное число от 0 до 10 спустя 1 секунду
   * Используйте сервис zio Random
   */
  lazy val eff: ZIO[Any, Nothing, Int] = Random.nextIntBetween(0,11) zipLeft Clock.sleep(1 second)

  /**
   * 4.2 Создайте коллукцию из 10 выше описанных эффектов (eff)
   */
  lazy val effects = List.fill(10)(eff)

  
  /**
   * 4.3 Напишите программу которая вычислит сумму элементов коллекции "effects",
   * напечатает ее в консоль и вернет результат, а также залогирует затраченное время на выполнение,
   * можно использовать ф-цию printEffectRunningTime, которую мы разработали на занятиях
   */

  lazy val app = ZIO.foreach(effects)(identity).map(_.sum).tap(sum => Console.printLine(s"Sum: $sum"))


  /**
   * 4.4 Усовершенствуйте программу 4.3 так, чтобы минимизировать время ее выполнения
   */

  lazy val appSpeedUp = ZIO.foreachPar(effects)(identity).map(_.sum)


  /**
   * 5. Оформите ф-цию printEffectRunningTime разработанную на занятиях в отдельный сервис, так чтобы ее
   * можно было использовать аналогично zio.Console.printLine например
   */

  object EffectiveRunningTime {

    def printEffectRunningTime[R, E, A](eff: ZIO[R, E, A]): ZIO[R, E, A] = {
      for {
        start <- Clock.currentTime(TimeUnit.SECONDS)
        result <- eff
        end <- Clock.currentTime(TimeUnit.SECONDS)
        _ <- Console.printLine(s"Time taken: ${end - start}").orDie
      } yield result
    }

    def apply[R, E, A](eff: ZIO[R, E, A]): ZIO[R, E, A] = printEffectRunningTime(eff)
  }


   /**
     * 6.
     * Воспользуйтесь написанным сервисом, чтобы создать эффект, который будет логировать время выполнения программы из пункта 4.3
     *
     * 
     */

  lazy val appWithTimeLogg = EffectiveRunningTime.printEffectRunningTime(appSpeedUp)


  /**
    * 
    * Подготовьте его к запуску и затем запустите воспользовавшись ZioHomeWorkApp
    */

  lazy val runApp = appWithTimeLogg

}
