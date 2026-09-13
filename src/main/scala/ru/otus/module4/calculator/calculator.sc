import cats.effect.kernel.Ref
import java.time.Instant
import cats.effect.{IO, Ref}
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import scala.tools.reflect.ToolBoxError

// ------------------------------------------------------------
// 1. Domain event: факт, который уже произошёл
// ------------------------------------------------------------

sealed trait DomainEvent
final case class CalculatorEvent(query: String) extends DomainEvent

// ------------------------------------------------------------
// 2. Envelope: техническая информация вокруг доменного события
// ------------------------------------------------------------

final case class EventEnvelope[E <: DomainEvent](
                                                  eventId: Long,
                                                  version: Int,
                                                  aggregateId: Int,
                                                  occurredAt: Instant,
                                                  payload: E
                                                )


class EventEnvelopeFactory[E <: DomainEvent](
                                              sequenceRef: Ref[IO, Long],
                                              version: Int,
                                              aggregateId: Int
                                            ) {
  def fromPayload(payload: E): IO[EventEnvelope[E]] =
    for {
      id  <- sequenceRef.updateAndGet(_ + 1)
      now <- IO(Instant.now())
    } yield EventEnvelope(
      eventId     = id,
      version     = version,
      aggregateId = aggregateId,
      occurredAt  = now,
      payload     = payload
    )
}

// ------------------------------------------------------------
// 3. Event log: append-only история событий
// ------------------------------------------------------------

final class InMemoryEventLog:
  private var events: Vector[EventEnvelope[? <: DomainEvent]] =
    Vector.empty

  def append[E <: DomainEvent](event: EventEnvelope[E]): Unit =
    events = events :+ event

  def all: Vector[EventEnvelope[? <: DomainEvent]] =
    events

  def byAggregateId(
                     aggregateId: Int
                   ): Vector[EventEnvelope[? <: DomainEvent]] =
    events.filter(_.aggregateId == aggregateId)


// ------------------------------------------------------------
// 4. Aggregate state
//
// Это НЕ CQRS read model.
// Это состояние aggregate, которое можно восстановить из событий.
// ------------------------------------------------------------

sealed trait CalculatorState
case class InitialState() extends CalculatorState
case class TransitionState(value: Int) extends CalculatorState
case class ErrorState(msg: String) extends CalculatorState

object Calculator {
  val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

  def evolve[E <: CalculatorEvent](state: CalculatorState, event: E): CalculatorState = (state, event) match {
    case (st: ErrorState, ev) => st
    case (_: InitialState, ev) => try {
      val res: Int = tb.eval(tb.parse(ev.query)).toString.toInt
      TransitionState(res)
    } catch {
      case exc: java.lang.reflect.InvocationTargetException =>
        val real = exc.getCause
        ErrorState(s"${real.getClass.getSimpleName}: ${real.getMessage}")
      case exc: Exception     => ErrorState(exc.toString)
    }
    case (st: TransitionState, ev) => try {
      val res: Int = tb.eval(tb.parse(st.value.toString ++ ev.query)).toString.toInt
      TransitionState(res)
    } catch {
      case exc: java.lang.reflect.InvocationTargetException =>
        val real = exc.getCause
        ErrorState(s"${real.getClass.getSimpleName}: ${real.getMessage}")
      case exc: Exception     => ErrorState(exc.toString)
    }
  }

  def replay(events: Seq[EventEnvelope[CalculatorEvent]]): CalculatorState =
    events.foldLeft[CalculatorState](InitialState()) {
      case (state, envelope) =>
        val res = evolve(state, envelope.payload)
        println(res)
        res
    }
}

// ------------------------------------------------------------
// 5. Demo
// ------------------------------------------------------------

def prog() = for {
  seqRef  <- Ref[IO].of(0L)
  calculatorId = 42
  eventLog = new InMemoryEventLog
  events = eventLog.byAggregateId(42)
  factory = new EventEnvelopeFactory[CalculatorEvent](seqRef, 1, calculatorId)
  expr1 <- factory.fromPayload(CalculatorEvent("1+2+3"))
  expr2 <- factory.fromPayload(CalculatorEvent("-2"))
  expr3 <- factory.fromPayload(CalculatorEvent("*7"))
  expr4 <- factory.fromPayload(CalculatorEvent("/0"))
  expr5 <- factory.fromPayload(CalculatorEvent("+1"))
  exprs = Seq(expr1, expr2, expr3, expr4, expr5)
  _ = exprs.foreach(eventLog.append)
} yield Calculator.replay(exprs)

import cats.effect.unsafe.implicits.global

val result: CalculatorState = prog().unsafeRunSync()
println(result)



