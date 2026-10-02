import java.time.Instant
import java.util.UUID
import zio.*

import scala.reflect.runtime.universe
import scala.tools.reflect.{ToolBox, ToolBoxError}

// ------------------------------------------------------------
// 1. Domain event: факт, который уже произошёл
// ------------------------------------------------------------

sealed trait DomainEvent
final case class CalculatorEvent(query: String) extends DomainEvent
final case class ResetStateEvent() extends DomainEvent

// ------------------------------------------------------------
// 2. Commands
// ------------------------------------------------------------

sealed trait CalculatorCommand {
  def aggregateId: Int
}
final case class Execute(aggregateId: Int, query: String) extends CalculatorCommand
final case class Initialize(aggregateId: Int) extends CalculatorCommand
final case class Recover(aggregateId: Int) extends CalculatorCommand

// ------------------------------------------------------------
// 3. Event envelope
// ------------------------------------------------------------

final case class EventEnvelope[+E <: DomainEvent](
                                                   eventId: Long,
                                                   version: Int,
                                                   aggregateId: Int,
                                                   occurredAt: Instant,
                                                   payload: E
                                                 )

class EventEnvelopeFactory[E <: DomainEvent](
                                              sequenceRef: Ref[Int],
                                              version: Int,
                                              aggregateId: Int
                                            ) {
  def fromPayload(payload: E): UIO[EventEnvelope[E]] =
    for {
      id <- sequenceRef.updateAndGet(_ + 1)
    } yield EventEnvelope(
      eventId = id,
      version = version,
      aggregateId = aggregateId,
      occurredAt = Instant.now(),
      payload = payload
    )
}

// ------------------------------------------------------------
// 4. Event log
// ------------------------------------------------------------

trait EventLog {
  def append(event: EventEnvelope[DomainEvent]): UIO[Unit]
  def byAggregateId(aggregateId: Int): UIO[Vector[EventEnvelope[DomainEvent]]]
}

final class InMemoryEventLog(
                              ref: Ref[Vector[EventEnvelope[DomainEvent]]]
                            ) extends EventLog {

  override def append(event: EventEnvelope[DomainEvent]): UIO[Unit] =
    ref.update(_ :+ event)

  override def byAggregateId(aggregateId: Int): UIO[Vector[EventEnvelope[DomainEvent]]] =
    ref.get.map(_.filter(_.aggregateId == aggregateId))
}

object InMemoryEventLog {
  val layer: ULayer[EventLog] =
    ZLayer.fromZIO(
      Ref
        .make(Vector.empty[EventEnvelope[DomainEvent]])
        .map(new InMemoryEventLog(_))
    )
}

// ------------------------------------------------------------
// 5. Aggregate state
// ------------------------------------------------------------

sealed trait CalculatorState
case class InitialState() extends CalculatorState
case class TransitionState(value: Int) extends CalculatorState
case class ErrorState(msg: String) extends CalculatorState

// ------------------------------------------------------------
// 6. Domain errors
// ------------------------------------------------------------

sealed trait CalculatorError
final case class ArithmeticDomainError(msg: String) extends CalculatorError
final case class ParseDomainError(msg: String) extends CalculatorError
final case class StateDomainError(msg: String) extends CalculatorError

// ------------------------------------------------------------
// 7. Aggregate layer
// ------------------------------------------------------------

object Calculator {
  private val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

  def evolve(state: CalculatorState, event: DomainEvent): CalculatorState =
    (state, event) match {
      case (st: ErrorState, ev: CalculatorEvent) => st
      case (st: InitialState, ev: CalculatorEvent) =>
        try {
          val res: Int = tb.eval(tb.parse(ev.query)).toString.toInt
          TransitionState(res)
        } catch {
          case tbe: ToolBoxError => ErrorState(tbe.message)
          case e: (Exception | Error)  => ErrorState(e.getMessage)
        }
      case (st: TransitionState, ev: CalculatorEvent) =>
        try {
          val res: Int = tb.eval(tb.parse(st.value.toString + ev.query)).toString.toInt
          TransitionState(res)
        } catch {
          case tbe: ToolBoxError => ErrorState(tbe.message)
          case e: (Exception | Error)  => ErrorState(e.getMessage)
        }
      case (_, ResetStateEvent()) => InitialState()
    }

  def replay(events: Seq[EventEnvelope[DomainEvent]]): CalculatorState =
    events.foldLeft[CalculatorState](InitialState()) {
      (state, envelope) => evolve(state, envelope.payload)
    }

  def decide(
              state: CalculatorState,
              command: CalculatorCommand
            ): Either[CalculatorError, List[DomainEvent]] =
    (state, command) match {
      case (_, Recover(_)) =>
        Right(List(ResetStateEvent()))

      case (st: ErrorState, _: Execute) =>
        Left(StateDomainError(s"Can't execute command on ErrorState: ${st.msg}"))

      case (_, Initialize(_)) =>
        Right(List(CalculatorEvent("0")))

      case (_, Execute(_, query)) =>
        Right(List(CalculatorEvent(query)))
    }
}

// ------------------------------------------------------------
// 8. Command Handler
// ------------------------------------------------------------

final class CommandHandler(eventLog: EventLog) {

  def handle(
              command: CalculatorCommand,
              correlationId: UUID
            ): IO[CalculatorError, CalculatorState] =
    for {
      history <- eventLog.byAggregateId(command.aggregateId)
      state = Calculator.replay(history)
      newEvents <- ZIO.fromEither(Calculator.decide(state, command))

      seqRef <- Ref.make(history.size)
      factory = new EventEnvelopeFactory[DomainEvent](seqRef, version = 1, aggregateId = command.aggregateId)

      newState <- ZIO.foldLeft(newEvents)(state) { (currentState, event) =>
        for {
          envelope <- factory.fromPayload(event)
          _        <- eventLog.append(envelope)
        } yield Calculator.evolve(currentState, event)
      }
    } yield newState
}

object CommandHandler {
  val layer: URLayer[EventLog, CommandHandler] =
    ZLayer.fromFunction(new CommandHandler(_))
}

// ------------------------------------------------------------
// 9. Demo & Main App
// ------------------------------------------------------------


val program = for {
  eventLog <- ZIO.service[EventLog]
  handler  <- ZIO.service[CommandHandler]

  calculatorId = 42
  correlationId = UUID.randomUUID()

  commands = List(
    Initialize(calculatorId),
    Execute(calculatorId, "1+2+3"),
    Execute(calculatorId, "-2"),
    Execute(calculatorId, "*7"),
    Execute(calculatorId, "/0"),
    Recover(calculatorId),
    Execute(calculatorId, "1+1"),
    Execute(calculatorId, "a")
  )

  _ <- ZIO.foreachDiscard(commands) { cmd =>
    handler.handle(cmd, correlationId)
  }

  history <- eventLog.byAggregateId(calculatorId)

  replayedState = Calculator.replay(history)
  _ <- Console.printLine(s"Replayed State from Log: $replayedState").orDie

  finalState <- handler.handle(Recover(calculatorId), correlationId)
  _          <- Console.printLine(s"Final State: $finalState").orDie
} yield finalState

val run = program.provide(
  InMemoryEventLog.layer,
  CommandHandler.layer
)


Unsafe.unsafe { implicit unsafe =>
  Runtime.default.unsafe.run(run)
}
