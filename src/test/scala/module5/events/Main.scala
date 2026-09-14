import OrderStatus.{Paid, Placed}
import zio.*

import java.time.Instant
import java.util.UUID

// ------------------------------------------------------------
// 1. Domain event: факт, который уже произошёл
// ------------------------------------------------------------

sealed trait DomainEvent

final case class OrderPlaced(
                              orderId: String,
                              customerId: String,
                              total: BigDecimal
                            ) extends DomainEvent

final case class OrderPaid(
                          orderId: String,
                          amount: BigDecimal
                          ) extends DomainEvent

// ------------------------------------------------------------
// 2. Commands
//
// Команда = намерение изменить состояние системы
// Команда может быть отклонена.
// ------------------------------------------------------------

sealed trait OrderCommand:
  def orderId: String

final case class PayOrder(
                         orderId: String,
                         amount: BigDecimal
                         ) extends OrderCommand

// ------------------------------------------------------------
// 3. Event envelope
//
// Техническая оболочка вокруг доменного события
// ------------------------------------------------------------

final case class EventEnvelope[E <: DomainEvent](
                                                  eventId: UUID,
                                                  aggregateId: String,
                                                  eventType: String,
                                                  version: Int,
                                                  occurredAt: Instant,
                                                  correlationId: Option[UUID],
                                                  causationId: Option[UUID],
                                                  producer: String,
                                                  payload: E
                                                )

// ------------------------------------------------------------
// 4. Event log
//
// В реальном приложении здесь будет repository / Event Store.
// ------------------------------------------------------------

trait EventLog:

  def append(
            event: EventEnvelope[? <: DomainEvent]
            ): UIO[Unit]

  def byAggregateId(
                   aggregateId: String
                   ): UIO[Vector[EventEnvelope[? <: DomainEvent]]]


final class InMemoryEventLog(
                            ref: Ref[Vector[EventEnvelope[? <: DomainEvent]]]
                            ) extends EventLog:
  override def append(
                       event: EventEnvelope[? <: DomainEvent]
                     ): UIO[Unit] = {
    ref.update(_ :+ event)
  }

  override def byAggregateId(
                            aggregateId: String
                            ): UIO[Vector[EventEnvelope[? <: DomainEvent]]] =
    ref.get.map(
      _.filter(_.aggregateId == aggregateId)
    )


object InMemoryEventLog:
  val layer: ULayer[EventLog] =
    ZLayer.fromZIO(
      Ref
        .make(Vector.empty[EventEnvelope[? <: DomainEvent]])
        .map(new InMemoryEventLog(_))
    )

// ------------------------------------------------------------
// 5. Aggregate state
//
// Это состояние write model.
// Это НЕ CQRS read model.
// ------------------------------------------------------------

enum OrderStatus:
  case Placed
  case Paid

final case class OrderState(
                             orderId: String,
                             customerId: String,
                             total: BigDecimal,
                             status: OrderStatus
                           )

// ------------------------------------------------------------
// 6. Domain errors
//
// Ошибки бизнес-правил.
// ------------------------------------------------------------

sealed trait OrderError

final case class OrderNotFound(
                              orderId: String
                              ) extends OrderError

final case class OrderAlreadyPaid(
                                 orderId: String
                                 ) extends OrderError

final case class InvalidPaymentAmount(
                                     expected: BigDecimal,
                                     actual: BigDecimal
                                     ) extends OrderError

// ------------------------------------------------------------
// 7. Aggregate layer
//
// evolve:
// State + Event -> New State
//
// decide:
// State + Command -> Event(s)
// ------------------------------------------------------------

object Order:

  def evolve(
              state: Option[OrderState],
              event: DomainEvent
            ): Option[OrderState] =
    event match
      case e: OrderPlaced =>
        Some(
          OrderState(
            orderId = e.orderId,
            customerId = e.customerId,
            total = e.total,
            status = Placed
          )
        )

      case _: OrderPaid =>
        state.map(
          _.copy(
            status = Paid
          )
        )

  def replay(
              events: Seq[EventEnvelope[? <: DomainEvent]]
            ): Option[OrderState] =
    events.foldLeft(Option.empty[OrderState]) {
      case (state, envelope) =>
        evolve(state, envelope.payload)
    }

  def decide(
            state: Option[OrderState],
            command: OrderCommand
            ): Either[OrderError, List[DomainEvent]] =
    command match
      case cmd: PayOrder =>

        state match {
          case None =>
            Left(
              OrderNotFound(cmd.orderId)
            )
          case Some(order)
            if order.status == Paid =>
            Left(
              OrderAlreadyPaid(order.orderId)
            )
          case Some(order)
            if order.total != cmd.amount =>
            Left(
              InvalidPaymentAmount(
                expected = order.total,
                actual = cmd.amount
              )
            )
          case Some(order) =>
            Right(
              List(
                OrderPaid(
                  orderId = order.orderId,
                  amount = cmd.amount
                )
              )
            )
        }

// ------------------------------------------------------------
// 8. Command Handler
//
// orchestration:
//
// 1. получить команду
// 2. загрузить историю aggregate
// 3. replay state
// 4. вызвать domain decide
// 5. сохранить новые события
// ------------------------------------------------------------

final class OrderCommandHandler(
                               eventLog: EventLog
                               ):
  def handle(
            command: OrderCommand,
            correlationId: UUID
            ): IO[OrderError, List[DomainEvent]] =
    for {

      history <- eventLog.byAggregateId(command.orderId)

      state = Order.replay(history)

      newEvents <-
        ZIO.fromEither(
          Order.decide(
            state, command
          )
        )

      _ <- ZIO.foreachDiscard(newEvents) { event =>

        val envelope = {
          EventEnvelope(
            eventId = UUID.randomUUID(),
            aggregateId = command.orderId,
            eventType = event.getClass.getSimpleName,
            version = 1,
            occurredAt = Instant.now(),
            correlationId = Some(correlationId),
            causationId = None,
            producer = "order-service",
            payload = event
          )
        }
        eventLog.append(envelope)
      }

    } yield newEvents

object OrderCommandHandler:

  val layer: URLayer[EventLog, OrderCommandHandler] =
    ZLayer.fromFunction(
      new OrderCommandHandler(_)
    )

// ------------------------------------------------------------
// 9. Demo
//
// Здесь мы сначала добавляем события OrderPlaced.
// Затем отправляем команду PayOrder.
// ------------------------------------------------------------

object CommandSideApp extends ZIOAppDefault:

  val program = {
    for {
      eventLog <- ZIO.service[EventLog]

      handler <- ZIO.service[OrderCommandHandler]

      correlationId = UUID.randomUUID()

      initialEvent = OrderPlaced(
        orderId = "order-123",
        customerId = "customer-42",
        total = BigDecimal("3250.00")
      )

      initialEnvelope = {
        EventEnvelope(
          eventId = UUID.randomUUID(),
          aggregateId = "order-123",
          eventType = "OrderPlaced",
          version = 1,
          occurredAt = Instant.now(),
          correlationId = Some(correlationId),
          causationId = None,
          producer = "order-service",
          payload = initialEvent
        )
      }

      _ <- eventLog.append(initialEnvelope)

      command = PayOrder(
        orderId = "order-123",
        amount = BigDecimal("3250.00")
      )

      _ <- Console.printLine(command)

      result <- handler.handle(command, correlationId).either

      _ <- Console.printLine(result)

      history <- eventLog.byAggregateId("order-123")

      _ <- ZIO.foreachDiscard(history.zipWithIndex) { case (event, offset) =>
        Console.printLine(
          s"offset=$offset, " +
            s"type=${event.eventType}, " +
            s"payload=${event.payload}"
        )
      }

      state = Order.replay(history)

      _ <- Console.printLine(state)

    } yield ()
  }

  override def run =
    program.provide(
      InMemoryEventLog.layer,
      OrderCommandHandler.layer
    )