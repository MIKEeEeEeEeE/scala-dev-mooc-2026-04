package module5.events

import java.time.Instant
import java.util.UUID

// Общая метаинформация для любого события.
final case class EventMetadata(
  eventId: UUID,
  eventType: String,
  version: Int,
  occurredAt: Instant,
  aggregateId: String,
  correlationId: Option[UUID] = None,
  causationId: Option[UUID] = None,
  producer: String
)

// Базовый контракт доменного события.
sealed trait DomainEvent:
  def metadata: EventMetadata

// Версия 1 события OrderPlaced.
final case class OrderPlacedV1(
  metadata: EventMetadata,
  customerId: String,
  total: BigDecimal
) extends DomainEvent

// Версия 2: добавили optional-поле currency,
// не ломая чтение старых событий.
final case class OrderPlacedV2(
  metadata: EventMetadata,
  customerId: String,
  total: BigDecimal,
  currency: Option[String]
) extends DomainEvent

// Простой append-only event log в памяти.
// Для лекционного примера важна именно семантика:
// события только добавляются, существующие записи не изменяются.
final class InMemoryEventLog:
  private var events: Vector[DomainEvent] = Vector.empty

  def append(event: DomainEvent): Unit =
    events = events :+ event

  def all: Vector[DomainEvent] =
    events

  def byAggregateId(aggregateId: String): Vector[DomainEvent] =
    events.filter(_.metadata.aggregateId == aggregateId)

// Проекция: восстанавливаем текущее состояние заказа
// из последовательности событий.
final case class OrderView(
  orderId: String,
  customerId: String,
  total: BigDecimal,
  currency: String
)

object OrderProjection:
  def replay(events: Seq[DomainEvent]): Option[OrderView] =
    events.foldLeft(Option.empty[OrderView]) {
      case (_, event: OrderPlacedV1) =>
        Some(
          OrderView(
            orderId = event.metadata.aggregateId,
            customerId = event.customerId,
            total = event.total,
            currency = "RUB" // значение по умолчанию для старой версии
          )
        )

      case (_, event: OrderPlacedV2) =>
        Some(
          OrderView(
            orderId = event.metadata.aggregateId,
            customerId = event.customerId,
            total = event.total,
            currency = event.currency.getOrElse("RUB")
          )
        )
    }

@main def runEventExample(): Unit =
  val eventLog = new InMemoryEventLog

  val correlationId = UUID.randomUUID()

  val event1 =
    OrderPlacedV1(
      metadata = EventMetadata(
        eventId = UUID.randomUUID(),
        eventType = "OrderPlaced",
        version = 1,
        occurredAt = Instant.now(),
        aggregateId = "order-123",
        correlationId = Some(correlationId),
        producer = "order-service"
      ),
      customerId = "customer-42",
      total = BigDecimal("3250.00")
    )

  val event2 =
    OrderPlacedV2(
      metadata = EventMetadata(
        eventId = UUID.randomUUID(),
        eventType = "OrderPlaced",
        version = 2,
        occurredAt = Instant.now(),
        aggregateId = "order-456",
        correlationId = Some(correlationId),
        producer = "order-service"
      ),
      customerId = "customer-99",
      total = BigDecimal("1490.00"),
      currency = Some("RUB")
    )

  eventLog.append(event1)
  eventLog.append(event2)

  println("=== Event log ===")
  eventLog.all.zipWithIndex.foreach { case (event, offset) =>
    println(s"offset=$offset, event=$event")
  }

  println()
  println("=== Replay order-123 ===")

  val order123Events =
    eventLog.byAggregateId("order-123")

  val order123 =
    OrderProjection.replay(order123Events)

  println(order123)
