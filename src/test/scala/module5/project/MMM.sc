import zio.*
import zio.stm.*
import java.time.Instant

// ------------------------------------------------------------
// ACTOR SYSTEM
// ------------------------------------------------------------

sealed trait DomainError

sealed trait CommandError


// ------------------------------------------------------------
// ACTOR SYSTEM
// ------------------------------------------------------------



// ------------------------------------------------------------
// RESPONSE
// ------------------------------------------------------------

sealed trait Response
final case class ActionResponse(message: String) extends Response
final case class BalanceCheckResponse(amount: Int) extends Response

sealed trait ResponseError

// ------------------------------------------------------------
// ACTOR
// ------------------------------------------------------------

trait Actor[Command, ZEnvironment]:
  val state: ZState[Command]
  val mailbox: TRef[Seq[Command]]
  val behavior: Behavior[Command]
  val context: TypedActorContext[ZEnvironment]


trait Behavior[Command]:
  def receive(command: Command): UIO[Behavior[Command]]

object Behavior {
  def receiveMessage[Command](handler: Command => UIO[Behavior[Command]]): Behavior[Command] =
    (msg: Command) => handler(msg)

  def stopped[Command]: Behavior[Command] =
    (msg: Command) => ZIO.succeed(stopped[Command])
}

trait TypedActorContext[ZEnvironment]:
  def environment: ZEnvironment
  def log(msg: String): UIO[Unit]

trait ActorRef[Protocol]:
  def tell(msg: Protocol): UIO[Unit]
  def ask[Response](makeMsg: ActorRef[Response] => Protocol): Task[Response]

final class ActorRefImpl[Protocol](mailbox: Queue[Protocol]) extends ActorRef[Protocol]:
  def tell(msg: Protocol): UIO[Unit] = mailbox.offer(msg).unit
  def tell(msgs: Seq[Protocol]) = mailbox.offerAll(msgs)


sealed trait PersistenceId(val entityId: String, val entityTypeHint: String)


final class EventSourcedBehavior[Command, Event, State, PersistenceId](
                                                                        val persistenceId: PersistenceId,
                                                                        val emptyState: State,
                                                                        val commandHandler: (State, Command) => Effect[Event, State],
                                                                        val eventHandler: (State, Event) => State
                                                                     ) extends Behavior[Command]:
  override def receive(command: Command): UIO[Behavior[Command]] = ???


sealed trait Effect[+Event, State]

object Effect:
  final case class Persist[Event, State](event: Event) extends Effect[Event, State]
  final case class PersistAll[Event, State](events: List[Event]) extends Effect[Event, State]
  final case class None[Event, State]() extends Effect[Event, State]
  final case class Reply[Event, State](response: Response) extends Effect[Event, State]
  final case class Unhandled[Event, State]() extends Effect[Event, State]
  def persist[Event, State](event: Event): Effect[Event, State] = Persist(event)
  def reply[Event, State](response: Response): Effect[Event, State] = Reply(response)
  def unhandled[Event, State]: Effect[Event, State] = Unhandled()






// ------------------------------------------------------------
// EVENT
// ------------------------------------------------------------

sealed trait DomainEvent
final case class EventEnvelope[E <: DomainEvent](
                                                  eventId: Long,
                                                  aggregateId: String,
                                                  eventType: String,
                                                  version: Int,
                                                  occurredAt: Instant,
                                                  correlationId: Option[Long],
                                                  causationId: Option[Long],
                                                  producer: String,
                                                  payload: E
                                                )

sealed trait AccountEvent extends DomainEvent
case object AccountOpened extends AccountEvent
final case class FundsDeposited(amount: MoneyAmount) extends AccountEvent
final case class FundsWithdrawn(amount: MoneyAmount) extends AccountEvent
case object AccountClosed extends AccountEvent


// ------------------------------------------------------------
// COMMAND
// ------------------------------------------------------------

sealed trait StatusReply[+A <: Response]

object StatusReply:
  case object Pending extends StatusReply
  case object Cancelled extends StatusReply
  final case class Success[A](value: A) extends StatusReply[A]
  final case class Error(message: String) extends StatusReply[ResponseError]
  def success[A](value: A): StatusReply[A] = Success(value)
  def error(message: String): StatusReply[ResponseError] = Error(message)


sealed trait Command

sealed trait AccountCommand
final case class OpenAccountCommand(
                                     replyTo: ActorRef[StatusReply[Response]]
                                   ) extends AccountCommand
final case class CloseAccount(
                               replyTo: ActorRef[StatusReply[Response]]
                             ) extends AccountCommand
final case class BalanceCheckCommand(
                                      replyTo: ActorRef[StatusReply[Response]]
                                    ) extends AccountCommand
final case class WithdrawCommand(
                                  amount: MoneyAmount,
                                  replyTo: ActorRef[StatusReply[Response]]
                                ) extends AccountCommand
final case class DepositCommand(
                                 amount: MoneyAmount,
                                 replyTo: ActorRef[StatusReply[Response]]
                               ) extends AccountCommand

// ------------------------------------------------------------
// USER
// ------------------------------------------------------------

case class UserId(value: Long)

// ------------------------------------------------------------
// ACCOUNT
// ------------------------------------------------------------

object BankAccount:

  final case class State(amount: MoneyAmount)

  def apply(id: AccountId, currency: Currency): Behavior[Command] =
    EventSourcedBehavior[AccountCommand, AccountEvent, AccountState, String](
      persistenceId = PersistenceId(id.toString, "bank-account"),
      emptyState = Active(MoneyAmount.zero(currency)),
      commandHandler = BankAccountCommandHandler,
      eventHandler = eventHandler
    )




final class BankAccountCommandHandler(

                                     )

final class BankAccountEventHandler(
                                   
                                   )

final case class AggregateRefs(
                                history: TRef[Seq[EventEnvelope[_ <: DomainEvent]]],
                                state: TRef[AccountState],
                                tmap: TMap[Long, AggregateRefs]
                              )



case class AccountId(value: Long)

final case class Account(id: AccountId, owner: UserId, balance: MoneyAmount)

sealed trait AccountState
case object Uninitialized extends AccountState
final case class Active(account: Account) extends AccountState
final case class Frozen(account: Account) extends AccountState

sealed trait AccountStateError extends DomainError
case object IsAlreadyOpenError extends AccountStateError
case object IsFrozenError extends AccountStateError

sealed trait AccountOperation
final case class InitAccount(ownerId: Long, currency: Currency) extends AccountOperation
case object FreezeAccount extends AccountOperation

// ------------------------------------------------------------
// OPERATION
// ------------------------------------------------------------

case class OperationEventId(value: Long)

sealed trait OperationEvent extends DomainEvent
case class Deposited(operationId: Long, amount: MoneyAmount)


case object InsufficientFunds extends DomainError
sealed trait ConcurrencyError extends DomainError
final case class VersionConflict(expectedVersion: Long, currentVersion: Long) extends ConcurrencyError

// ------------------------------------------------------------
// EVENT LOG
// ------------------------------------------------------------

sealed trait EventLogError

trait EventLog:
  def append(aggregateId: Long, expectedVersion: Long, event: EventEnvelope[DomainEvent]): STM[EventLogError, Unit]
  def byAggregateId(aggregateId: Long): STM[EventLogError, Seq[EventEnvelope[DomainEvent]]]


final class InMemoryEventLog(events: TMap[Long, Seq[EventEnvelope[DomainEvent]]]) extends EventLog:

  override def append(
                       aggregateId: Long,
                       expectedVersion: Long,
                       event: EventEnvelope[DomainEvent]
                     ): DomainSTM[Unit] =
    for
      history <- events.get(aggregateId).map(_.getOrElse(Seq.empty))
      currentVersion =
        history.lastOption.map(_.version).getOrElse(0L)
      _ <-
        if currentVersion == expectedVersion then
          events.put(aggregateId, history :+ event)
        else
          STM.fail(
            VersionConflict(expectedVersion, currentVersion)
          )
    yield ()

  override def byAggregateId(
                              aggregateId: Long
                            ): DomainSTM[Seq[EventEnvelope[DomainEvent]]] =
    events.get(aggregateId).map(_.getOrElse(Seq.empty))

object InMemoryEventLog:

  val layer: ULayer[EventLog] =
    ZLayer.fromZIO(
      TMap
        .empty[Long, Seq[EventEnvelope[DomainEvent]]]
        .commit
        .map(new InMemoryEventLog(_))
    )

object Account:

  def evolve(
              state: AccountState,
              event: DomainEvent
            ): AccountState =
    (state, event) match

      case (
        Uninitialized,
        AccountOpened(accountId, ownerId, currency)
      ) =>
        Active(
          Account(
            id = accountId,
            owner = ownerId,
            balance = MoneyAmount.zero(currency)
          )
        )

      case (
        Active(account),
        FundsDeposited(amount)
      ) =>
        account.balance + amount match
          case Right(newBalance) =>
            Active(account.copy(balance = newBalance))

          case Left(_) =>
            state

      case (
        Active(account),
        FundsWithdrawn(amount)
      ) =>
        account.balance - amount match
          case Right(newBalance) =>
            Active(account.copy(balance = newBalance))

          case Left(_) =>
            state

      case (
        Active(account),
        AccountClosed
      ) =>
        Closed(account)

      case (currentState, event) =>
        throw new IllegalStateException(
          s"Cannot apply $event to $currentState"
        )

  def replay(
              events: Seq[EventEnvelope[DomainEvent]]
            ): AccountState =
    events.foldLeft[AccountState](Uninitialized) {
      (state, envelope) =>
        evolve(state, envelope.payload)
    }

  def replayFromSnapshot(
                          snapshot: AccountState,
                          events: Seq[EventEnvelope[DomainEvent]]
                        ): AccountState =
    events.foldLeft(snapshot) {
      (state, envelope) =>
        evolve(state, envelope.payload)
    }

  def decide(
              state: AccountState,
              operation: Operation
            ): Either[DomainError, List[DomainEvent]] =
    (state, operation) match

      case (
        Uninitialized,
        StartAccount(id, ownerId, currency)
      ) =>
        Right(
          List(
            AccountOpened(
              accountId = AccountId(id),
              ownerId = UserId(ownerId),
              currency = currency
            )
          )
        )

      case (_, StartAccount(_, _, _)) =>
        Left(AlreadyStarted)

      case (
        Active(_),
        Deposit(_, amount)
      ) =>
        Right(List(FundsDeposited(amount)))

      case (
        Active(account),
        Withdraw(_, amount)
      ) =>
        account.balance - amount match
          case Right(_) =>
            Right(List(FundsWithdrawn(amount)))

          case Left(_) =>
            Left(InsufficientFunds)

      case (
        Active(_),
        StopAccount(_)
      ) =>
        Right(List(AccountClosed))

      case (Closed(_), _) =>
        Left(NotActive)

final class OperationHandler(
                              eventLog: EventLog,
                              eventIdRef: TRef[Long]
                            ):

  def handle(
              operation: Operation
            ): IO[DomainError, AccountState] =

    (
      for
        history <- eventLog.byAggregateId(
          operation.aggregateId
        )

        state = Account.replay(history)

        events <-
          STM
            .fromEither(Account.decide(state, operation))

        finalState <-
          events.foldLeft[
            STM[
              DomainError,
              (Seq[EventEnvelope[DomainEvent]], AccountState)
            ]
          ](
            STM.succeed((history, state))
          ):

            (acc, domainEvent) =>
              acc.flatMap { case (currentHistory, currentState) =>
                for
                  eventId <- eventIdRef.updateAndGet(_ + 1)

                  currentVersion =
                    currentHistory.lastOption
                      .map(_.version)
                      .getOrElse(0L)

                  newVersion = currentVersion + 1

                  envelope =
                    EventEnvelope(
                      eventId = eventId,
                      version = newVersion,
                      aggregateId = operation.aggregateId,
                      occurredAt = Instant.now(),
                      payload = domainEvent
                    )

                  _ <- eventLog.append(
                    aggregateId = operation.aggregateId,
                    expectedVersion = currentVersion,
                    event = envelope
                  )

                  newState =
                    Account.evolve(
                      currentState,
                      domainEvent
                    )

                yield (
                  currentHistory :+ envelope,
                  newState
                )
              }

      yield finalState._2
      ).commit

object OperationHandler:

  val layer: URLayer[EventLog, OperationHandler] =
    ZLayer.fromZIO:
      for
        eventLog <- ZIO.service[EventLog]
        eventIdRef <- TRef.makeCommit(0L)
      yield OperationHandler(
        eventLog,
        eventIdRef
      )


// ------------------------------------------------------------
// MONEY & CURRENCY
// ------------------------------------------------------------

enum Currency(val symbol: String):
  case Dollar extends Currency("$")
  case Euro   extends Currency("€")
  case Ruble  extends Currency("₽")

sealed trait MoneyAmountError
case object NegativeValue extends MoneyAmountError
case object CurrencyMismatch extends MoneyAmountError

final case class MoneyAmount private (currency: Currency, value: BigDecimal):
  def +(other: MoneyAmount): Either[MoneyAmountError, MoneyAmount] =
    if currency != other.currency then Left(CurrencyMismatch)
    else MoneyAmount(currency, value + other.value)
  def -(other: MoneyAmount): Either[MoneyAmountError, MoneyAmount] =
    if currency != other.currency then Left(CurrencyMismatch)
    else MoneyAmount(currency, value - other.value)

object MoneyAmount:
  def apply(currency: Currency, value: BigDecimal): Either[MoneyAmountError, MoneyAmount] =
    if value < 0.0 then Left(NegativeValue)
    else Right(new MoneyAmount(currency, value))
  def zero(currency: Currency): MoneyAmount = new MoneyAmount(currency, 0.0)


object BillingApp extends ZIOAppDefault:

  val program: ZIO[OperationHandler, DomainError, Unit] =
    for
      handler <- ZIO.service[OperationHandler]

      accountId = 42L
      ownerId = 100L
      currency = Currency.Dollar

      s1 <- handler.handle(
        StartAccount(
          aggregateId = accountId,
          ownerId = ownerId,
          currency = currency
        )
      )

      _ <- Console.printLine(
        s"After start: $s1"
      )

      deposit <- ZIO.fromEither(
        MoneyAmount(currency, 100.0)
      )

      s2 <- handler.handle(
        Deposit(accountId, deposit)
      )

      _ <- Console.printLine(
        s"After deposit: $s2"
      )

      withdraw <- ZIO.fromEither(
        MoneyAmount(currency, 40.0)
      )

      s3 <- handler.handle(
        Withdraw(accountId, withdraw)
      )

      _ <- Console.printLine(
        s"After withdraw: $s3"
      )

    yield ()

  def run =
    program.provide(
      InMemoryEventLog.layer,
      OperationHandler.layer
    )