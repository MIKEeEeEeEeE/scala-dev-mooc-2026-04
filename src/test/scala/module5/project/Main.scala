package module5.project

import zio.*
import zio.stm.*
import java.time.Instant

// ------------------------------------------------------------
// PERSISTENCE ID
// ------------------------------------------------------------
final case class BankPersistenceId(entityId: Long, entityTypeHint: String)

// ------------------------------------------------------------
// ERROR
// ------------------------------------------------------------
sealed trait ActorError
sealed trait BankAccountError                                 extends ActorError
case object BankIllegalAccountState                           extends BankAccountError
final case class BankIllegalAccountOperation(err: ActorError) extends BankAccountError

// ------------------------------------------------------------
// MONEY & CURRENCY
// ------------------------------------------------------------
enum BankCurrency(val symbol: String):
  case Dollar extends BankCurrency("$")
  case Euro   extends BankCurrency("€")
  case Ruble  extends BankCurrency("₽")

sealed trait BankMoneyAmountError extends ActorError
case object BankNegativeValue     extends BankMoneyAmountError
case object BankCurrencyMismatch  extends BankMoneyAmountError

final case class BankMoneyAmount private (currency: BankCurrency, value: BigDecimal):

  def +(other: BankMoneyAmount): Either[BankMoneyAmountError, BankMoneyAmount] =
    if currency != other.currency then Left(BankCurrencyMismatch)
    else BankMoneyAmount(currency, value + other.value)

  def -(other: BankMoneyAmount): Either[BankMoneyAmountError, BankMoneyAmount] =
    if currency != other.currency then Left(BankCurrencyMismatch)
    else BankMoneyAmount(currency, value - other.value)

object BankMoneyAmount:

  def apply(currency: BankCurrency, value: BigDecimal): Either[BankMoneyAmountError, BankMoneyAmount] =
    if value < 0.0 then Left(BankNegativeValue)
    else Right(new BankMoneyAmount(currency, value))

  def zero(currency: BankCurrency): BankMoneyAmount = new BankMoneyAmount(currency, 0.0)

// ------------------------------------------------------------
// STATE
// ------------------------------------------------------------
sealed trait ActorState
sealed trait BankAccountState                         extends ActorState
case object BankUninitialized                         extends BankAccountState
final case class BankActive(balance: BankMoneyAmount) extends BankAccountState
final case class BankFrozen(balance: BankMoneyAmount) extends BankAccountState

// ------------------------------------------------------------
// EVENT
// ------------------------------------------------------------
sealed trait ActorEvent
sealed trait BankAccountEvent                                extends ActorEvent
final case class BankAccountOpened(currency: BankCurrency)   extends BankAccountEvent
final case class BankFundsDeposited(amount: BankMoneyAmount) extends BankAccountEvent
final case class BankFundsWithdrawn(amount: BankMoneyAmount) extends BankAccountEvent
case object BankAccountFrozen                                extends BankAccountEvent

// ------------------------------------------------------------
// COMMAND
// ------------------------------------------------------------
sealed trait ActorCommand[Response]:
  def replyTo: BankTellable[Response]

type BankAccountResponse = BankStatusReply[BankAccountState]
sealed trait BankAccountCommand extends ActorCommand[BankAccountResponse]

final case class BankStartAccount(currency: BankCurrency, replyTo: BankTellable[BankAccountResponse])
    extends BankAccountCommand

final case class BankStopAccount(replyTo: BankTellable[BankAccountResponse])       extends BankAccountCommand
final case class BankGetAccountBalance(replyTo: BankTellable[BankAccountResponse]) extends BankAccountCommand

final case class BankDeposit(amount: BankMoneyAmount, replyTo: BankTellable[BankAccountResponse])
    extends BankAccountCommand

final case class BankWithdraw(amount: BankMoneyAmount, replyTo: BankTellable[BankAccountResponse])
    extends BankAccountCommand

final case class BankReplayAt(version: Long, replyTo: BankTellable[BankAccountResponse]) extends BankAccountCommand

// ------------------------------------------------------------
// RESPONSE
// ------------------------------------------------------------
sealed trait BankStatusReply[+T]

object BankStatusReply:
  final case class Success[T](value: T)  extends BankStatusReply[T]
  final case class Fail(message: String) extends BankStatusReply[Nothing]
  def success[T](value: T): BankStatusReply[T]        = Success(value)
  def fail(message: String): BankStatusReply[Nothing] = Fail(message)

// ------------------------------------------------------------
// BEHAVIOR
// ------------------------------------------------------------
trait BankBehavior[Command]:
  def receive(command: Command): UIO[BankBehavior[Command]]

object BankBehavior {

  def receiveMessage[Command](handler: Command => UIO[BankBehavior[Command]]): BankBehavior[Command] =
    (msg: Command) => handler(msg)

  def stopped[Command]: BankBehavior[Command] =
    (msg: Command) => ZIO.succeed(stopped[Command])

}

// ------------------------------------------------------------
// ACTOR
// ------------------------------------------------------------
trait Actor[Command, ZEnvironment]:
  val mailbox: TRef[Seq[Command]]
  val behavior: BankBehavior[Command]
  val context: BankTypedActorContext[ZEnvironment]

// ------------------------------------------------------------
trait BankTypedActorContext[ZEnvironment]:
  def environment: ZEnvironment
  def log(msg: String): UIO[Unit]

// ------------------------------------------------------------
trait BankTellable[Protocol]:
  def tell(msg: Protocol): UIO[Unit]

trait ActorRef[Protocol] extends BankTellable[Protocol]:
  def ask[Response](makeMsg: BankTellable[Response] => Protocol): Task[Response]

// ------------------------------------------------------------
final class ActorRefImpl[Protocol](mailbox: Queue[Protocol], fiber: Fiber.Runtime[Nothing, Unit])
    extends ActorRef[Protocol]:
  def tell(msg: Protocol): UIO[Unit] = mailbox.offer(msg).unit

  def ask[Response](makeMsg: BankTellable[Response] => Protocol): Task[Response] =
    for
      promise <- Promise.make[Nothing, Response]
      replyRef = new BankTellable[Response] {
        def tell(response: Response): UIO[Unit] = promise.succeed(response).unit
      }
      msg = makeMsg(replyRef)
      _        <- mailbox.offer(msg)
      response <- promise.await
    yield response

  def stop: UIO[Unit] = fiber.interrupt.unit

// ------------------------------------------------------------
object ActorRuntime:

  private def readMail[Command](mailbox: Queue[Command], behaviorRef: Ref[BankBehavior[Command]]): UIO[Unit] =
    (for
      msg             <- mailbox.take
      currentBehavior <- behaviorRef.get
      nextBehavior    <- currentBehavior.receive(msg)
      _               <- behaviorRef.set(nextBehavior)
    yield ()).forever

  def spawn[Protocol](initial: BankBehavior[Protocol]): UIO[ActorRef[Protocol]] =
    for
      mailbox  <- Queue.unbounded[Protocol]
      behavior <- Ref.make(initial)
      fiber    <- readMail(mailbox, behavior).forkDaemon
    yield new ActorRefImpl[Protocol](mailbox, fiber)

// ------------------------------------------------------------
// EFFECT
// ------------------------------------------------------------
sealed trait BankEffect[+Event <: ActorEvent, State <: ActorState]

object BankEffect:

  final case class Persist[+Event <: ActorEvent, State <: ActorState](event: Event, reply: State => UIO[Unit])
      extends BankEffect[Event, State]

  final case class Reply[+Event <: ActorEvent, State <: ActorState](action: UIO[Unit])
      extends BankEffect[Event, State]

// ------------------------------------------------------------
// EVENT SOURCED BEHAVIOR
// ------------------------------------------------------------
final class BankEventSourcedBehavior(
  val aggregateId: BankPersistenceId,
  val eventLog: BankEventLog[BankAccountEvent],
  val eventEnvelopeFactory: BankEventEnvelopeFactory[BankAccountEvent],
  val emptyState: BankAccountState,
  val commandHandler: (BankAccountState, BankAccountCommand) => BankEffect[BankAccountEvent, BankAccountState],
  val eventHandler: (BankAccountState, BankAccountEvent) => BankAccountState,
  val currentState: BankAccountState
) extends BankBehavior[BankAccountCommand]:

  override def receive(command: BankAccountCommand): UIO[BankBehavior[BankAccountCommand]] = {
    command match
      case BankReplayAt(pred, replyTo) =>
        for
          history <- eventLog.byAggregateId(aggregateId).commit
          checkpoint = BankAccount.replayAt(history, pred)
          _ <- replyTo.tell(BankStatusReply.success(checkpoint))
        yield this

      case _ =>
        commandHandler(currentState, command) match {
          case BankEffect.Persist(event, reply) =>
            for
              envelope <- eventEnvelopeFactory.fromPayload(event)
              _        <- eventLog.append(envelope).commit
              state = eventHandler(currentState, event)
              _ <- reply(state)
            yield new BankEventSourcedBehavior(
              aggregateId,
              eventLog,
              eventEnvelopeFactory,
              emptyState,
              commandHandler,
              eventHandler,
              state
            )
          case BankEffect.Reply(action) => action.as(this)
        }
  }

// ------------------------------------------------------------
// EVENT LOG
// ------------------------------------------------------------
sealed trait BankEventLogError

trait BankEventLog[E <: ActorEvent]:
  def append(eventEnvelope: BankEventEnvelope[E]): USTM[Unit]

  def byAggregateId(aggregateId: BankPersistenceId): USTM[Chunk[BankEventEnvelope[E]]]

// ------------------------------------------------------------
// EVENT ENVELOPE
// ------------------------------------------------------------
final case class BankEventEnvelope[+E <: ActorEvent](
  eventId: Long,
  versionId: Long,
  aggregateId: BankPersistenceId,
  occurredAt: Instant,
  payload: E
)

class BankEventEnvelopeFactory[E <: ActorEvent](
  eventIdRef: Ref[Long],
  versionIdRef: Ref[Long],
  aggregateId: BankPersistenceId,
  history: BankEventLog[E]
) {

  def fromPayload(payload: E): UIO[BankEventEnvelope[E]] =
    for
      eventId   <- eventIdRef.updateAndGet(_ + 1)
      versionId <- versionIdRef.updateAndGet(_ + 1)
    yield BankEventEnvelope(
      eventId = eventId,
      versionId = versionId,
      aggregateId = aggregateId,
      occurredAt = Instant.now(),
      payload = payload
    )

}

object BankEventEnvelopeFactory:

  def make[E <: ActorEvent](
    aggregateId: BankPersistenceId,
    history: BankEventLog[E],
    globalEventIdRef: Ref[Long]
  ): UIO[BankEventEnvelopeFactory[E]] = {
    for
      pastEvents <- history.byAggregateId(aggregateId).commit
      lastVersion = pastEvents.lastOption.map(_.versionId).getOrElse(0L)
      versionIdRef <- Ref.make(lastVersion)
    yield new BankEventEnvelopeFactory[E](globalEventIdRef, versionIdRef, aggregateId, history)
  }

final class BankInMemoryEventLog[E <: ActorEvent](ref: TRef[Chunk[BankEventEnvelope[E]]])
    extends BankEventLog[E]:

  override def append(eventEnvelope: BankEventEnvelope[E]): USTM[Unit] =
    ref.update(_ :+ eventEnvelope)

  override def byAggregateId(aggregateId: BankPersistenceId): USTM[Chunk[BankEventEnvelope[E]]] =
    ref.get.map(_.filter(_.aggregateId == aggregateId))

object BankInMemoryEventLog:

  val layer: ULayer[BankEventLog[BankAccountEvent]] =
    ZLayer.fromZIO(
      TRef
        .make(Chunk.empty[BankEventEnvelope[BankAccountEvent]])
        .commit
        .map(new BankInMemoryEventLog(_))
    )

// ------------------------------------------------------------
// ACCOUNT
// ------------------------------------------------------------
object BankAccount:

  def apply(
    id: Long,
    eventLog: BankEventLog[BankAccountEvent],
    globalEventIdRef: Ref[Long]
  ): UIO[BankBehavior[BankAccountCommand]] =
    val persistenceId = BankPersistenceId(id, "bank-account")
    for
      history <- eventLog.byAggregateId(persistenceId).commit
      restoredState = replay(history)
      envelopeFactory <- BankEventEnvelopeFactory.make(persistenceId, eventLog, globalEventIdRef)
    yield new BankEventSourcedBehavior(
      aggregateId = persistenceId,
      eventLog = eventLog,
      eventEnvelopeFactory = envelopeFactory,
      emptyState = BankUninitialized,
      commandHandler = commandHandler,
      eventHandler = evolve,
      currentState = restoredState
    )

  def evolve(state: BankAccountState, event: BankAccountEvent): BankAccountState =
    (state, event) match
      case (BankUninitialized, BankAccountOpened(currency)) => BankActive(BankMoneyAmount.zero(currency))
      case (BankFrozen(balance), _)                         => BankFrozen(balance)
      case (BankActive(balance), BankFundsDeposited(amount)) =>
        balance + amount match
          case Right(newBalance) => BankActive(newBalance)
          case Left(_)           => state
      case (BankActive(balance), BankFundsWithdrawn(amount)) =>
        balance - amount match
          case Right(newBalance) => BankActive(newBalance)
          case Left(_)           => state
      case (BankActive(balance), BankAccountOpened(_)) => BankActive(balance)
      case (BankActive(balance), BankAccountFrozen)    => BankFrozen(balance)
      case (BankUninitialized, BankFundsWithdrawn(_))  => BankUninitialized
      case (BankUninitialized, BankAccountFrozen)      => BankUninitialized
      case (BankUninitialized, BankFundsDeposited(_))  => BankUninitialized

  def replay(events: Chunk[BankEventEnvelope[BankAccountEvent]]): BankAccountState =
    events.foldLeft[BankAccountState](BankUninitialized) {
      (state, envelope) =>
        evolve(state, envelope.payload)
    }

  def replayAt(events: Chunk[BankEventEnvelope[BankAccountEvent]], version: Long): BankAccountState =
    replay(events.takeWhile(_.versionId <= version))

  def decide(
    state: BankAccountState,
    command: BankAccountCommand
  ): Either[BankAccountError, BankAccountEvent] =
    (state, command) match
      case (BankUninitialized, BankStartAccount(currency, _)) =>
        Right(BankAccountOpened(currency))
      case (BankActive(balance), BankDeposit(amount, _)) =>
        balance + amount match
          case Right(_)  => Right(BankFundsDeposited(amount))
          case Left(err) => Left(BankIllegalAccountOperation(err))
      case (BankActive(balance), BankWithdraw(amount, _)) =>
        balance - amount match
          case Right(_)  => Right(BankFundsWithdrawn(amount))
          case Left(err) => Left(BankIllegalAccountOperation(err))
      case (BankActive(_), BankStopAccount(_)) => Right(BankAccountFrozen)
      case (_, _)                              => Left(BankIllegalAccountState)

  def commandHandler(
    state: BankAccountState,
    command: BankAccountCommand
  ): BankEffect[BankAccountEvent, BankAccountState] =
    command match
      case BankGetAccountBalance(replyTo) => BankEffect.Reply(replyTo.tell(BankStatusReply.success(state)))
      case _ =>
        decide(state, command) match
          case Right(event) =>
            BankEffect.Persist(event, newState => command.replyTo.tell(BankStatusReply.success(newState)))
          case Left(error) =>
            BankEffect.Reply(command.replyTo.tell(BankStatusReply.fail(error.toString)))

// ------------------------------------------------------------
// TRANSFER COORDINATOR
// ------------------------------------------------------------
object BankTransferCoordinator:

  def transfer(
    fromActorRef: ActorRef[BankAccountCommand],
    toActorRef: ActorRef[BankAccountCommand],
    amount: BankMoneyAmount
  ): Task[BankAccountResponse] =
    for
      reply1 <- fromActorRef.ask(r => BankWithdraw(amount, r))
      reply <- reply1 match
        case ok @ BankStatusReply.Success(_) =>
          for
            reply2 <- toActorRef.ask(r => BankDeposit(amount, r))
          yield reply2 match
            case ok2 @ BankStatusReply.Success(_) => ok2
            case fail @ BankStatusReply.Fail(_)   => fail
        case fail @ BankStatusReply.Fail(_) => ZIO.succeed(fail)
    yield reply

// ------------------------------------------------------------
// BILLING APP
// ------------------------------------------------------------
object BankBillingApp extends ZIOAppDefault:

  val program: ZIO[BankEventLog[BankAccountEvent], Object, Unit] =
    for
      globalEventIdRef <- Ref.make(0L)
      eventLog         <- ZIO.service[BankEventLog[BankAccountEvent]]

      acc1    <- BankAccount(1L, eventLog, globalEventIdRef)
      acc1Ref <- ActorRuntime.spawn(acc1)
      acc2    <- BankAccount(2L, eventLog, globalEventIdRef)
      acc2Ref <- ActorRuntime.spawn(acc2)

      d10 <- ZIO.fromEither(BankMoneyAmount(BankCurrency.Dollar, 10L))
      d5  <- ZIO.fromEither(BankMoneyAmount(BankCurrency.Dollar, 5L))
      d1  <- ZIO.fromEither(BankMoneyAmount(BankCurrency.Dollar, 1L))

      start1 <- acc1Ref.ask(r => BankStartAccount(BankCurrency.Dollar, r))
      _      <- Console.printLine(s"Start acc1: $start1")

      start2 <- acc2Ref.ask(r => BankStartAccount(BankCurrency.Dollar, r))
      _      <- Console.printLine(s"Start acc2: $start2")

      deposit <- acc1Ref.ask(r => BankDeposit(d10, r))
      _       <- Console.printLine(s"Deposit acc1: $deposit")

      withdraw <- acc1Ref.ask(r => BankWithdraw(d5, r))
      _        <- Console.printLine(s"Withdraw acc1: $withdraw")

      transfer1 <- BankTransferCoordinator.transfer(acc1Ref, acc2Ref, d10)
      _         <- Console.printLine(s"Transfer result 1: $transfer1")

      transfer2 <- BankTransferCoordinator.transfer(acc1Ref, acc2Ref, d5)
      _         <- Console.printLine(s"Transfer result 2: $transfer2")

      transfer3 <- BankTransferCoordinator.transfer(acc1Ref, acc2Ref, d5)
      _         <- Console.printLine(s"Transfer result 3: $transfer3")

      replayed <- acc1Ref.ask(r => BankReplayAt(1L, r))
      _        <- Console.printLine(s"Replayed at v4: $replayed")

      results <- ZIO.foreachPar(1 to 10) { i =>
        acc1Ref.ask(r => BankWithdraw(d10, r))
      }

      balance1 <- acc1Ref.ask(r => BankGetAccountBalance(r))
      _        <- Console.printLine(s"Balance acc1: $balance1")

      balance2 <- acc2Ref.ask(r => BankGetAccountBalance(r))
      _        <- Console.printLine(s"Balance acc2: $balance2")

      historyAcc1 <- eventLog.byAggregateId(BankPersistenceId(1L, "bank-account")).commit
      _ <- ZIO.foreach(historyAcc1) { envelope =>
        Console.printLine(s"[v${envelope.versionId}] ${envelope.payload} @ ${envelope.occurredAt}")
      }

      historyAcc2 <- eventLog.byAggregateId(BankPersistenceId(2L, "bank-account")).commit
      _ <- ZIO.foreach(historyAcc2) { envelope =>
        Console.printLine(s"[v${envelope.versionId}] ${envelope.payload} @ ${envelope.occurredAt}")
      }
    yield ()

  def run: ZIO[ZIOAppArgs & Scope, Any, Any] = program.provide(BankInMemoryEventLog.layer)
