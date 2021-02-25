// AUTHOR: JONAS VANDER VENNET
// adapted from: ShardingContainerPoolBalancer.scala

package org.apache.openwhisk.core.loadBalancer

import akka.actor.ActorRef
import akka.actor.ActorRefFactory

import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.management.scaladsl.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import pureconfig._
import pureconfig.generic.auto._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size.SizeLong
import org.apache.openwhisk.common.LoggingMarkers._
import org.apache.openwhisk.core.loadBalancer.InvokerState.{Healthy, Offline, Unhealthy, Unresponsive}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.spi.SpiLoader

import scala.concurrent.Future
import java.util.concurrent.ThreadLocalRandom

class ThesisNetworkBalancer(
  config: WhiskConfig,
  controllerInstance: ControllerInstanceId,
  feedFactory: FeedFactory,
  val invokerPoolFactory: InvokerPoolFactory,
  implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  /** Build a cluster of all loadbalancers */
  private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
    AkkaManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
    Some(Cluster(actorSystem))
  } else if (loadConfigOrThrow[Seq[String]]("akka.cluster.seed-nodes").nonEmpty) {
    Some(Cluster(actorSystem))
  } else {
    None
  }

  // CHANGE: All invokers are assumed managed, and are references as STATE.invokers
  override protected def emitMetrics() = {
    super.emitMetrics()
    // MetricEmitter.emitGaugeMetric(
    //   INVOKER_TOTALMEM_BLACKBOX,
    //   schedulingState.blackboxInvokers.foldLeft(0L) { (total, curr) =>
    //     if (curr.status.isUsable) {
    //       curr.id.userMemory.toMB + total
    //     } else {
    //       total
    //     }
    //   })
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_MANAGED,
      schedulingState.invokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_MANAGED, schedulingState.invokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_MANAGED,
      schedulingState.invokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_MANAGED,
      schedulingState.invokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_MANAGED, schedulingState.invokers.count(_.status == Offline))
    // MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Healthy))
    // MetricEmitter.emitGaugeMetric(
    //   UNHEALTHY_INVOKER_BLACKBOX,
    //   schedulingState.blackboxInvokers.count(_.status == Unhealthy))
    // MetricEmitter.emitGaugeMetric(
    //   UNRESPONSIVE_INVOKER_BLACKBOX,
    //   schedulingState.blackboxInvokers.count(_.status == Unresponsive))
    // MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Offline))
  }

  /** State needed for scheduling. */
  val schedulingState = ThesisNetworkBalancerState()(lbConfig)

  /**
   * Monitors invoker supervision and the cluster to update the state sequentially
   *
   * All state updates should go through this actor to guarantee that
   * [[ThesisNetworkBalancerState.updateInvokers]] and [[ThesisNetworkBalancerState.updateCluster]]
   * are called exclusive of each other and not concurrently.
   */
  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    // all members of the cluster that are available
    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        schedulingState.updateInvokers(newState)

      // State of the cluster as it is right now
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        schedulingState.updateCluster(availableMembers.size)

      // General lifecycle events and events concerning the reachability of members. Split-brain is not a huge concern
      // in this case as only the invoker-threshold is adjusted according to the perceived cluster-size.
      // Taking the unreachable member out of the cluster from that point-of-view results in a better experience
      // even under split-brain-conditions, as that (in the worst-case) results in premature overloading of invokers vs.
      // going into overflow mode prematurely.
      case event: ClusterDomainEvent =>
        availableMembers = event match {
          case MemberUp(member)          => availableMembers + member
          case ReachableMember(member)   => availableMembers + member
          case MemberRemoved(member, _)  => availableMembers - member
          case UnreachableMember(member) => availableMembers - member
          case _                         => availableMembers
        }

        schedulingState.updateCluster(availableMembers.size)
    }
  }))

  /** Loadbalancer interface methods */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)
  override def clusterSize: Int = schedulingState.clusterSize

  /** 1. Publish a message to the loadbalancer */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    // CHANGE: ALL INVOKERS ARE MANAGED NOW
    val invokersToUse = schedulingState.invokers
    val chosen = if (invokersToUse.nonEmpty) {
      logging.info(
          this, // ${action.namespace.toFullyQualifiedEntityName} || 
          s"[THESIS] deciding on invoker for ${action.fullyQualifiedName(false)}"
        )
      logging.info(
          this,
          s"[THESIS] parentTransId: ${msg.ptransid}"
        )
      logging.info(
          this,
          s"[THESIS] transId: ${msg.transid}"
        )
      logging.info(
          this,
          s"[THESIS] action: ${msg.action}"
        )
      logging.info(
          this,
          s"[THESIS] activationId: ${msg.activationId}"
        )
      logging.info(
          this,
          s"[THESIS] content: ${msg.content}"
        )
      logging.info(
          this,
          s"[THESIS] cause: ${msg.cause}"
        )
      val invoker: Option[(InvokerInstanceId, Boolean)] = ThesisNetworkBalancer.schedule(
        msg,
        schedulingState,
        action.limits.concurrency.maxConcurrent,
        action.fullyQualifiedName(false),
        action.name,
        invokersToUse,
        schedulingState.invokerSlots,
        action.limits.memory.megabytes)

      invoker.map(_._1)
    } else {
      None
    }

    // try to debug "error": "Failed to resolve action with name 'guest/increment' during composition."
    logging.info(this, s"[OWN] chosen: ${chosen}")

    chosen
      .map { invoker =>
        // MemoryLimit() and TimeLimit() return singletons - they should be fast enough to be used here
        val memoryLimit = action.limits.memory
        val memoryLimitInfo = if (memoryLimit == MemoryLimit()) { "std" } else { "non-std" }
        val timeLimit = action.limits.timeout
        val timeLimitInfo = if (timeLimit == TimeLimit()) { "std" } else { "non-std" }
        
        logging.info(
          this,
          s"[OWN] scheduled activation ${msg.activationId}, action '${msg.action.asString}', ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB (${memoryLimitInfo}), time limit ${timeLimit.duration.toMillis} ms (${timeLimitInfo}) to ${invoker}")
        val activationResult = setupActivation(msg, action, invoker)
        sendActivationToInvoker(messageProducer, msg, invoker).map(_ => activationResult)
      }
      .getOrElse {
        // report the state of all invokers
        val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
          val count = agg.getOrElse(curr.status, 0) + 1
          agg + (curr.status -> count)
        }

        logging.error(
          this,
          s"[OWN] failed to schedule activation ${msg.activationId}, action '${msg.action.asString}', ns '${msg.user.namespace.name.asString}' - invokers to use: $invokerStates")
        Future.failed(LoadBalancerException("No invokers available"))
      }
  }

  override val invokerPool =
    invokerPoolFactory.createInvokerPool(
      actorSystem,
      messagingProvider,
      messageProducer,
      sendActivationToInvoker,
      Some(monitor))

  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry) = {
    schedulingState.invokerSlots
      .lift(invoker.toInt)
      .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.memoryLimit.toMB.toInt))
  }
}

object ThesisNetworkBalancer extends LoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = {

    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(
        actorRefFactory: ActorRefFactory,
        messagingProvider: MessagingProvider,
        messagingProducer: MessageProducer,
        sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
        monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messagingProducer, m, i),
            messagingProvider.getConsumer(whiskConfig, s"health${instance.asString}", "health", maxPeek = 128),
            monitor))
      }

    }
    new ThesisNetworkBalancer(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      invokerPoolFactory)
  }

  def requiredProperties: Map[String, String] = kafkaHosts

  /**
   * Scans through all invokers and searches for an invoker tries to get a free slot on an invoker. If no slot can be
   * obtained, randomly picks a healthy invoker.
   *
   * @param maxConcurrent concurrency limit supported by this action
   * @param invokers a list of available invokers to search in, including their state
   * @param dispatched semaphores for each invoker to give the slots away from
   * @param slots Number of slots, that need to be acquired (e.g. memory in MB)
   * @return an invoker to schedule to or None of no invoker is available
   */
  def schedule(
    msg: ActivationMessage,
    schedulingState: ThesisNetworkBalancerState,
    maxConcurrent: Int,
    fqn: FullyQualifiedEntityName,
    fqnName: EntityName,
    invokers: IndexedSeq[InvokerHealth],
    dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
    slots: Int)(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean)] = {

    val healthyInvokers = invokers.filter(_.status.isUsable)
    if (!healthyInvokers.nonEmpty) {
      return None
    }

    var parentTransid = msg.ptransid
    val currentTransid = msg.transid
    val outputSize = 3.14  // TODO: from action annotations, derive a formula to combine input dimensions to calculate output dimensions

    if (parentTransid == TransactionId.unknown) {
      // there should be a cause in the (recent) history of invocations
      msg.cause match {
        case Some(cause) => {
          parentTransid = schedulingState.causeToTransidHistory(cause) // TODO: watch out for multi-influx pattern!
          if (schedulingState.knownComposition(cause)) {schedulingState.addChildToComposition(currentTransid, parentTransid)}
          else {schedulingState.addComposition(currentTransid, cause)}
        }
        case None => {
          logging.warn(this, s"[THESIS] Cause field is empty, but it was not expected to be..")
        }
      }
      
    }
    var wasForceAcquisition = true
    val chosenInvokerId = if (parentTransid != TransactionId.unknown) {
      // there is a parent transaction id
      // options: - direct descendant of another action
      //          - first scheduled action of this composition (or not a composition at all..)
      if (schedulingState.knownTransid(parentTransid)) {
        // descendant of previous method (includes splits!)
        // GOAL: try to acquire same invoker as parent (TODO: maybe check for outputSize > 0? or is this always the case?)
        val parentInvokerId = schedulingState.transidToInvokerMap(parentTransid)
        val parentInvoker = invokers(parentInvokerId.toInt)
        if (parentInvoker.status.isUsable && dispatched(parentInvokerId.toInt).tryAcquireConcurrent(fqn, maxConcurrent, slots)) {
          logging.info(this, s"[THESIS] tryAcquire successful..")
          wasForceAcquisition = false
          parentInvokerId
        } else {
          // Cannot schedule to parent invoker
          // GOAL: choose an empty invoker (as empty as possible) to house potential descendants as well
          // TODO: how to know invoker load?
          // for now: random healthy invoker
          logging.info(this, s"[THESIS] line 329: force acquire..")
          val randomInvokerId = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
          dispatched(randomInvokerId.toInt).forceAcquireConcurrent(fqn, maxConcurrent, slots)
          randomInvokerId
        }
      } else {
        // first scheduled action of this composition (or not a composition at all.. [would this be flagged by an empty 'cause' field?])
        // GOAL: choose an empty invoker (as empty as possible) to house potential descendants as well
        // TODO: how to know invoker load?
        // for now: random healthy invoker
          logging.info(this, s"[THESIS] line 339: force acquire..")
        val randomInvokerId = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
        dispatched(randomInvokerId.toInt).forceAcquireConcurrent(fqn, maxConcurrent, slots)
        randomInvokerId
      }
    } else {
      logging.warn(this, s"[THESIS] Have to fall back due to lack of scheduling information: Scheduled randomly!")
      val randomInvokerId = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
      dispatched(randomInvokerId.toInt).forceAcquireConcurrent(fqn, maxConcurrent, slots)
      randomInvokerId
    }
    logging.info(
      this,
      s"[THESIS] Chosen invoker with ID ${chosenInvokerId.toInt} as target for execution"
    )

    // Register transid with invoker choice
    schedulingState.registerInvokerAcquisition(currentTransid, chosenInvokerId)
    // Register start of output transfer
    schedulingState.registerPendingOutputTransfer(currentTransid, outputSize, chosenInvokerId)
    // Resolve potential incoming data transfer to finish
    schedulingState.resolveIncomingDataTransfer(parentTransid, chosenInvokerId)
    // Register cause-transid
    msg.cause.foreach { c =>
      schedulingState.updateCauseHistory(c, currentTransid)
    }
    if (fqnName.asString.contains("__") && fqnName.asString.split("__")(1) == "stop") {
      // stop registering for this composition,
      // log the internal bookkeeping structures
      val transfers = schedulingState.finishComposition(schedulingState.getCompositionIdentifier(currentTransid))
      logging.info(
        this,
        s"[THESIS] Transfers: ${transfers}"
      )
    }

    Some(chosenInvokerId, wasForceAcquisition)
  }
}

/**
 * Holds the state necessary for scheduling of actions.
 *
 * @param _invokers all of the known invokers in the system
 * @param _invokerSlots state of accessible slots of each invoker
 */
case class ThesisNetworkBalancerState(
  private var _causeToTransidHistory: Map[ActivationId, TransactionId] = Map(),
  private var _transidToInvokerMap: Map[TransactionId, InvokerInstanceId] = Map(),
  private var _pendingOutputTransfer: Map[TransactionId, (Number, InvokerInstanceId)] = Map(),
  // TODO: can add more metadata to transferlist if needed, potentially make the content of this List a case class on its own
  private var _outputTransferHistoryPerComposition: Map[ActivationId, List[(Number, InvokerInstanceId, InvokerInstanceId)]] = Map(), 
  private var _transidToCompositionIdentifier: Map[TransactionId, ActivationId] = Map(),

  private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  protected[loadBalancer] var _invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] =
    IndexedSeq.empty[NestedSemaphore[FullyQualifiedEntityName]],
  private var _clusterSize: Int = 1)(
  lbConfig: CommonBalancerConfig =
    loadConfigOrThrow[CommonBalancerConfig](ConfigKeys.loadbalancer))(implicit logging: Logging) {


  /** Getters for the variables, setting from the outside is only allowed through the update methods below */
  def causeToTransidHistory: Map[ActivationId, TransactionId] = _causeToTransidHistory
  def transidToInvokerMap: Map[TransactionId, InvokerInstanceId] = _transidToInvokerMap

  // invoker id is the home invoker from which the transfer is pending
  // transaction id shows the origin of the output transfer
  def pendingOutputTransfer: Map[TransactionId, (Number, InvokerInstanceId)] = _pendingOutputTransfer
  // size, origin, destination
  def outputTransferHistory: Map[ActivationId, List[(Number, InvokerInstanceId, InvokerInstanceId)]] = _outputTransferHistoryPerComposition

  def invokers: IndexedSeq[InvokerHealth] = _invokers
  def invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] = _invokerSlots
  def clusterSize: Int = _clusterSize

  /**
   * JONAS THESIS CHANGE
   * @param cause encoutered cause activation id to update
   * @param transid transid that encountered the cause, and is now the parent of future entries in this causal chain
   * @return calculated invoker slot
   */
  def updateCauseHistory(cause: ActivationId, transid: TransactionId) = {
    _causeToTransidHistory = _causeToTransidHistory + (cause -> transid)
  }
  def registerInvokerAcquisition(currentTransid: TransactionId, chosenInvokerId: InvokerInstanceId) = {
    _transidToInvokerMap = _transidToInvokerMap + (currentTransid -> chosenInvokerId)
  }
  def knownTransid(transid: TransactionId): Boolean = {
    _transidToInvokerMap.contains(transid)
  }
  def registerPendingOutputTransfer(transid: TransactionId, outputSize: Number, originInvoker: InvokerInstanceId) = {
    _pendingOutputTransfer =  _pendingOutputTransfer + (transid -> (outputSize, originInvoker))
  }
  def resolveIncomingDataTransfer(parentTransid: TransactionId, destinationInvoker: InvokerInstanceId) = {
    _pendingOutputTransfer.get(parentTransid) match {
      case Some ((transferSize, originInvoker)) => _outputTransferHistoryPerComposition = _outputTransferHistoryPerComposition + (_transidToCompositionIdentifier(parentTransid) -> List((transferSize, originInvoker, destinationInvoker)))
      case None => {}
    }
  }
  def getCompositionIdentifier(transid: TransactionId): ActivationId = {
    _transidToCompositionIdentifier(transid)
  }
  def knownComposition(rootCause: ActivationId): Boolean = {
    _transidToCompositionIdentifier.exists(x => x._2 == rootCause)
  }
  def addComposition(transid: TransactionId, rootCause: ActivationId) = {
    _transidToCompositionIdentifier = _transidToCompositionIdentifier + (transid -> rootCause)
  }
  def addChildToComposition(childTransid: TransactionId, parentTransid: TransactionId) = {
    _transidToCompositionIdentifier = _transidToCompositionIdentifier + (childTransid -> _transidToCompositionIdentifier(parentTransid))
  }
  def finishComposition(rootCause: ActivationId): List[(Number, InvokerInstanceId, InvokerInstanceId)] =  {
    // TODO: clean up everything that has information about members of this composition!
    val transfers = _outputTransferHistoryPerComposition(rootCause)
    _outputTransferHistoryPerComposition = _outputTransferHistoryPerComposition - rootCause

    var newCompositionIdentifierMap: Map[TransactionId, ActivationId] = Map()
    _transidToCompositionIdentifier foreach (x => if (x._2 != rootCause) {newCompositionIdentifierMap = newCompositionIdentifierMap + (x._1 -> x._2)})
    _transidToCompositionIdentifier = newCompositionIdentifierMap
    transfers
  }


  /**
   * @param memory
   * @return calculated invoker slot
   */
  private def getInvokerSlot(memory: ByteSize): ByteSize = {
    val invokerShardMemorySize = memory / _clusterSize
    val newTreshold = if (invokerShardMemorySize < MemoryLimit.MIN_MEMORY) {
      logging.error(
        this,
        s"registered controllers: calculated controller's invoker shard memory size falls below the min memory of one action. "
          + s"Setting to min memory. Expect invoker overloads. Cluster size ${_clusterSize}, invoker user memory size ${memory.toMB.MB}, "
          + s"min action memory size ${MemoryLimit.MIN_MEMORY.toMB.MB}, calculated shard size ${invokerShardMemorySize.toMB.MB}.")(
        TransactionId.loadbalancer)
      MemoryLimit.MIN_MEMORY
    } else {
      invokerShardMemorySize
    }
    newTreshold
  }

  /**
   * Updates the scheduling state with the new invokers.
   *
   * This is okay to not happen atomically since dirty reads of the values set are not dangerous. It is important though
   * to update the "invokers" variables last, since they will determine the range of invokers to choose from.
   *
   * Handling a shrinking invokers list is not necessary, because InvokerPool won't shrink its own list but rather
   * report the invoker as "Offline".
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateCluster]]
   */
  def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = {
    val oldSize = _invokers.size
    val newSize = newInvokers.size

    _invokers = newInvokers

    val logDetail = if (oldSize != newSize) {
      if (oldSize < newSize) {
        // Keeps the existing state..
        val onlyNewInvokers = _invokers.drop(_invokerSlots.length)
        _invokerSlots = _invokerSlots ++ onlyNewInvokers.map { invoker =>
          new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
        }
        val newInvokerDetails = onlyNewInvokers
          .map(i =>
            s"${i.id.toString}: ${i.status} / ${getInvokerSlot(i.id.userMemory).toMB.MB} of ${i.id.userMemory.toMB.MB}")
          .mkString(", ")
        s"number of known invokers increased: new = $newSize, old = $oldSize. details: $newInvokerDetails."
      } else {
        s"number of known invokers decreased: new = $newSize, old = $oldSize."
      }
    } else {
      s"no update required - number of known invokers unchanged: $newSize."
    }

    logging.info(
      this,
      s"loadbalancer invoker status updated. $logDetail")(
      TransactionId.loadbalancer)
  }

  /**
   * Updates the size of a cluster. Throws away all state for simplicity.
   *
   * This is okay to not happen atomically, since a dirty read of the values set are not dangerous. At worst the
   * scheduler works on outdated invoker-load data which is acceptable.
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateInvokers]]
   */
  def updateCluster(newSize: Int): Unit = {
    val actualSize = newSize max 1 // if a cluster size < 1 is reported, falls back to a size of 1 (alone)
    if (_clusterSize != actualSize) {
      val oldSize = _clusterSize
      _clusterSize = actualSize
      _invokerSlots = _invokers.map { invoker =>
        new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
      }
      // Directly after startup, no invokers have registered yet. This needs to be handled gracefully.
      val invokerCount = _invokers.size
      val totalInvokerMemory =
        _invokers.foldLeft(0L)((total, invoker) => total + getInvokerSlot(invoker.id.userMemory).toMB).MB
      val averageInvokerMemory =
        if (totalInvokerMemory.toMB > 0 && invokerCount > 0) {
          (totalInvokerMemory / invokerCount).toMB.MB
        } else {
          0.MB
        }
      logging.info(
        this,
        s"loadbalancer cluster size changed from $oldSize to $actualSize active nodes. ${invokerCount} invokers with ${averageInvokerMemory} average memory size - total invoker memory ${totalInvokerMemory}.")(
        TransactionId.loadbalancer)
    }
  }
}
