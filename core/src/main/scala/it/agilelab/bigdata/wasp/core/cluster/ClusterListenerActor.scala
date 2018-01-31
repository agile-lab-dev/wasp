package it.agilelab.bigdata.wasp.core.cluster

import akka.actor.Actor
import akka.cluster.ClusterEvent._
import akka.cluster._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.DownUnreachableMembers
import it.agilelab.bigdata.wasp.core.utils.ConfigManager

import scala.concurrent.duration._

/**
  * Cluster lifecycle handling
  * - log events
  * - set "down" the unreachable members
  *    Custom logic: the key is if network partition happens, only cluster nodes which have majority will take down UnreachableMember after downingTimeout.
  */

object ClusterListenerActor {
  val name = "CLusterListenerAdminActor"
  val downingTimeout = ConfigManager.getWaspConfig.actorDowningTimeout millisecond
}

class ClusterListenerActor extends Actor with Logging {

  val cluster = Cluster(context.system)
  var unreachableMembers: Set[Member] = Set.empty

  /** Subscribe to cluster changes, re-subscribe when restart.
      N.B.  ClusterDomainEvent includes all events (e.g.  MemberEvent, ReachabilityEvent)
            Implementation using InitialStateAsSnapshot and matching CurrentClusterState already tested but not suitable!
  */
  override def preStart(): Unit = cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[ClusterDomainEvent])

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Actor.Receive = {

    /* MemberEvent */
    case MemberUp(member) => logger.info(s"Member ${member.address} joined cluster")
    case MemberLeft(member) => logger.info(s"Member ${member.address} lefted cluster")
    case MemberRemoved(member, previousStatus) => logger.info(s"Member is removed: ${member.address} - previous status: $previousStatus")

    /*  ReachabilityEvent */
    case UnreachableMember(member) => onUnreachableMember(member)
    case ReachableMember(member) => onReachableMember(member)

    /* other ClusterDomainEvent */
    case ClusterMetricsChanged(nodeMetrics) => onClusterMetricsChanged(nodeMetrics)
    case _: ClusterDomainEvent =>  // ignore

    case DownUnreachableMembers =>
      unreachableMembers.foreach {
        member => {
          logger.info(s"Member ${member.address} downing")
          cluster.down(member.address)
        }
      }
      unreachableMembers.empty
  }

  private def onUnreachableMember(member: Member) = {

    def isMajority(total: Int, unreachable: Int): Boolean = {

      // find out majority number of the group
      def majority: Int = (total+1)/2 + (total+1)%2

      require(total > 0)
      require(unreachable >= 0)
      (total - unreachable) >= majority
    }

    def scheduleTakeDown = {
      implicit val dispatcher = context.system.dispatcher
      unreachableMembers = unreachableMembers + member
      context.system.scheduler.scheduleOnce(ClusterListenerActor.downingTimeout, self, DownUnreachableMembers)
    }

    logger.info(s"Member detected as unreachable: ${member}")
    val state = cluster.state
    if (isMajority(state.members.size, state.unreachable.size))
      scheduleTakeDown
  }

  private def onReachableMember(member: Member) = {
    logger.info(s"Member detected as reachable: ${member}")
    unreachableMembers = unreachableMembers - member
  }

  private def onClusterMetricsChanged(nodeMetrics: Set[NodeMetrics]) {

    def filter(nodeMetrics: Set[Metric]): String = {
      val filtered = nodeMetrics collect { case v if v.name != "processors" => s"${v.name}:${v.value}" }
      s"NodeMetrics[${filtered.mkString(",")}]"
    }

    nodeMetrics collectFirst {
      case m if m.address == cluster.selfAddress => logger.debug (s"${filter (m.metrics)}")
    }
  }
}