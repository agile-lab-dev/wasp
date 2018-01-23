package it.agilelab.bigdata.wasp.core.cluster

import akka.actor.Actor
import akka.cluster.ClusterEvent._
import akka.cluster._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.messages.DownUnreachableMembers

import scala.concurrent.duration._

/**
 * Cluster lifecycle handling
 * - log events
  * - set "down" the unreachable members
  *    Custom logic: the key is if network partition happens, only cluster nodes which have majority will take down UnreachableMember after downingTimeout.
 */

object ClusterListenerActor {
  val name = "CLusterListenerAdminActor"
  val downingTimeout = 10 seconds
}

class ClusterListenerActor extends Actor with Logging {

  val cluster = Cluster(context.system)
  var unreachableMembers: Set[Member] = Set()

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[UnreachableMember], classOf[ReachableMember])

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Actor.Receive = {

    case MemberUp(member) => onMemberUp(member)
    case UnreachableMember(member) => onUnreachableMember(member)
    case ReachableMember(member) => onReachableMember(member)
    case MemberRemoved(member, previousStatus) => onMemberRemoved(member, previousStatus)
    case ClusterMetricsChanged(forNode) => onClusterMetricsChanged(forNode)
    case _: MemberEvent =>  // ignore

    case DownUnreachableMembers =>
      unreachableMembers.foreach {
        member => {
          logger.info(s"Member ${member.address} downing.")
          cluster.down(member.address)
        }
      }
  }

  private def onMemberUp(member: Member) =
    logger.info(s"Member ${member.address} joined cluster.")

  private def onUnreachableMember(member: Member) = {
    logger.info(s"Member detected as unreachable: ${member}")
    val state = cluster.state
    if (isMajority(state.members.size, state.unreachable.size))
      scheduleTakeDown(member)
  }

  private def onReachableMember(member: Member) = {
    logger.info(s"Member detected as unreachable: ${member}")
    unreachableMembers = unreachableMembers - member
  }

  private def onMemberRemoved(member: Member, previousStatus: MemberStatus) =
    logger.info(s"Member is Removed: ${member.address} after $previousStatus")

  private def onClusterMetricsChanged(forNode: Set[NodeMetrics]) {
    forNode collectFirst {
      case m if m.address == cluster.selfAddress => logger.debug (s"${filter (m.metrics)}")
    }
  }

  private def filter(nodeMetrics: Set[Metric]): String = {
    val filtered = nodeMetrics collect { case v if v.name != "processors" => s"${v.name}:${v.value}" }
    s"NodeMetrics[${filtered.mkString(",")}]"
  }

  // find out majority number of the group
  private def majority(n: Int): Int = (n+1)/2 + (n+1)%2

  private def isMajority(total: Int, dead: Int): Boolean = {
    require(total > 0)
    require(dead >= 0)
    (total - dead) >= majority(total)
  }

  private def scheduleTakeDown(member: Member) = {
    implicit val dispatcher = context.system.dispatcher
    unreachableMembers = unreachableMembers + member
    context.system.scheduler.scheduleOnce(ClusterListenerActor.downingTimeout, self, DownUnreachableMembers)
  }
}