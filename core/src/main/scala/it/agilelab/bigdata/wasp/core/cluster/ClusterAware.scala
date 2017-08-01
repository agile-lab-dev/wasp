package it.agilelab.bigdata.wasp.core.cluster

import akka.actor.{Actor, ActorLogging}
import akka.cluster.{Metric, NodeMetrics, Cluster}
import akka.cluster.ClusterEvent._
import akka.cluster.Member
import akka.cluster.MemberStatus

/**
 * Creates the [[Cluster]] and does any cluster lifecycle handling
 * aside from join and leave so that the implementing applications can
 * customize when this is done.
 *
 * Implemented by [[ClusterAwareNodeGuardian]].
 */
abstract class ClusterAware extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  /** subscribe to cluster changes, re-subscribe when restart. */
  override def preStart(): Unit = cluster.subscribe(self, classOf[ClusterDomainEvent])

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Actor.Receive = {
    case MemberUp(member) =>
      onMemberUp(member)
    case UnreachableMember(member) =>
      onUnreachableMember(member)
    case MemberRemoved(member, previousStatus) =>
      onMemberRemoved(member, previousStatus)
    case ClusterMetricsChanged(forNode) =>
      forNode collectFirst {
        case m if m.address == cluster.selfAddress =>
          log.debug("{}", filter(m.metrics))
      }
    case _: MemberEvent =>
  }

  def filter(nodeMetrics: Set[Metric]): String = {
    val filtered = nodeMetrics collect { case v if v.name != "processors" => s"${v.name}:${v.value}" }
    s"NodeMetrics[${filtered.mkString(",")}]"
  }

  def onMemberUp(member: Member) = log.info("Member {} joined cluster.", member.address)

  def onUnreachableMember(member: Member) = log.info("Member detected as unreachable: {}", member)

  def onMemberRemoved(member: Member, previousStatus: MemberStatus) = log.info("Member is Removed: {} after {}", member.address, previousStatus)

}
