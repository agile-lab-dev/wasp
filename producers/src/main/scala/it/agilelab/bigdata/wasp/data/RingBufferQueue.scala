package it.agilelab.bigdata.wasp.data

import scala.collection.immutable.Queue

/**
  * @author Eugenio Liso
  */
case class RingBufferQueue[A](capacity: Int, size: Int, queue: Queue[A]) extends RingBuffer[A] {

  override def push(elem: A): RingBufferQueue[A] = {
    if (size < capacity) RingBufferQueue[A](capacity, size + 1, queue.enqueue(elem))
    else queue.dequeue match {
      case (_, queueWithoutOldestItem) => RingBufferQueue[A](capacity, size, queueWithoutOldestItem.enqueue(elem))
    }
  }

  override def sumElements(implicit num: Numeric[A]): A = queue.sum

  override def isFull: Boolean = capacity == size

  override def minOfElements(implicit num: Numeric[A]): A = queue.min

  override def maxOfElements(implicit num: Numeric[A]): A = queue.max

}

object RingBufferQueue {
  def empty[A](capacity: Int): RingBufferQueue[A] = RingBufferQueue(capacity, 0, Queue.empty[A])

  def apply[A](capacity: Int)(xs: A*): RingBufferQueue[A] = {
    val elems = if (xs.size <= capacity) xs else xs.takeRight(capacity.toInt)
    RingBufferQueue(capacity, elems.size, Queue(elems: _*))
  }
}
