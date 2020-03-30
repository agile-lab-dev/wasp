package it.agilelab.bigdata.wasp.data

/**
  * Represents a Ring Buffer that allows to store elements and to compute a function that sums them.
  * @author Eugenio Liso
  */
trait RingBuffer[A] {
  /**
    * Push an element into the array buffer
    * @param elem the element to be added in the ring buffer
    * @return A new, immutable, RingBuffer with the new element added
    */
  def push(elem: A): RingBuffer[A]

  /**
    * Compute the sum of the elements.
    * @param num implicit parameter
    * @return The sum of the elements
    */
  def sumElements(implicit num: Numeric[A]): A

  /**
    * Returns the minimum among the elements in this buffer.
    * @param num implicit parameter
    * @return The minimimum element in the buffer
    */
  def minOfElements(implicit num: Numeric[A]): A

  /**
    * Returns the maximum among the elements in this buffer.
    * @param num implicit parameter
    * @return The maximum element in the buffer
    */
  def maxOfElements(implicit num: Numeric[A]): A

  /**
    * Checks if the ring buffer is full or not
    * @return true if it is full, false otherwise
    */
  def isFull: Boolean
}
