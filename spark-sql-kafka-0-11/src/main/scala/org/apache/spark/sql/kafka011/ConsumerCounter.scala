package org.apache.spark.sql.kafka011

import java.util.concurrent.ConcurrentHashMap
import org.slf4j.LoggerFactory


/**
  * This class is useful in order to investigate infamous Spark-Kafka bugs pre 2.4
  * https://github.com/apache/spark/pull/21983/files
  * https://issues.apache.org/jira/browse/SPARK-23636
  */
object ConsumerCounter {

  private lazy val logger = LoggerFactory.getLogger(getClass)


  private val concurrentMap = new ConcurrentHashMap[Wrapper, java.lang.Long]()

  def increment(consumer: CachedKafkaConsumer): Unit = {
    if(logger.isDebugEnabled){
      concurrentMap.put(new Wrapper(consumer), Thread.currentThread().getId)
      logger.debug(s"INCREMENT " + concurrentMap.size())
    }
  }

  def decrement(consumer: CachedKafkaConsumer): Unit = {
    if(logger.isDebugEnabled){
      assert(concurrentMap.remove(new Wrapper(consumer)) != null)
      logger.debug(s"DECREMENT " +  concurrentMap.size())
    }
  }
}

class Wrapper(val cons: CachedKafkaConsumer){
  override def hashCode(): Int = cons.uuid.hashCode()

  override def equals(o: Any): Boolean =
    o match {
      case null => false
      case x : Wrapper => x.cons.uuid == cons.uuid
      case _ => false
    }

}