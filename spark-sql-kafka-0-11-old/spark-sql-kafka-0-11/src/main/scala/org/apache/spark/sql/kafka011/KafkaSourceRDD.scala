/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kafka011

import java.{util => ju}

import scala.collection.mutable.ArrayBuffer

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.NextIterator


/** Offset range that one partition of the KafkaSourceRDD has to read */
private[kafka011] case class KafkaSourceRDDOffsetRange(
    topicPartition: TopicPartition,
    fromOffset: Long,
    untilOffset: Long,
    preferredLoc: Option[String]) {
  def topic: String = topicPartition.topic
  def partition: Int = topicPartition.partition
  def size: Long = untilOffset - fromOffset
}


/** Partition of the KafkaSourceRDD */
private[kafka011] case class KafkaSourceRDDPartition(
  index: Int, offsetRange: KafkaSourceRDDOffsetRange) extends Partition


/**
 * An RDD that reads data from Kafka based on offset ranges across multiple partitions.
 * Additionally, it allows preferred locations to be set for each topic + partition, so that
 * the [[KafkaSource]] can ensure the same executor always reads the same topic + partition
 * and cached KafkaConsuemrs (see [[CachedKafkaConsumer]] can be used read data efficiently.
 *
 * @param sc the [[SparkContext]]
 * @param executorKafkaParams Kafka configuration for creating KafkaConsumer on the executors
 * @param offsetRanges Offset ranges that define the Kafka data belonging to this RDD
 */
private[kafka011] class KafkaSourceRDD(
    sc: SparkContext,
    executorKafkaParams: ju.Map[String, Object],
    offsetRanges: Seq[KafkaSourceRDDOffsetRange],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean,
    reuseKafkaConsumer: Boolean)
  extends RDD[ConsumerRecord[Array[Byte], Array[Byte]]](sc, Nil) {

  override def persist(newLevel: StorageLevel): this.type = {
    logError("Kafka ConsumerRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) => new KafkaSourceRDDPartition(i, o) }.toArray
  }

  override def count(): Long = offsetRanges.map(_.size).sum

  override def countApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] = {
    val c = count
    new PartialResult(new BoundedDouble(c, 1.0, c, c), true)
  }

  override def isEmpty(): Boolean = count == 0L

  override def take(num: Int): Array[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val nonEmptyPartitions =
      this.partitions.map(_.asInstanceOf[KafkaSourceRDDPartition]).filter(_.offsetRange.size > 0)

    if (num < 1 || nonEmptyPartitions.isEmpty) {
      return new Array[ConsumerRecord[Array[Byte], Array[Byte]]](0)
    }

    // Determine in advance how many messages need to be taken from each partition
    val parts = nonEmptyPartitions.foldLeft(Map[Int, Int]()) { (result, part) =>
      val remain = num - result.values.sum
      if (remain > 0) {
        val taken = Math.min(remain, part.offsetRange.size)
        result + (part.index -> taken.toInt)
      } else {
        result
      }
    }

    val buf = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]
    val res = context.runJob(
      this,
      (tc: TaskContext, it: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]]) =>
      it.take(parts(tc.partitionId)).toArray, parts.keys.toArray
    )
    res.foreach(buf ++= _)
    buf.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val part = split.asInstanceOf[KafkaSourceRDDPartition]
    part.offsetRange.preferredLoc.map(Seq(_)).getOrElse(Seq.empty)
  }

  override def compute(
      thePart: Partition,
      context: TaskContext): Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val sourcePartition = thePart.asInstanceOf[KafkaSourceRDDPartition]
    val topic = sourcePartition.offsetRange.topic
    val kafkaPartition = sourcePartition.offsetRange.partition
    val consumer =
      if (!reuseKafkaConsumer) {
        // If we can't reuse CachedKafkaConsumers, creating a new CachedKafkaConsumer. As here we
        // uses `assign`, we don't need to worry about the "group.id" conflicts.
        CachedKafkaConsumer.createUncached(topic, kafkaPartition, executorKafkaParams)
      } else {
        CachedKafkaConsumer.getOrCreate(topic, kafkaPartition, executorKafkaParams)
      }
    val range = resolveRange(consumer, sourcePartition.offsetRange)
    assert(
      range.fromOffset <= range.untilOffset,
      s"Beginning offset ${range.fromOffset} is after the ending offset ${range.untilOffset} " +
        s"for topic ${range.topic} partition ${range.partition}. " +
        "You either provided an invalid fromOffset, or the Kafka topic has been damaged")
    if (range.fromOffset == range.untilOffset) {
      logInfo(s"Beginning offset ${range.fromOffset} is the same as ending offset " +
        s"skipping ${range.topic} ${range.partition}")
      Iterator.empty
    } else {
      val underlying = new NextIterator[ConsumerRecord[Array[Byte], Array[Byte]]]() {
        var requestOffset = range.fromOffset

        override def getNext(): ConsumerRecord[Array[Byte], Array[Byte]] = {
          if (requestOffset >= range.untilOffset) {
            // Processed all offsets in this partition.
            finished = true
            null
          } else {
            val r = consumer.get(requestOffset, range.untilOffset, pollTimeoutMs, failOnDataLoss)
            if (r == null) {
              // Losing some data. Skip the rest offsets in this partition.
              finished = true
              null
            } else {
              requestOffset = r.offset + 1
              r
            }
          }
        }

        override protected def close(): Unit = {
          if (!reuseKafkaConsumer) {
            // Don't forget to close non-reuse KafkaConsumers. You may take down your cluster!
            consumer.close()
          } else {
            // Indicate that we're no longer using this consumer
            CachedKafkaConsumer.releaseKafkaConsumer(topic, kafkaPartition, executorKafkaParams)
          }
        }
      }
      // Release consumer, either by removing it or indicating we're no longer using it
      context.addTaskCompletionListener { _ =>
        underlying.closeIfNeeded()
      }
      underlying
    }
  }

  private def resolveRange(consumer: CachedKafkaConsumer, range: KafkaSourceRDDOffsetRange) = {
    if (range.fromOffset < 0 || range.untilOffset < 0) {
      // Late bind the offset range
      val availableOffsetRange = consumer.getAvailableOffsetRange()
      val fromOffset = if (range.fromOffset < 0) {
        assert(range.fromOffset == KafkaOffsetRangeLimit.EARLIEST,
          s"earliest offset ${range.fromOffset} does not equal ${KafkaOffsetRangeLimit.EARLIEST}")
        availableOffsetRange.earliest
      } else {
        range.fromOffset
      }
      val untilOffset = if (range.untilOffset < 0) {
        assert(range.untilOffset == KafkaOffsetRangeLimit.LATEST,
          s"latest offset ${range.untilOffset} does not equal ${KafkaOffsetRangeLimit.LATEST}")
        availableOffsetRange.latest
      } else {
        range.untilOffset
      }
      KafkaSourceRDDOffsetRange(range.topicPartition,
        fromOffset, untilOffset, range.preferredLoc)
    } else {
      range
    }
  }
}
