package com.github.chico.kafka

/**
  * Created by chico on 01/06/2017.
  */
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.spark.sql.SparkSession

object GetOffset {

  case class OffsetInfo(topic: String, partitions: Int, offset: Long, ts: Long) // partitions for MySQL

  def main(args: Array[String]) {
    val topics = args(0)
    val brokerList = args(1)
    val clientId = "GetOffsetShell"
    val maxWaitMs = 1000
    val time = -1
    val nOffsets = 1

    val jdbcUsername = "appstar"
    val jdbcPassword = "SuperN0va"
    val jdbcHostname = "moveitapp.ck2itt5beb7u.eu-west-1.rds.amazonaws.com"
    val jdbcDatabase = "moveit"
    val myTable = "offsets"
    val jdbcPort = 3306

    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    val spark = SparkSession.builder().appName("Get Kafka Offsets").getOrCreate()
    import spark.implicits._

    val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)
    val topicsMetadata = ClientUtils.fetchTopicMetadata(topics.split(",").toSet, metadataTargetBrokers, clientId, maxWaitMs)
      .topicsMetadata
    val partitions = topicsMetadata.head.partitionsMetadata.map(_.partitionId)

    topicsMetadata.foreach { topicMetadata =>
      val topic = topicMetadata.topic
      partitions.foreach { partitionId =>
        val partitionMetadataOpt = topicMetadata.partitionsMetadata.find(_.partitionId == partitionId)
        partitionMetadataOpt match {
          case Some(metadata) =>
            metadata.leader match {
              case Some(leader) =>
                val ts = System.currentTimeMillis()
                val consumer = new SimpleConsumer(leader.host, leader.port, 10000, 100000, clientId)
                val topicAndPartition = TopicAndPartition(topic, partitionId)
                val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(time, nOffsets)))
                val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets
                println("%s:%d:%s".format(topic, partitionId, offsets.mkString(",")))
                val ds = Seq(OffsetInfo(topic, partitionId, offsets.head, ts)).toDS()
                ds.repartition(1).write.mode("append")
                  .jdbc(jdbcUrl, myTable, connectionProperties)  // write to MySQL table
              case None =>
                System.err.println("Error: partition %d does not have a leader. Skip getting offsets".format(partitionId))
            }
          case None => System.err.println("Error: partition %d does not exist".format(partitionId))
        }
      }
    }

  }
}

