package com.atguigu.sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ronin
 * @create 2020-11-03 23:46
 */
object SparkStreaming04_DirectAuto {
  def main(args: Array[String]): Unit = {
    //1 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    //2 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguiguGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    //3 创建DStream
    KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,//优先位置
      ConsumerStrategies.Subscribe[String,String](Set("testTopic"),kafkaParams) // 消费策略：（订阅多个主题，配置参数）
    )


    //6 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
