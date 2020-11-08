package com.atguigu.sparkstreaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @author ronin
 * @create 2020-11-02 13:49
 * 优雅关闭:
 *         关闭方式：使用外部文件系统来控制内部程序关闭。
 */
object SparkStreaming11_stop {
  def main(args: Array[String]): Unit = {
    //1 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

    //2 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //设置优雅关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    //3 创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //业务逻辑
    lineDStream.flatMap(_.split(" ")).map((_,1)).print()
    //开启监控程序
    new Thread(new MonitorStop(ssc)).start()
    //6 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
class MonitorStop(ssc:StreamingContext) extends Runnable{
  override def run(): Unit = {
    //获取hdfs文件系统
    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020"), new Configuration(), "atguigu")
    //通过判断外部条件:某个文件是否存在,来优雅的关闭
    while (true){
      Thread.sleep(5000)
      //获取/stopSpark路径是否存在
      val result: Boolean = fs.exists(new Path("hdfs://hadoop102:8020/stopSpark"))
      if(result){
        val state: StreamingContextState = ssc.getState()
        //获取当前任务是否正在进行
        if(state==StreamingContextState.ACTIVE){
          //优雅的关闭
          ssc.stop(true,true)
          System.exit(0)
        }
      }
    }
  }
}
