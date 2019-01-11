package com.aisiot.bigdata.flinkcore
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object flinkwc {
  def main(args: Array[String]) {
    //val env = StreamExecutionEnvironment.getExecutionEnvironment
     val env = ExecutionEnvironment.getExecutionEnvironment
   // val socketStream = env.socketTextStream("ec2-13-126-44-170.ap-south-1.compute.amazonaws.com",9000)
  //  val wordsStream = socketStream.flatMap(x => x.split("\\s+")).map(value => (value,1))
  val path="C:\\work\\datasets\\wcdata.txt"
    val output ="C:\\work\\datasets\\output\\flinkwcdata"
    val text = env.readTextFile(path)
   val res = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1).setParallelism(1)
    res.print()
    res.writeAsCsv(output)
//    val keyValuePair = text.keyBy(0)

  //  val countPair = keyValuePair.sum(1)
    //countPair.print()
    env.execute()
  }
}
