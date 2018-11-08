//import org.apache.spark.{SparkConf, SparkContext}
//
///**
// * Created by JackieLee on 2018/10/11.
// */
//
//object WordCount {
//
//  def main (args: Array[String]) {
//    val conf = new SparkConf().setAppName("WordCount Job").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//
//    val fileRdd = sc.textFile("", 2)
//    fileRdd.flatMap(_.split("\\s+"))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//      .map(x => (x._2,x._1))
//      .sortByKey()
//      .map(x => (x._2, x._1))
//      .collect()
//
//    //
//    sc.stop()
//
//  }
//
//}
