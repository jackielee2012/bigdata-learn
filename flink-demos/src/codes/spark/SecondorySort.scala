//import org.apache.spark.{SparkConf, SparkContext}
//
///**
// * Created by JackieLee on 2018/10/17.
// */
//object SecondorySort {
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("SecondarySort Job").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//
//    val lines = sc.textFile("")
//    val rdd = lines.map(_.split("\\s+"))
//      .filter(_.length == 3)
//      .map(x => (x(0), new TimeValueTuple(x(1).toInt, x(2).toInt)))
//      .sortBy(_._2)
//      .collect
//
//    sc.stop()
//  }
//
//}
