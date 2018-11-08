//import org.apache.spark.{SparkConf, SparkContext}
//
///**
// * Created by JackieLee on 2018/10/19.
// */
//object CommonFriends {
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("CommonFriends Job").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//
//    val data = Seq(
//                    (100, Array(200, 300, 400)),
//                    (200, Array(100)),
//                    (300, Array(100, 400)),
//                    (400, Array(100, 300)),
//                    (500, Array(600, 700)),
//                    (600, Array(500)),
//                    (700, Array(500))
//                  )
//
//    val rdd = sc.parallelize(data)
//
////    rdd.map(x => {
////      val l = new scala.collection.mutable.ListBuffer[((Int, Int), Array[Int])]
////      val person = x._1
////      val friends = x._2
////      for (i <- friends.length) {
////        if (friends(i) > person) {
////         var kv =  ((person, friends(i)), friends)
////         l += kv.copy()
////        }
////        else {
////          var kv =  ((friends(i), person), friends)
////          l += kv.copy()
////        }
////      }
////      l.toList
////
////    })
//    sc.stop()
//  }
//
//}
