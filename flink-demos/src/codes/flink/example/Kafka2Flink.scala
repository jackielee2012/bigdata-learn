import java.util.Properties

import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.scala._

/**
 * Created by JackieLee on 2018/10/23.
 */
object Kafka2Flink {

  def main(args: Array[String]) {

    /*
自定义的序列化类型，继承SerializationSchema[T]类，重写serialize方法，
实现对(String, Int)类型数据的序列化，符合Kafka消息传输
 */
    class Tuple2Schema extends SerializationSchema[(String, Int)]{

      override def serialize(t: (String, Int)): Array[Byte] = {
        var out = new scala.collection.mutable.ArrayBuffer[Byte]()
        out ++= t._1.getBytes("utf-8")
        out ++= ", ".getBytes("utf-8")
        out ++= t._2.toString.getBytes("utf-8")

        out.toArray
      }
    }

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.enableCheckpointing(1000)
    senv.setMaxParallelism(1)
    senv.setNumberOfExecutionRetries(2)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "jackielee.hadoop.com:9092")
    prop.setProperty("zookeeper.connect", "jackielee.hadoop.com:2181")
    prop.setProperty("group.id", "test")

    // KafkaConsumer
    val customConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), prop)
    customConsumer.setStartFromEarliest()

    val stream = senv.addSource(customConsumer)

    val result: DataStream[(String, Int)] = stream.flatMap{
      line => line.toLowerCase.split("\\s+")}
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(1)

    // 定义Kafka Producer，针对Flink生成的数据类型[(String, Int)]，需要使用自定义的Tuple2Schema类
    val customProducer = new FlinkKafkaProducer011[(String, Int)]("jackielee.hadoop.com:9092","out", new Tuple2Schema())
    result.addSink(customProducer)

    senv.execute("Kafka to Flink WordCount Demo")

  }
}


