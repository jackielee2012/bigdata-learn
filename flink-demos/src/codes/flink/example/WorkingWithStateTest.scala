import java.util.Properties
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.mutable.HashMap


object WorkingWithStateTest {

  def main(args: Array[ String ]): Unit = {

    // 获取StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 配置并行数为1
    env.setParallelism(1)
    env.setNumberOfExecutionRetries(3)
    // 设置Checkpoint，时间间隔为60秒
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)

    // state用于保存计算状态，在Tuple2MapStateFunction类中实现时，只会根据当前窗口的输入数据获取state中对应的值
    var state: MapState[String, Int] = null

    // 从状态获得的值取决于input元素的键。 因此，如果所涉及的关键字不同，则在一次调用用户函数时获得的值可能与另一次调用中的值不同。
    // 比如第一个窗口计算结果为Map("hadoop" -> 2, "flink" -> 3)，第二个窗口经过sum计算后的数据为("hadoop",3)，则经过自定义的flatMap
    // 处理后，输出结果是("hadoop" -> 5)，即只处理第二个窗口输入数据相关的状态，所以需要使用一个HashMap保存所有结果并排序。
    val resultMap: HashMap[String, Int] = new HashMap[String, Int]()

    class Tuple2MapStateFunction extends RichFlatMapFunction[(String, Int), (String, Int)] {

      override def open(parameters: Configuration): Unit = {
        state = getRuntimeContext.getMapState(new MapStateDescriptor[String, Int]("WordCount State", classOf[String], classOf[Int]))
      }

      override def flatMap(value: (String, Int), out: Collector[(String, Int)]): Unit = {

        val word = value._1
        var count = value._2
        // 如果之前的数据已经处理过相同的词，那么更新相应的值，否则在state中加入当前窗口的对应数据
        if(state.contains(word)) {
          count += state.get(word)
          state.put(word, count)
        }
        else {
          state.put(word, count)
        }

        // state.keys.iterator结果是一个Java的Iterator类，需要隐式转换为Scala的Iterator类，
        // 并可以使用nonEmpty方法，判断state是否为空。
        import scala.collection.JavaConversions._
        val  stateItr: Iterator[String] = state.keys.iterator
        if (stateItr.nonEmpty) {
          while (stateItr.hasNext) {
            val key = stateItr.next()
            resultMap.put(key,state.get(key))
          }
        }
        // 所有结果根据count升序排列
        val sortedResult = resultMap.toList.sortWith(_._2 < _._2)

        // 输出所有窗口全部根据第一个字段排序的计算结果
        val resultItr = sortedResult.toIterator
        if (resultItr.nonEmpty) {
          while(resultItr.hasNext) {
            out.collect(resultItr.next)
          }
        }
      }
    }

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "jackielee.hadoop.com:9092")
    prop.setProperty("zookeeper.connect", "jackielee.hadoop.com:2181")
    prop.setProperty("group.id", "stateTest")
    // 自定义Flink实现的Kafka生产者
    val Mycustomer = new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), prop)
    // 设置读取topic中的数据从最新的开始
    Mycustomer.setStartFromLatest()

    val stream = env.addSource(Mycustomer)
    stream.flatMap(line => line.toLowerCase.split("\\s+"))
      .map((_, 1))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .sum(1)
      .keyBy(_._1)  // MapState基于KeyedStream处理数据，所以需要在这里再一次使用keyBy()方法
      .flatMap(new Tuple2MapStateFunction).print()

    // 运行程序
    env.execute("Working With State Demo")

  }
}