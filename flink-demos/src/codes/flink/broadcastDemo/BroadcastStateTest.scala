import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.HashMap

object BroadcastStateTest {

  def main(args: Array[ String ]): Unit = {
    // 初始化
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 处理逻辑根据事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)

    // 用于保存广播变量，这个Demo中使用的是固定配置，可以通过使用Kafka来动态配置
    val ruleStateDesc = new MapStateDescriptor(
      "RulesBroadcastState",
      classOf[String],  // 渠道channel
      classOf[Int]      // 交易数量amount
    )
    // 用于保存用户状态信息的描述信息，Map[(userId, productId), amount]
    val userMapStateDesc = new MapStateDescriptor("UserMapStateDesc", classOf[(Int, String)], classOf[Int])
    // 用于保存用户统计状态
    var userState: MapState[(Int,String), Int] = null

    // 用户保存所有历史结果Map[(userId, productId), amount]
    val resultMap: HashMap[(Int, String), Int] = new HashMap[(Int, String), Int]

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "jackielee.hadoop.com:9092")
    prop.setProperty("zookeeper.connect", "jackielee.hadoop.com:2181")
    prop.setProperty("group.id", "test")
    // Kafka消费者
    val FirstConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), prop)
    FirstConsumer.setStartFromLatest()

    // 用户行为输入数据流
    val userEventStream: KeyedStream[UserEvent, Int] = env.addSource(FirstConsumer)
      .map(x => UserEvent.buildUserEvent(x))
      .assignTimestampsAndWatermarks(new CustomWatermarkExtractor)
      .keyBy(_.getUserId())

    // 广播变量流
    val broadcastStream: BroadcastStream[(String, Int)] = env.fromElements(("app", 50)).broadcast(ruleStateDesc)

    val resultStream = userEventStream.connect(broadcastStream)
      .process(new KeyedBroadcastProcessFunction[Int, UserEvent,(String, Int), (Int, String, Int)] {

        // 处理广播变量流
        override def processBroadcastElement(value: (String, Int), ctx: KeyedBroadcastProcessFunction[ Int, UserEvent, (String, Int),
          (Int, String, Int) ]#Context, out: Collector[ (Int, String, Int) ]): Unit = {

          val rulestate = ctx.getBroadcastState(ruleStateDesc)
          rulestate.put(value._1, value._2)
        }

        // 处理用户数据流，程序主要执行代码
        override def processElement(value: UserEvent, ctx: KeyedBroadcastProcessFunction[ Int, UserEvent, (String, Int),
          (Int, String, Int) ]#ReadOnlyContext, out: Collector[ (Int, String, Int) ]): Unit = {

          userState = getRuntimeContext.getMapState(userMapStateDesc)
          // 获取广播变量配置信息
          val configAmount = ctx.getBroadcastState(ruleStateDesc).get("app")

          val userId = value.getUserId()
          val channel = value.getChannel()
          val productId = value.getProductId()
          val amount = value.getAmount()

          // 处理UserEvent
          if (channel.equals("qpp")) {
            if (userState.contains((userId, productId))) {
              val totalAmount = userState.get((userId, productId)) + amount
              userState.put((userId, productId), totalAmount)
            }
            else {
              userState.put((userId, productId), amount)
            }
          }
          // 将所有结果保存在resultMap中
          import scala.collection.JavaConversions._
          val stateKeyItr: Iterator[ (Int, String) ] = userState.keys.iterator
          if (stateKeyItr.nonEmpty) {
            while (stateKeyItr.hasNext) {
              val key = stateKeyItr.next()
              resultMap.put(key, userState.get(key))
            }
          }
          // 遍历resultMap，输出所有数据
          val resultItr = resultMap.iterator
          if (resultItr.nonEmpty) {
            while (resultItr.hasNext) {
              val outKey = resultItr.next()._1
              val outValue = resultMap.get(outKey) match { // get方法得到的是Option[Int]类型的数据，需要用match获取Int类型的数据
                case Some(x) => x
                case None => 0
              }
              if (outValue > configAmount) {
                out.collect((outKey._1, outKey._2, outValue))
              }

            }
          }
        }

      }).print()


    env.execute("BroadCast State Demo")


  }

}
