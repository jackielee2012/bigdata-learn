import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * Created by JackieLee on 2018/10/22.
 */
object FlinkTest {

  def main(args: Array[String]) {

    val see = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 7200, "")
    val fileSource: DataStream[String] = see.readFile[String](TextInputFormat[String] ,"file:///usr/local/data/test.txt")

    fileSource.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))


  }

}
