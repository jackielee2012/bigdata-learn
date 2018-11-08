import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.{WikipediaEditEvent, WikipediaEditsSource}

/**
 * Created by JackieLee on 2018/10/20.
 */
object Wiki2Flink {

  def main(args: Array[String]) {

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    val stream : DataStream[WikipediaEditEvent] = see.addSource[WikipediaEditEvent](new WikipediaEditsSource())

    stream.keyBy(event => new KeySelector[WikipediaEditEvent, String](){
      def getKey(event : WikipediaEditEvent) : String = {
        event.getUser
      }
    }).timeWindow(Time.seconds(5))


    see.execute("Wiki2Flink Demo")

  }

}
