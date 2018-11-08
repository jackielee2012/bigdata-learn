import java.text.SimpleDateFormat
import java.util.stream.Collector.Characteristics

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import scala.math.max

/**
 * Created by JackieLee on 2018/10/20.
 */
object Socket2Flink {

  def main(args: Array[String]) {
    // Custom WindowFunction
    class WindowFunctionTest extends WindowFunction[(String, Long),(String, Int, String, String, String, String), String,TimeWindow] {

      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int, String, String, String, String)]): Unit = {
        val list = input.toList.sortBy(_._2)
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        out.collect(key, input.size, format.format(list.head._2), format.format(list.last._2), format.format(window.getStart), format.format(window.getEnd))
      }
    }

    if(args.length != 2) {
      System.err.println("USAGE:\nSocketWatermarkTest <hostname> <port>")
      return
    }

    val host = args(0)
    val port = args(1).toInt

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    // Job depends on EventTime
    see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    see.setMaxParallelism(1)

    val stream = see.socketTextStream(host, port)
    val extractStream = stream.map{event: String => (event.split("\\s+")(0), event.split("\\s+")(1).toLong)}

    // Assign TimeStamp and generate WaterMark
    val watermark = extractStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      // Set delay as 5 seconds
      val maxOutOfOrderness = 5000L
      var currentMaxTimestamp: Long = 0L

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(t: (String, Long), l: Long): Long = {
        val timestamp = t._2
        currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
        timestamp

      }
    })

    // Operators with WindowFunction
    val window = watermark
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply(new WindowFunctionTest)

    // Write results to HDFS
    window.writeAsText("""hdfs://jackielee.hadoop.com:8020/data/out/socketOutput.txt""")

    // Run job
    see.execute("Socket2Flink Demo")

  }

}
