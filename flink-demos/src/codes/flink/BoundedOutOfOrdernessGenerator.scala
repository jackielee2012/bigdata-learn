import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * Created by JackieLee on 2018/10/20.
 */
class BoundedOutOfOrdernessGenerator extends AssignerWithPeriodicWatermarks[String] {

  val maxOutOfOrderness = 5000L // 5 seconds

  var currentMaxTimestamp: Long = 0L

  override def extractTimestamp(t: String, l: Long): Long = {

    val timestamp = t.split(",")(1).toLong

    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)

    timestamp
  }

  override def getCurrentWatermark: Watermark = {

    new Watermark(currentMaxTimestamp - maxOutOfOrderness)

  }


}
