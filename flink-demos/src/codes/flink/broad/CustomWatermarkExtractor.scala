import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import scala.math.max

class CustomWatermarkExtractor extends AssignerWithPeriodicWatermarks[UserEvent]{
  val maxOutOfOrderness = 60000L // 1 minute
  var currentMaxTimestamp: Long = 0L

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }

  override def extractTimestamp(element: UserEvent, previousElementTimestamp: Long): Long = {
    val timestamp = element.getEventTime()
    currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
    timestamp
  }
}
