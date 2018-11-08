import java.text.SimpleDateFormat
import com.alibaba.fastjson.JSON

class UserEvent(userId: Int, channel: String, productId: String, amount: Int, action: Int, eventTime: Long) {

  def getUserId(): Int = userId
  def getChannel(): String = channel
  def getProductId(): String = productId
  def getAmount(): Int = amount
  def getAction(): Int = action
  def getEventTime(): Long = eventTime

}

object UserEvent{
  private var userId: Int = 0
  private var channel: String = null
  private var productId: String = null
  private var amount: Int = 0
  private var action: Int = 0
  private var eventTime: Long = 0L

  def buildUserEvent(event: String): UserEvent = {
    val eventJson = JSON.parseObject(event.trim)
    userId = eventJson.getIntValue("userId")
    channel = eventJson.getString("channel")
    productId = eventJson.getString("productId")
    amount = eventJson.getIntValue("amount")
    action = eventJson.getIntValue("action")

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    eventTime = format.parse(eventJson.getString("eventTime")).getTime

    new UserEvent(userId, channel, productId, amount, action, eventTime)
  }

}

