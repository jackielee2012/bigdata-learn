import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.parsing.json.JSONObject

object Test {

  def main(args: Array[ String ]): Unit = {

    val FIELDS = Array("userId", "channel", "productId", "amount", "action", "eventTime")
    val str =
      """
        |10000,pc,ETH,0,50,2018-10-27 12:30:00
      """.stripMargin.trim
    val values = str.split(",")
    val kv = new mutable.HashMap[String, Any]()

    var i = 1
    while(i < FIELDS.length) {
      kv.put(FIELDS(i), values(i))
      i = i + 1
    }

    val json = new JSONObject(kv.toMap)
    print(json)

    val a = UserEvent.buildUserEvent(
      """
        |{"channel" : "pc", "amount" : "0", "eventTime" : "2018-10-27 12:30:00", "productId" : "ETH", "action" : "50"}
      """.stripMargin)

    print(a.getAction())

  }
}
