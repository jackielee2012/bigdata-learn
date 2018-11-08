import java.util.Properties

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{Kafka09TableSource, FlinkKafkaConsumerBase, KafkaJsonTableSource}
import org.apache.flink.table.api.{TableSchema, Table, TableEnvironment}
import org.apache.flink.types.Row

object WcTest {

  case class WordCount(word: String, frequecy: Int)

  def main(args: Array[String]) {
    // 初始化
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // 生成DataSet
    val input = env.fromElements[WordCount](WordCount("Flink", 1), WordCount("Spark", 2),
      WordCount("Kafka", 2), WordCount("Flume", 1))

    // 将DataSet注册为表wordcount
    tEnv.registerDataSet("wordcount", input)

    // 执行SQL并生成新表赋值给变量名table
    val table: Table = tEnv.sqlQuery(
      """
        |Select word,sum(frequency) as sum_frequency
        |from wordcount group by word order by sum_frequency;
      """.stripMargin)

    // 查询结果表转换为DataSet[Row]
    val result: DataSet[Row] = tEnv.toDataSet[Row](table)
    // 打印
    result.print()

    // 查询结果表转换为DataSet[(String, Int)]
    val result2: DataSet[(String, Int)] = tEnv.toDataSet[(String, Int)](table)
    result2.print()

  }

}
