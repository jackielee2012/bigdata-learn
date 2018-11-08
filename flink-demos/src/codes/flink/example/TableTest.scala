import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.api.scala._
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

/**
 * Created by JackieLee on 2018/10/22.
 *
 */
object TableTest {

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    // TableEnvironment is the abstract base class for batch and stream TableEnvironments.
    val tEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

    // ******************
    // 通过序列化数据注册表
    // ******************
//    val input = env.fromElements(WordCount("hello", 1), WordCount("time", 1), WordCount("spark", 1), WordCount("flink", 2))
//    tEnv.registerDataSet("word_count", input)
//    val table = tEnv.scan("word_count")
//    // Register temporary table
//    val table2 = tEnv.fromDataSet(input)

    // ************************
    // 通过读取HDFS上的文件注册表
    // ************************
    val fieldNames: Array[String] = Array("word", "frequency")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT)
    // Create a Flink Table Source
    val source = new CsvTableSource("""hdfs://jackielee.hadoop.com:8020/data/test/wc.input""", fieldNames, fieldTypes)
    // register a table named word_count
    tEnv.registerTableSource("word_count", source)

    // SQL Query
    val result = tEnv.sqlQuery(
      """
        |SELECT word, sum(frequency) AS `sum_fre`
        |FROM word_count GROUP BY word order by sum_fre
      """.stripMargin)

    // Transform results to DataSet and write to HDFS
    // ** The type of sql results must be [ROW], and ROW cann't be analysed.
//    tEnv.toDataSet[Row](result).writeAsText("""hdfs://jackielee.hadoop.com:8020/data/out/wc.out""")
    tEnv.toDataSet[Row](result).print()

//    env.execute("Table Test Job")
  }
}
