import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment

/**
 * Created by JackieLee on 2018/10/22.
 */
object AthletesAnalysis {

  // Create Flink Context
  val env = ExecutionEnvironment.getExecutionEnvironment

  // Create Flink Table Context
  val tEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

  // Read From HDFS and Create DataSet
  val data = env.readCsvFile("/in/path/to/hdfs")

  // Create Table
  val athletes: Table = tEnv.fromDataSet(data)

  // Register Table to BatchTableEnvironment
  tEnv.registerTable("athletes", athletes)

  // SQL Query
  val result: Table = tEnv.sqlQuery(
    """
      |Select name, sum(score) as sum_score
      |From athletes where id=1000 group by name order by sum_score;
    """.stripMargin)

  // Result to DataSet
  val resultData = tEnv.toDataSet(result)

  resultData.print()

}
