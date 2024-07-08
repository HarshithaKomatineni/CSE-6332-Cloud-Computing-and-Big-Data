import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.File

object Multiply {

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Multiply")
    val sc = new SparkContext(conf)

    val matrixM = sc.textFile(args(0))
    val matrixN = sc.textFile(args(1))

    val matrixMTuples = matrixM.map(line => {
      val tokens = line.split(",")
      (tokens(0).toInt, tokens(1).toInt, tokens(2).toDouble)
    })

    val matrixNTuples = matrixN.map(line => {
      val tokens = line.split(",")
      (tokens(0).toInt, tokens(1).toInt, tokens(2).toDouble)
    })

    val numWorkers = sc.getConf.getInt("spark.executor.instances", 1)
    val matrixMPartitioned = matrixMTuples.repartition(numWorkers)
    val matrixNPartitioned = matrixNTuples.repartition(numWorkers)

    val result = matrixMPartitioned.map(m => (m._2, m))
      .join(matrixNPartitioned.map(n => (n._1, n)))
      .map({ case (_, (m, n)) => ((m._1, n._2), m._3 * n._3) })
      .reduceByKey(_ + _)
      .map({ case ((i, j), value) => s"$i,$j,$value" })
      .sortBy(identity)
    result.saveAsTextFile(args(2))
  
    val otpDir = new File(args(2))
    val otptFile = new File(otpDir, "part-00000")
    val rnFile = new File(otpDir, "result-small-matrix.txt")
    otptFile.renameTo(rnFile)
  
    sc.stop()

  }
}
