package Test

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object test001 {
  def main(args: Array[String]): Unit = {
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("CollectHiveTableSize4day")
    val spark = SparkSession.builder
      .config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    //设置日志级别
    sc.setLogLevel("warn")
    import java.sql.DriverManager
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    val date=dateFormat.format(cal.getTime())
    println(date )

    val filePath = args(0).toString()
    println(filePath)
    val file_df = sc.textFile(filePath).map(line=>{

      val fields = line.replaceAll("\\t", " ").split(" ")
      val list = List("cmd=open", "cmd=create", "cmd=append", "cmd=delete", "cmd=mkdirs", "cmd=rename")


      val length = fields.size
      var date = ""
      var cmd = ""
      var src = ""

      //这里有两种情况，日志中的字段可能是16也可能是13
      if (length == 16) {
        if (list.contains(fields(11))) {


          date = fields(0)
          cmd = fields(11)
          src = fields(12)


        }

      } else if (length == 13) {
        if (list.contains(fields(8))) {

          date = fields(0)
          cmd = fields(8)
          src = fields(9)

        }
      }


      (date, cmd, src)
    }).filter(x=>x._1!=null).toDF().withColumnRenamed("_1", "DATE").withColumnRenamed("_2", "cmd").withColumnRenamed("_3", "src").show(10)




  }

}
/*  val file_df = sc.textFile(filePath).map(line => {
    val fields = line.replaceAll("\\t", " ").split(" ")
    val list=List("cmd=open","cmd=create","cmd=append","cmd=delete","cmd=mkdirs","cmd=rename")


    val length = fields.size
    var date = ""
    var cmd = ""
    var src = ""
    //这里有两种情况，日志中的字段可能是16也可能是13
    if (length == 16) {
      if(list.contains(fields(11))){
        date = fields(0)
        cmd = fields(11)
        src = fields(12)


      }

    } else if (length == 13) {
      if(list.contains(fields(8))){
        date = fields(0)
        cmd = fields(8)
        src = fields(9)


      }
    }


    (date, cmd, src)



  }).toDF().withColumnRenamed("_1", "DATE").withColumnRenamed("_2", "cmd").withColumnRenamed("_3", "src").show(10)


}*/


