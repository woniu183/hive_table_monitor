package service

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadFile {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ReadFile").setMaster("local")
    val spark = SparkSession.builder
      .config(sparkConf).getOrCreate()
    val sc =spark.sparkContext
      val file =sc.textFile("d://hdfs-audit.log").map(line => {
      val fields =line.replaceAll("\\t"," ").split(" ")

        val length =fields.size
        var date =""
        var cmd =""
        var src =""

        if(length ==16){
          date =fields(0)
          cmd =fields(11)
          src =fields(12)
        }else if(length==13){
          date =fields(0)
          cmd =fields(8)
          src =fields(9)
        }





      (date,cmd,src)



    })

   // file.map(_._3).foreach(println)

    //file.saveAsTextFile("d://ddd")





      //.map(_.replace(" "," ")).saveAsTextFile("d://ccc")
     //file.foreach(println)






  }

}
