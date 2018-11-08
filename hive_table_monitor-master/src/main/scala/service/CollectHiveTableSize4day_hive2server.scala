package service

import java.sql.{Connection, DriverManager}
import java.util

import Util.HdfsUtil.conf
import Util.{HdfsUtil, HiveJdbcUtil}
import Util.HiveJdbcUtil.{Connection4db, connection}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class CollectHiveTableSize4day_hive2server {

}


object CollectHiveTableSize4day_hive2server {

  def main(args: Array[String]) {
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("CollectHiveTableSize4day")
    val spark = SparkSession.builder
      .config(sparkConf).getOrCreate()
    import spark.implicits._
    import spark.sql
    //获取 db,table,location


    val con = HiveJdbcUtil.getConnection("172.172.240.2", 10000, "hive", "")
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    HdfsUtil.setConf(spark.sparkContext.hadoopConfiguration)
    val databases = HiveJdbcUtil.getDataBases(con)
    val table4db = databases.map { db =>
      val conn4fdb = HiveJdbcUtil.Connection4db("172.172.240.2", 10000, "hive", "", db)
      val tables = HiveJdbcUtil.getTables4DB(conn4fdb)
      (db, tables.toArray)
    }

    val table4dbRDD = spark.sparkContext.makeRDD(table4db, 20).flatMapValues(x => x).cache()


    val map4tableLocation = new java.util.HashMap[String, String]()


    table4dbRDD.count()




    table4dbRDD.collect().foreach { r =>
      val location = HiveJdbcUtil.getTableLocation(con, r._1, r._2)
      map4tableLocation.put(r._1 + r._2, location)
      Thread.sleep(1000)
      //println(r._1 + "." + r._2 + ":" + location)
    }

    val map4tableSize = new util.HashMap[String, BigInt]()
    val fileSystem = HdfsUtil.getFileSystem()


    val iterator = map4tableSize.keySet().iterator()
    while (iterator.hasNext()) {
      val key = iterator.next();
      val location = map4tableLocation.get(key)
      val fileSize = HdfsUtil.getFileSize(fileSystem, location)
      map4tableSize.put(location, fileSize)
    }

    val tableArrayBuffer = new ArrayBuffer[(String, String, String, BigInt)]()

    table4dbRDD.collect().toList.foreach { t =>
      val location = map4tableLocation.get(t._1 + t._2)
      val size = map4tableSize.get(location)
      val value = (t._1, t._2, location, size)
      tableArrayBuffer ++= Array(value)
    }

    tableArrayBuffer.toArray



  }


}