package service

import java.io.{BufferedInputStream, File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.Calendar.DATE
import java.util.{Calendar, Date, GregorianCalendar, Properties, Random}

import Util.{HdfsUtil, RemoteHdfsUtil}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object CollectHiveTableSize {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("--class classname  path for hdfs-audit.log ")
    }

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("CollectHiveTableSize4day")
    val spark = SparkSession.builder
      .config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    //设置日志级别
    sc.setLogLevel("warn")
    import java.sql.DriverManager
    val sqlContext = spark.sqlContext

    //读取mproperties的相关配置
    val properties = new Properties

    val path = args(1).toString
    HdfsUtil.setConf(sc.hadoopConfiguration)
    val hdfsPropertiesFile=HdfsUtil.getFileSystem().open(new Path(path))
    properties.load(hdfsPropertiesFile)

    val hive_metedata_url = properties.getProperty("hive_metedata_url")
    val hive_metedata_user = properties.getProperty("hive_metedata_user")
    val hive_metedata_pwd = properties.getProperty("hive_metedata_pwd")
    val jdbc_url = properties.getProperty("jdbc_url")
    val jdbc_user = properties.getProperty("jdbc_user")
    val jdbc_pwd = properties.getProperty("jdbc_pwd")
    val begin_with = properties.getProperty("begin_with")

    val dbc = hive_metedata_url + "?user=" + hive_metedata_user + "&password=" + hive_metedata_pwd

    try {
      classOf[com.mysql.jdbc.Driver]
      val connection = DriverManager.getConnection(dbc)
      val statement = connection.createStatement()

      //从 TBLS A, DBS B, SDS  C,SERDES D表中查询出DB TABLE_NAME LOCATION 等表的相关信息
      val resultSet = statement.executeQuery("SELECT B.NAME AS DB, A.TBL_NAME AS TABLE_NAME, " +
        "A.SD_ID, A.TBL_TYPE,A.CREATE_TIME  , C.LOCATION,D.SLIB " +
        "FROM TBLS A, DBS B, SDS  C,SERDES D " +
        "WHERE A.`DB_ID` = B.`DB_ID` " +
        "AND C.`SD_ID` = A.`SD_ID` " +
        "AND C.`SERDE_ID`=D.`SERDE_ID` " +
        "AND D.SLIB  not like  '%hbase%' ")



      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      var cal: Calendar = Calendar.getInstance()
      cal.add(Calendar.DATE, -1)
      var date = dateFormat.format(cal.getTime())


      val dbc_2 = jdbc_url + "?user=" + jdbc_user + "&password=" + jdbc_pwd
      val connection_2 = DriverManager.getConnection(dbc_2)
      val statement2 = connection_2.createStatement()


      //初始化list为可变list
      var list = new ListBuffer[ArrayBuffer[String]]

      while (resultSet.next()) {
        try {

          val DB = resultSet.getString(1)
          val TABLE_NAME = resultSet.getString(2)
          val SD_ID = resultSet.getString(3)
          val TBL_TYPE = resultSet.getString(4)
          val CREATE_TIME = resultSet.getString(5)
          var LOCATION = resultSet.getString(6)
          val SLIB = resultSet.getString(7)
          var WRITE_COUNT = 0
          var READ_COUNT = 0

          //对LOCAION字段做处理，将字符串替换为和日志统一格式

          val src = LOCATION.replace(begin_with, "src=")


          //获取文件系统的类型
          val fileSystem = HdfsUtil.getFileSystem()

         // val  fileSystem=RemoteHdfsUtil.getFileSystem("hdfs://hadoop-master-jishi-01:8020")




          if (fileSystem.exists(new Path(LOCATION))) {

            //调用getFileSize方法 得到文件的大小FILESIZE
            val FILESIZE = HdfsUtil.getFileSize(fileSystem, LOCATION).toString()
          //  val FILESIZE =RemoteHdfsUtil.getFileSize(fileSystem,LOCATION).toString()




            //将字段写入到mysql中
            val result = statement2.executeUpdate("INSERT into t_hive_tableinfo values('" + date + "','" + DB + "','" + TABLE_NAME + "','" + SD_ID + "','" + TBL_TYPE + "','" + CREATE_TIME + "','" + LOCATION + "','" + SLIB + "','" + FILESIZE + "','" + WRITE_COUNT + "','" + READ_COUNT + "','" + src + "')" +
              "ON DUPLICATE KEY UPDATE date='" + date + "',DB='" + DB + "',TABLE_NAME='" + TABLE_NAME + "'")
            //用list收集每行的（LOCATION,FILESIZE)
            val arr = scala.collection.mutable.ArrayBuffer[String]()
            arr += (LOCATION, FILESIZE)
            list += arr

          } else {
            val FILESIZE = "null"
            //        LOCATION=LOCATION.replace("hdfs://centerhdp","hdfs://namenode1:8020")

            val result = statement2.executeUpdate("INSERT into t_hive_tableinfo values('" + date + "','" + DB + "','" + TABLE_NAME + "','" + SD_ID + "','" + TBL_TYPE + "','" + CREATE_TIME + "','" + LOCATION + "','" + SLIB + "','" + FILESIZE + "','" + WRITE_COUNT + "','" + READ_COUNT + "','" + src + "') " +
              "ON DUPLICATE KEY UPDATE date='" + date + "',DB='" + DB + "',TABLE_NAME='" + TABLE_NAME + "'")


            val arr = scala.collection.mutable.ArrayBuffer[String]()
            arr += (LOCATION, FILESIZE)
            list += arr


          }

          /*val FILESIZE = "null"
           val result = statement2.executeUpdate("INSERT into t_hive_tableinfo values('" + date + "','" + DB + "','" + TABLE_NAME + "','" + SD_ID + "','" + TBL_TYPE + "','" + CREATE_TIME + "','" + LOCATION + "','" + SLIB + "','" + FILESIZE + "','" + WRITE_COUNT + "','" + READ_COUNT + "','" + src + "') " +
             "ON DUPLICATE KEY UPDATE date='" + date + "',DB='" + DB + "',TABLE_NAME='" + TABLE_NAME + "'")

           // map +=(LOCATION->FILESIZE)
           val arr = scala.collection.mutable.ArrayBuffer[String]()
           arr += (LOCATION, FILESIZE)
           list += arr*/


        } catch {
          case e: Exception => e.printStackTrace()
        }
      }

      //将list注册为rdd
      val rdd1 = sc.makeRDD(list)
      //对rdd1进行处理 变成rdd[Row]格式的rdd2
      val rdd2 = rdd1.map(x =>
        Row(x(0), x(1))
      )

      //设置schema的格式
      val schema = StructType(
        Seq(
          StructField("LOCATION", StringType, true),
          StructField("FILESIZE", StringType, true)
        )
      )

      //将rdd2转换为datafream
      val rdd_df = spark.createDataFrame(rdd2, schema)
      //将rdd_df注册为临时表FILESIZE_TABLE
      rdd_df.createOrReplaceTempView("FILESIZE_TABLE")


      //从日志文件中获取datafream
      import sqlContext.implicits._

      // val file_df = sc.textFile("d://hdfs-audit.log").map(line => {


      val filePath = args(0).toString()
      val fileSystem_2 = HdfsUtil.getFileSystem()
      val fileList = listChildren(fileSystem_2, filePath)

      val colNames = Array("DATE", "cmd", "asrc")
      val schema_2 = StructType(colNames.map(fieldName => StructField(fieldName, StringType, true)))
      var file_all_Df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema_2)
      var uu = new Random()

      for (path <- fileList) {

        val file_df = sc.textFile(path).map(line => {
          val fields = line.replaceAll("\\t", " ").split(" ")
          val list = List("cmd=open", "cmd=create", "cmd=append", "cmd=delete", "cmd=mkdirs", "cmd=rename")


          val length = fields.size
          var date = ""
          var cmd = ""
          var asrc = ""
          //这里有两种情况，日志中的字段可能是16也可能是13
          if (length == 16) {
            if (list.contains(fields(11))) {

                date = fields(0)
                cmd = fields(11)
                asrc = fields(12)




            }

          } else if (length == 13) {
            if (list.contains(fields(8))) {


                date = fields(0)
                cmd = fields(8)
                asrc = fields(9)




            }
          }

          if (cmd == "cmd=open")
            cmd = cmd + "_" + uu.nextInt(200)
          (date, cmd, asrc)


        }).filter(x => x._2 != "").toDF().withColumnRenamed("_1", "DATE").withColumnRenamed("_2", "cmd").withColumnRenamed("_3", "asrc")

        file_all_Df = file_all_Df.union(file_df)
      }

      print("file_all_Df count:" + file_all_Df.count())

      //从数据库中获取datafream
      val db_df = sqlContext.read.format("jdbc").options(
        Map("url" -> dbc_2,
          "dbtable" -> "t_hive_tableinfo")).load()

      //从file_df，db_df两个datafream注册的临时表中查询出相同DATE和匹配的src对应的数据
      file_all_Df.repartition(8000).createOrReplaceTempView("file_table")
      val printdf = spark.sql("select cmd ,count(cmd) as ccmd from file_table group by cmd  order by ccmd desc limit 10 ")
      printdf.toDF().collect().foreach(x => println(x))
      db_df.cache().createOrReplaceTempView("db_table")


      /* val result_df0 = spark.sql("select b.DATE,b.DB,b.TABLE_NAME,b.LOCATION as LOCATION," +
        "a.cmd as cmd ,b.WRITE_COUNT as WRITE,b.READ_COUNT as READ,a.src as asrc ,broadcast(b.src as bsrc) from db_table b join file_table a   " +
        "on a.DATE=b.DATE ").toDF()*/

      import org.apache.spark.sql.functions.broadcast

     val result_df0= broadcast(spark.table("db_table"))
        .join(spark.table("file_table"), "DATE")

      //println("bcount:"+bcount)

      result_df0.repartition(8000).createOrReplaceTempView("result_df0_table")
     // spark.sql("select * from result_df0_table limit 10").show(10)

     // val result_df1=spark.sql("select DATE,DB,TABLE_NAME,LOCATION,cmd from result_df0_table  where asrc like concat(src,'%') ")
      val result_df1= result_df0.filter($"asrc".contains($"src"))


      //println("result_df1 count："+result_df1.count())
      //result_df1.limit(10).toDF().collect().foreach(x=>println(x))

      result_df1.repartition(1000).createOrReplaceTempView("result_df1_table")

      //对result_df1_table用DATE,DB,TABLE_NAME,LOCATION,cmd做groupBy 得出每个cmd对应的count
      val result_df3 = spark.sql("select DATE,DB,TABLE_NAME,LOCATION," +
        " substring(cmd, 5) as cmd,count(cmd) as cmdcount from result_df1_table group by DATE,DB,TABLE_NAME,LOCATION,cmd ").toDF().cache()

      //对cmd字段进行过滤
      //val result_df4 = result_df3.where($"cmd".isin("open", "create", "append", "detele", "mkdirs", "rename"))

      result_df3.createOrReplaceTempView("result_df3_table")
      println("result_df3_table count："+result_df3.count())

      //FILESIZE_TABLE和result_df4_table做join
      val result_df5 = spark.sql("SELECT a.DATE,a.DB,a.TABLE_NAME,a.LOCATION,a.cmd,a.cmdcount,b.FILESIZE FROM result_df3_table a join FILESIZE_TABLE b ON (a.LOCATION = b.LOCATION)").toDF()


      //将结果写到mysql中
      val properties = new Properties()
      properties.setProperty("user", jdbc_user)
      properties.setProperty("password", jdbc_pwd)
      //result_df5.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.184.61:3306/hive", "RESULT_DF", properties)

     println("result_df5:"+result_df5.count())
     result_df5.write.mode(SaveMode.Append).jdbc(jdbc_url, "t_hive_resultinfo", properties)


    } catch {
      case e: Exception => e.printStackTrace()
    } finally {}

    }



      def listChildren(hdfs: FileSystem, FilePath: String): ListBuffer[String] = {
        val filesStatus = hdfs.listStatus(new Path(FilePath))

        val holder = ListBuffer[String]()
        for (status <- filesStatus) {
          val filePath: Path = status.getPath
          if (!filePath.toString.endsWith("tmp")) {
            holder += filePath.toString
          }

        }
        holder
      }


    }
