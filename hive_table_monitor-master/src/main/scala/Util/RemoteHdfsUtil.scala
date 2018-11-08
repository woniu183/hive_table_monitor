package Util

import java.net.{URI, URL}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}



object RemoteHdfsUtil {
  var conf = new Configuration()

  /**
    *
    * @param url
    * @return
    */

  def getFileSystem(url:String): FileSystem = {
    return  FileSystem.get(new URI(url), conf)

  }

  def getFileSize(fs: FileSystem, path: String): BigInt = {
    val length = fs.getContentSummary(new Path(path)).getLength
    println("path size: " + length)
    length
  }

  def main(args: Array[String]): Unit = {
    val fs=getFileSystem("hdfs://hadoop-master-jishi-01:8020")
    print(fs.getContentSummary(new Path("/test/bdm_b2b2c_user_t_user_buyer_da/000163_0")).getLength)
  }


}
