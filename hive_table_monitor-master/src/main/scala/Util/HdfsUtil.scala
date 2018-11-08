package Util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI


object HdfsUtil {
  var conf = new Configuration()

  def setConf(conf: Configuration) {
    this.conf = conf

  }

  def getFileSystem(): FileSystem = {

    FileSystem.get(conf)
  }



  def getFileSize(fs: FileSystem, path: String): BigInt = {
    val length = fs.getContentSummary(new Path(path)).getLength
    println("path size: " + length)
    length
  }

}
