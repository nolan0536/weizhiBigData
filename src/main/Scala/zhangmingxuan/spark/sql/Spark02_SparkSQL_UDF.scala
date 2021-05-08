package zhangmingxuan.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkconf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("sparkSQL")
    //config是传配置
    val spark = SparkSession.builder().config(sparkconf).config("spark.sql.warehouse.dir","file:///D://test").getOrCreate()
    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("prefixName",(name:String)=>{
      "Name:"+name
    })

    spark.sql("select age,prefixName(username) from user").show()


    spark.stop()
  }

}
