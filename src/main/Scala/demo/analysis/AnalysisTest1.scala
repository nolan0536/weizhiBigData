package demo.analysis

import org.apache.spark.sql.{DataFrame, SparkSession}

object AnalysisTest1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName).config("spark.sql.warehouse.dir", "D:/spark")
      .getOrCreate()

//    //从mysql读取数据
//    val interfaceClassificationDF = readFromMysql(spark,
//      "(select * from employee ) as interface_classification")
//    interfaceClassificationDF.createTempView("interfaceClassificationView")
//    interfaceClassificationDF.show(5, false)

    //读取指定文件
    val srcDF = spark
      .read
      .format("csv")
      .option("header","true") //如果是true就是要头部信息，false就是不要头信息
      .option("multiLine", true)
      .load("file:///D:\\data\\xiaoshuo2.csv")

//    val rr = srcDF.toDF("id", "name","gender")  创建头部信息

    srcDF.createOrReplaceTempView("studentView") //创建临时视图

    spark.sql(
      s"""
         |select * from studentView
       """.stripMargin).show(false)




    spark.stop()
  }


  def readFromMysql(sparkSession: SparkSession, table: String): DataFrame = {
    val url = s"jdbc:mysql://localhost:3306/resale?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&zeroDateTimeBehavior=round"
    val user = "root"
    val password = "root"
    val driver = "com.mysql.jdbc.Driver"
    // 从 mysql 中读取数据
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .load()
    jdbcDF
  }
}
