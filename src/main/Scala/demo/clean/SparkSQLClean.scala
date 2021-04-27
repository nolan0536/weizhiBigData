package demo.clean

import org.apache.spark.sql.{Row, SparkSession}


object SparkSQLClean {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName).config("spark.sql.warehouse.dir", "D:/spark")
      .getOrCreate()
    val srcDF = spark
      .read
      .format("csv")
      .option("header", "true") //如果是true就是要头部信息，false就是不要头信息
      .option("multiLine", true)
      .load("file:///D:\\data\\org\\data.csv")


    val data = srcDF.rdd.map(l => {
      val datas = l
      if (datas.size == 13) {
        if ((datas(8) == null || datas(8) == "0") && (datas(9) == null || datas(9) == "0")) {
          Row("aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa")
        } else if (datas(3).toString.substring(0, 1) == "'" && datas(3).toString.substring(datas(3).toString.size - 1) == "'") {
          Row(datas(0), datas(1), datas(2), datas(3).toString.substring(1, datas(3).toString.size - 1), datas(4), datas(5), datas(6), datas(7), datas(8), datas(9), datas(10), datas(11), datas(12))
        } else {

          Row("aaa1", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa")
        }
      } else {
        Row("aaa2", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa")
      }
    })
    data.map(l => (l(0), 1))
      .groupByKey()
      .map(tuple => {
        (tuple._1, tuple._2.size)
      }
      ).coalesce(1)
      .foreach(re => {
        if (re._1 == "aaa") {
          println(".....删除的条目数为：" + re._2 + "行......")
        }
      })

    val resData = data.filter(l => l(0) != "aaa").filter(l => l(0) != "aaa1").filter(l => l(0) != "aaa2") //过滤异常值
    //输出结果
    resData.coalesce(1).foreach(println(_))
   val df= spark.createDataFrame(resData,srcDF.schema)
    df.coalesce(1).write.mode("Append").csv("file:///D:\\data\\bisai\\sqlClean12151")
    spark.stop()
  }


}
