package demo.analysis

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object AnalysisTest2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName).config("spark.sql.warehouse.dir", "D:/spark")
      .getOrCreate()
    //读取数据
    //val inputpath=args(0);
    //val outpath=args(1);
    val dataOrg=spark.sparkContext.textFile("file:///D:\\data\\student.txt")
    //val dataOrg=spark.sparkContext.textFile(inputpath)
    // 将数据转化成一行行ROW的形式
    val data=dataOrg.map(line=>{
      val datas=line.split(",")
      Row(datas(0).toInt,datas(1),datas(2))
    })

    //指定dataframe的类型
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("gender", StringType, true)
      ))
    //创建DataFrame
    val df = spark.createDataFrame(data,schema)
    df.show(5,false)
    //创建视图查询结果
    df.createOrReplaceTempView("studentView")
    spark.sql(
      s"""
         |select * from studentView
         |where id >5
       """.stripMargin).show(5,false)
    //resDF.show(5,false)
    //resDF.coalesce(1).write.mode("Append").csv(outpath)
    //spark.stop()

  }
}
