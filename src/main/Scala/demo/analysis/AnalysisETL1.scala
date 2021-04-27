package demo.analysis

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object AnalysisETL1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .config("spark.sql.warehouse.dir", "D:/spark")
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
      .option("header","true")
      .option("multiLine", true)
      .load("file:///D:\\data\\data1217.csv")

    val datasorg=srcDF.rdd.map(l => {

      if(l.length<27){
        try {
          var n=0
          var emptyNum=0
          val len=l.length
          while (n<len-1){
            if((l(n) == null || l(n).toString.trim().length < 1||l(n) == "NULL" )){
              emptyNum=emptyNum+1
            }
            n=n+1
          }
          if(emptyNum>=3){
            //          aba_2134,九寨沟名人酒店,中国,四川,阿坝,漳扎镇,三星级/舒适,低星,292,NULL,4.539999962,57,34.06,103,188,68,129,70,126,49,89,2,2.86%,34147,7.90%,1
            Row("aaa","aaa","aaa","aaa","aaa","aaa",
              "aaa","aaa","aaa","aaa","aaa","aaa",
              "aaa","aaa","aaa","aaa","aaa","aaa",
              "aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa")//三组以上空值
          }else{
            l
          }
        }
        catch{
          case e: Exception => {
            e.printStackTrace()
            Row("aaa1","aaa","aaa","aaa","aaa","aaa",
              "aaa","aaa","aaa","aaa","aaa","aaa",
              "aaa","aaa","aaa","aaa","aaa","aaa",
              "aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa")}//其他异常
        }
      }else(
        Row("aaa2","aaa","aaa","aaa","aaa","aaa",
          "aaa","aaa","aaa","aaa","aaa","aaa",
          "aaa","aaa","aaa","aaa","aaa","aaa",
          "aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa")//超长度
      )

    })

    val schema = srcDF.schema

    //创建DataFrame
    val df = spark.createDataFrame(datasorg,schema)

    df.show(5,false)
    df.createOrReplaceTempView("studentView")
    spark.sql(
      s"""
         |select '空值过多',count(*) from studentView
         |where SEQ='aaa'
       """.stripMargin).show(false)

    val cleanDF=df.filter("SEQ not in ('aaa','aaa1','aaa2')")//清洗
    cleanDF.show(5,false)

    cleanDF.coalesce(1).write.mode("Append").csv("file:///D:\\data\\clean")




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
