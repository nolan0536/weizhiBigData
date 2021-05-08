package zhangmingxuan.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkconf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("sparkSQL")
    //config是传配置
    val spark = SparkSession.builder().config(sparkconf).config("spark.sql.warehouse.dir","file:///D://test").getOrCreate()
    //TODO 执行逻辑操作
    //RDD
//    val df: DataFrame = spark.read.json("datas/user.json")
//    df.show()
    // TODO DataFrame =>sql
//    df.createOrReplaceTempView("people")
//    spark.sql("select * from people").show()
//    spark.sql("select avg(age) from people").show()
//    spark.sql("select age+1 from people").show()

    //在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    //转换规则
    // TODO DataFrame=>DSL
//    df.select("age","username").show()
//    df.select($"age"+1).show()
//    df.select('age+10).show()

    //TODO DataSet
    //DataFrame其实是特定泛型的DataSet
//    val seq = Seq(1,2,3,4,5)
//    val ds: Dataset[Int] = seq.toDS()
//    ds.show()

    //TODO RDD=》DataFrame
    val rdd=spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",30)))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    df.show()
    // TODO DataFrame=》RDD
    val rowRDD: RDD[Row] = df.rdd

    //TODO DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]
    //变成DF
    val df1: DataFrame = ds.toDF()

    //TODO RDD<=>DataSet
    val ds2: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    ds2.createOrReplaceTempView("people1")
    spark.sql("select * from people1").show()
    ds.show()

    val userRDD: RDD[User] = ds2.rdd

    //TODO 关闭环境
    spark.stop()
  }
  case class User(id:Int,name:String,age:Int)
}
