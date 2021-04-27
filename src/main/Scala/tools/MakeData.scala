package tools

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
//import redis.clients.jedis.Tuple

object MakeData {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .getOrCreate()

    //读取指定文件
    val srcDF = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("multiLine", true)
      .load("file:///D:\\data\\org\\data.csv")

    //  所属年月,商家名称,主营类型,店铺URL,特色菜,累计评论数,累计销售量,店铺评分,本月销量,本月销售额,城市,商家地址,电话
    //       0        1       2       3     4       5         6         7         8         9    10  11      12


    val datasorg = srcDF.rdd.map(l => {


//       var i=0
//      println("字段数====" + l.size)
//      while(i<l.size){
//        println(l(i))
//        i=i+1
//      }
      val cityNum = scala.util.Random.nextInt(5);
      //    val areaNum=
      var city = ""
      var adress = ""

      if (cityNum == 0) {
        city = "烟台"
        val adressNum = scala.util.Random.nextInt(5);
        if (adressNum == 0) {
          adress = city + "市" + "芝罘区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 1) {
          adress = city + "市" + "福山区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 2) {
          adress = city + "市" + "牟平区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 3) {
          adress = city + "市" + "莱山区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 4) {
          adress = city + "市" + "蓬莱区" + scala.util.Random.nextInt(100) + "号街"
        }
      } else if (cityNum == 1) {
        city = "济南"
        val adressNum = scala.util.Random.nextInt(5);
        if (adressNum == 0) {
          adress = city + "市" + "历下区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 1) {
          adress = city + "市" + "齐州区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 2) {
          adress = city + "市" + "天桥区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 3) {
          adress = city + "市" + "槐荫区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 4) {
          adress = city + "市" + "历城区" + scala.util.Random.nextInt(100) + "号街"
        }
      } else if (cityNum == 2) {
        city = "青岛"
        val adressNum = scala.util.Random.nextInt(5);
        if (adressNum == 0) {
          adress = city + "市" + "市南区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 1) {
          adress = city + "市" + "市北区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 2) {
          adress = city + "市" + "四方区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 3) {
          adress = city + "市" + "黄岛区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 4) {
          adress = city + "市" + "崂山区" + scala.util.Random.nextInt(100) + "号街"
        }
      } else if (cityNum == 3) {
        city = "临沂"
        val adressNum = scala.util.Random.nextInt(3);
        if (adressNum == 0) {
          adress = city + "市" + "兰山区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 1) {
          adress = city + "市" + "罗庄区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 2) {
          adress = city + "市" + "河东区" + scala.util.Random.nextInt(100) + "号街"
        }
      } else if (cityNum == 4) {
        city = "潍坊"
        val adressNum = scala.util.Random.nextInt(4);
        if (adressNum == 0) {
          adress = city + "市" + "奎文区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 1) {
          adress = city + "市" + "潍城区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 2) {
          adress = city + "市" + "坊子区" + scala.util.Random.nextInt(100) + "号街"
        } else if (adressNum == 3) {
          adress = city + "市" + "寒亭区" + scala.util.Random.nextInt(100) + "号街"
        }
      }


      //  所属年月,商家名称,主营类型,店铺URL,特色菜,累计评论数,累计销售量,店铺评分,本月销量,本月销售额,城市,商家地址,电话
      //       0        1       2       3     4       5         6         7         8         9    10  11      12

//      Row(l(0), city + "第" + scala.util.Random.nextInt(100) + "号店", l(2), """ + l(3) + """, l(4), l(5), l(6), l(7), l(8), l(9), city, adress, "15988888888")
      val name=city + "第" + scala.util.Random.nextInt(100) + "号店"
      val t="'"
      if(l(0).toString.trim.size>8){
        val url="'"+l(0)+"'"
        println(url)

        Row(name,url,city,adress,"18888888888")
      }else{
        Row("aaa","123",city,adress,"18888888888")
      }

    })

    val schema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("url", StringType, true),
        StructField("city", StringType, true),
        StructField("address", StringType, true),
        StructField("phone", StringType, true)
      ))

    //创建DataFrame
    val df = spark.createDataFrame(datasorg, schema)

    val resDF=df.filter("name !='aaa'")
    resDF.show( false)
    //    df.createOrReplaceTempView("studentView")
    //    spark.sql(
    //      s"""
    //         |select '空值过多',count(*) from studentView
    //         |where SEQ='aaa'
    //       """.stripMargin).show(false)
    //
    //    val cleanDF=df.filter("SEQ not in ('aaa','aaa1','aaa2')")//清洗
    //    cleanDF.show(5,false)
    //
    resDF.coalesce(1).write.mode("Append").csv("file:///D:\\data\\bisai666")


    spark.stop()
  }

  def getData() {
    val cityNum = scala.util.Random.nextInt(5);
    //    val areaNum=
    var city = ""
    var adress = ""
    println("cityNUM====" + cityNum)
    if (cityNum == 0) {
      city = "烟台"
      val adressNum = scala.util.Random.nextInt(5);
      if (adressNum == 0) {
        adress = city + "市" + "芝罘区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 1) {
        adress = city + "市" + "福山区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 2) {
        adress = city + "市" + "牟平区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 3) {
        adress = city + "市" + "莱山区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 4) {
        adress = city + "市" + "蓬莱区" + scala.util.Random.nextInt(100) + "号街"
      }
    } else if (cityNum == 1) {
      city = "济南"
      val adressNum = scala.util.Random.nextInt(5);
      if (adressNum == 0) {
        adress = city + "市" + "历下区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 1) {
        adress = city + "市" + "齐州区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 2) {
        adress = city + "市" + "天桥区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 3) {
        adress = city + "市" + "槐荫区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 4) {
        adress = city + "市" + "历城区" + scala.util.Random.nextInt(100) + "号街"
      }
    } else if (cityNum == 2) {
      city = "青岛"
      val adressNum = scala.util.Random.nextInt(5);
      if (adressNum == 0) {
        adress = city + "市" + "市南区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 1) {
        adress = city + "市" + "市北区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 2) {
        adress = city + "市" + "四方区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 3) {
        adress = city + "市" + "黄岛区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 4) {
        adress = city + "市" + "崂山区" + scala.util.Random.nextInt(100) + "号街"
      }
    } else if (cityNum == 3) {
      city = "临沂"
      val adressNum = scala.util.Random.nextInt(3);
      if (adressNum == 0) {
        adress = city + "市" + "兰山区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 1) {
        adress = city + "市" + "罗庄区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 2) {
        adress = city + "市" + "河东区" + scala.util.Random.nextInt(100) + "号街"
      }
    }else if (cityNum == 4) {
      city = "潍坊"
      val adressNum = scala.util.Random.nextInt(4);
      if (adressNum == 0) {
        adress = city + "市" + "奎文区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 1) {
        adress = city + "市" + "潍城区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 2) {
        adress = city + "市" + "坊子区" + scala.util.Random.nextInt(100) + "号街"
      } else if (adressNum == 3) {
        adress = city + "市" + "寒亭区" + scala.util.Random.nextInt(100) + "号街"
      }
    }
    val res = (city + "第" + scala.util.Random.nextInt(100) + "号店", city, adress)
  }
}
