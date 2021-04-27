package demo.analysis

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {

  // 字段： SEQ	酒店	国家	省份	城市	商圈	星级	业务部门	房间数	图片数	评分	评论数	城市平均实住间夜	酒店总订单	酒店总间夜	酒店实住订单	酒店实住间夜	酒店直销订单
  //         1    2   3   4   5    6  7    8      9      10    11   12   13               14        15       16          17          18
  // 酒店直销间夜	酒店直销实住订单	酒店直销实住间夜	酒店直销拒单	酒店直销拒单率	城市直销订单	城市直销拒单率	拒单率是否小于等于直销城市均值
  //      19                  20        21                22          23              24          25          26
  //   样例数据   aba_2069	阿坝马尔康县澜峰大酒店	中国	四川	阿坝	NULL	二星及其他	低星	115	NULL	3.977930069	129	34.06	35	72	27	59	34	71	27	59	6	0.1765	34147	0.079	0

  //数据对象类
  case class Hotel(SEQ: String,
                   hotel: String,
                   country: String,
                   province: String,
                   city: String,
                   BusinessDistrict: String,
                   lever: String,
                   department: String,
                   rooms: String,
                   pictures: String,
                   pingfen: String,
                   pinglunshu: String,
                   chengshishizhujianye: String,
                   jiudianzongdingdan: String,
                   jiudianzongjianye: String,
                   jiudianshizhudingdan: String,
                   jiudianshizhujianye: String,
                   jiudianzhixiaodingdan: String,
                   jiudianzhixiaojianye: String,
                   jiudianzhixiaoshizhudingdan: String,
                   jiudianzhixiaoshizhujianye: String,
                   jiudianzhixiaojudan: String,
                   jiudianzhixiaojudanlv: String,
                   chegnshizhixiaodingdan: String,
                   chengshizhixiaojudanlv: String,
                   junzhi: String)

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[2]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //读取数据
    val rdd01: RDD[String] = sc.textFile("file:///D:\\data\\clean\\jiudian.csv")
    val rddData: RDD[Hotel] = rdd01.map {
      line => {
        val infos: Array[String] = line.split(",")
        new Hotel(
          infos(0),
          infos(1),
          infos(2),
          infos(3),
          infos(4),
          infos(5),
          infos(6),
          infos(7),
          infos(8),
          infos(9),
          infos(10),
          infos(11),
          infos(12),
          infos(13),
          infos(14),
          infos(15),
          infos(16),
          infos(17),
          infos(18),
          infos(19),
          infos(20),
          infos(21),
          infos(22),
          infos(23),
          infos(24),
          infos(25)
        )
      }
    }

    //按城市分组求每个城市订单总和
    rddData.map(r => (r.city, r.jiudianzongdingdan))//选取需要的字段创建元组
      .groupByKey()//按key值分组
      .map(tuple => {
        var sum = 0.0
        for (score <- tuple._2) {
          sum += score.toDouble
        }
        val formatSum = f"$sum%.0f"//小数点后0位
        (tuple._1, formatSum)
      })
      .filter(line=>(line._1 =="鞍山")||(line._1 =="白沙"))
      .coalesce(1)//重分区成一个分区
      .foreach(println(_))

    //按城市分组求平均酒店直销拒单
    rddData.map(r => (r.city, r.jiudianzhixiaojudan))
      .groupByKey()
      .map(tuple => {
        var sum = 0.0
        val num = tuple._2.size
        for (score <- tuple._2) {
          sum += score.toDouble
        }
        val avg = sum / num
        val formatAvg = f"$avg%.2f"
        (tuple._1, formatAvg)
      }).coalesce(1)
      .foreach(println(_))

    //计算多项指标城市的总销量及平均拒单率
    rddData.map(r => (r.city, (r.jiudianzongdingdan,r.jiudianzhixiaojudanlv)))
      .groupByKey()
      .map(tuple => {
        var sum = 0.0
        var JuSum=0.0
        val num=tuple._2.size
        for (score <- tuple._2) {
          sum += score._1.toDouble
          JuSum += score._2.toDouble
        }
        val avg = JuSum / num
        val formatAvg = f"$avg%.2f"
        val formatSum = f"$sum%.0f"
        (tuple._1, formatSum,formatAvg)
      }).coalesce(1)
      .foreach(println(_))

    //计算各城市酒店订单数的最大值最小值
    rddData.map(r => (r.city, r.jiudianzongdingdan))
      .groupByKey()
      .map(x => {
//        var min = Integer.MAX_VALUE
//        var max = Integer.MIN_VALUE
        var min = 666666
        var max = 0
        for (num <- x._2) {
          if (num.toInt > max) {
            max = num.toInt
          }
          if (num.toInt < min) {
            min = num.toInt
          }
        }
        (x._1, max, min)
      }).coalesce(1)
      .foreach(x => {
        println(x._1 + "max\t" + x._2 + "   min\t" + x._3)
      })



    // 按城市分组求和，按总订单数排序排序(top5)
    rddData.map(r => (r.city, r.jiudianzongdingdan))
      .groupByKey()
      .map(tuple => {
        var sum = 0.0
        for (score <- tuple._2) {
          sum += score.toDouble
        }
        val formatSum = f"$sum%.0f"
        (tuple._1, formatSum)
      }).map(data => (data._2, data._1))
      .sortByKey(false)//加false是降序，不加升序
      .map(data => (data._2, data._1))
      .coalesce(1)
      .take(5)//取5条
      .foreach(println(_))

    //统计各市一共有多少旅馆（词频统计）
    rddData.map(r => (r.city))
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)
      .foreach(println)
    rddData.map(r => (r.city,1))
      .groupByKey()
      .map(tuple => {
        var sum = 0.0
        val num = tuple._2.size
        (tuple._1, num)
      }).coalesce(1)
      .foreach(println(_))

    //分组topN
    val top3Hotel=rddData.map(r => (r.city, r.jiudianzongdingdan))
      .groupByKey()
      .map(groupedPair => (groupedPair._1, groupedPair._2.toList.sortWith((x,y) => x > y).take(3)))


    top3Hotel.foreach(pair => {
      println(pair._1 + ":")
      pair._2.foreach(println(_))
    })

    sc.stop()

  }



}
