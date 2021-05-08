package zhangmingxuan.spark.req

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object shengsai {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("shengsai")
    val sc=new SparkContext(sparkconf)
    val rdd: RDD[String] = sc.textFile("datas/data.csv")
    val data=rdd.map(l=>{
      val datas=l.split(",")
      if(datas.size==13){
        if(((datas(8).substring(1,datas(8).size-1)=="0")||(datas(8).substring(1,datas(8).size-1)==" "))&&((datas(9).substring(1,datas(9).size-1)=="0")||(datas(9).substring(1,datas(9).size-1)==" "))){
          ("aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa")
        }else if(((datas(0).substring(0,1)=="\"")&&(datas(0).substring(datas(0).size-1)=="\""))||
          ((datas(1).substring(0,1)=="\"")&&(datas(1).substring(datas(1).size-1)=="\""))||
          ((datas(2).substring(0,1)=="\"")&&(datas(2).substring(datas(2).size-1)=="\""))||
          ((datas(3).substring(0,1)=="\"")&&(datas(3).substring(datas(3).size-1)=="\""))||
          ((datas(4).substring(0,1)=="\"")&&(datas(4).substring(datas(4).size-1)=="\""))||
          ((datas(5).substring(0,1)=="\"")&&(datas(5).substring(datas(5).size-1)=="\""))||
          ((datas(6).substring(0,1)=="\"")&&(datas(6).substring(datas(6).size-1)=="\""))||
          ((datas(7).substring(0,1)=="\"")&&(datas(7).substring(datas(7).size-1)=="\""))||
          ((datas(8).substring(0,1)=="\"")&&(datas(8).substring(datas(8).size-1)=="\""))||
          ((datas(9).substring(0,1)=="\"")&&(datas(9).substring(datas(9).size-1)=="\""))||
          ((datas(10).substring(0,1)=="\"")&&(datas(10).substring(datas(10).size-1)=="\""))||
          ((datas(11).substring(0,1)=="\"")&&(datas(11).substring(datas(11).size-1)=="\""))||
          ((datas(12).substring(0,1)=="\"")&&(datas(12).substring(datas(12).size-1)=="\""))){
          (datas(0).substring(1,datas(0).size-1),datas(1).substring(1,datas(1).size-1),datas(2).substring(1,datas(2).size-1),
            datas(3).substring(1,datas(3).size-1),datas(4).substring(1,datas(4).size-1),datas(5).substring(1,datas(5).size-1),
            datas(6).substring(1,datas(6).size-1),datas(7).substring(1,datas(7).size-1),datas(8).substring(1,datas(8).size-1),
            datas(9).substring(1,datas(9).size-1),datas(10).substring(1,datas(10).size-1),datas(11).substring(1,datas(11).size-1),
            datas(12).substring(1,datas(12).size-1))
        }else{
          ("aaa1","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa")
        }
      }else{
        ("aaa2","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa","aaa")
      }
    })
    val rdd3=data.map(l => (l._1, 1)).reduceByKey(_+_)
    val rdd4: RDD[Unit] = rdd3.map {
      t1 => {
        if (t1._1 == "aaa") {
          println("删除的行数为:" + t1._2)
        }
      }
    }
    rdd4.collect()
    val data2: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)] = data.filter(l => l._1 != "aaa").filter(l=>l._1!="aaa1").filter(l=>l._1!="aaa2")
    data2.map(l=>(l.productIterator.mkString(","))).coalesce(1).saveAsTextFile("output")


    sc.stop()

  }

}
