package zhangmingxuan.spark.scala_lx

class People {
  var name="zhangsan"
  def setPeole(p:String): Unit ={
    this.name=p;
  }
  def getPeole(): String ={
    return this.name
  }
}
