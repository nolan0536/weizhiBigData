package zhangmingxuan.spark.scala_lx

//默认参数的主构造函数
object moren extends App {
  class Peerson(var name:String="",var age:Int=18){
    override def toString()="name="+name+":"+"age="+age
  }
  val p1=new Peerson("join")
  println(p1)
  class Peerson1(var name:String,var age:Int){
    override def toString()="name="+name+":"+"age="+age
  }
  val p2=new Peerson("join",19)
  println(p2)

}

