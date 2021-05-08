package zhangmingxuan.spark.scala_lx

//apply()方法，在执行val s=Student()时会自动调用伴生对象student的apply（）方法
object Example6_03 extends App {
  class Student{
    var name:String=null
  }
  object Student{
    def apply()=new Student()
  }
  val s=Student()
  s.name="joins"
  println(s.name)

}
