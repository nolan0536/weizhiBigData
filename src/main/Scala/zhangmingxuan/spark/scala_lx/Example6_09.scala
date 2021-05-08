package zhangmingxuan.spark.scala_lx

//构造函数的执行顺序
object Example6_09 extends App {
  class Person(var name:String,var age:Int){
    override def toString:String={
      s"name:$name,age:$age"
    }
    println("执行person类的主构造函数")
  }
  class Student(name:String,age:Int,var studentno:Int)extends Person (name,age){
    override def toString:String={
      s"name:$name,age:$age,sudentno:$studentno"
    }
    println("执行student类的主构造函数")
  }
  println(new Student("join",11,121212))
  //调用子类对象时，先调用父类主构造函数，然后在调用子类的主构造函数
}
