package zhangmingxuan.spark.scala_lx

//类的继承
object Example6_08 extends App {
  class Person(var name:String,var age:Int){
    override def toString:String="name="+name+":"+"age="+age
  }
  //通过extends关键字实现类的继承，name和age前面没有var关键字修饰
  //表示这两个成员继承自Person类，var studentno是student类新定义的成员
  class Student(name:String,age:Int,var studentno:String) extends Person(name,age){
    override def toString:String="name="+name+":"+"age="+age+":"+"studentNO="+studentno
  }
  println(new Person("join",17))
  println(new Student("ali",17,"sexs"))

}
