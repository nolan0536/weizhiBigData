package zhangmingxuan.spark.scala_lx

//方法重写
object Example6_10 extends App {
  class Person(var name:String,var age:Int){
    //对父类Any中的toString方法进行重写
    override def toString=s"Person($name,$age)"
  }
  class student(name:String,age:Int,var studentno:Int) extends Person(name,age){
    //对父类person中的toString方法进行重写
    override def toString=s"Student($name,$age,$studentno)"
  }
  val p=new student("join",18,111)
  println(p)


}
