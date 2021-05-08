package zhangmingxuan.spark.scala_lx

//多态
object Example6_11 extends App {
  class Person(var name:String,var age:Int){
    def walk():Unit=println("walk() method in Person")
    def talkTo(p:Person):Unit=println("talkTo() method in Person")
  }
  class Student(name:String,age:Int) extends Person(name,age) {
    private var studentno:Int=0
    override def walk()=println("walk like an elegant swan")
    override def talkTo(p:Person)={
      println("talkTo() method in Student")
      println(this.name+"is talking to"+p.name)
    }
  }
  class Teacher(name:String,age:Int) extends Person(name,age){
    private var teacherNO:Int=0
    override def walk()=println("walk like an elegant swan")
    override def talkTo(p:Person)={
      println("talkTo() method in Teacher")
      println(this.name+"is talking wo"+p.name)
    }
  }
  val p1:Person=new Teacher("albert",38)
  val p2:Person=new Student("join",38)

}
