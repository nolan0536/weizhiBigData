package zhangmingxuan.spark.scala_lx

//应用程序对象
object extends_App extends App {
  object student{
    private  var studentNO:Int=0
    def uniqueStudentNo(p:Int):Unit={
      studentNO=studentNO+p
      println(studentNO)
    }
  }
  student.uniqueStudentNo(10)
}
