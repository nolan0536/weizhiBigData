package zhangmingxuan.spark.scala_lx

object Student{
  private  var studentNO:Int=0
  def uniqueStudentNo(p:Int):Unit={
    studentNO=studentNO+p
    println(studentNO)
  }

  def main(args: Array[String]): Unit = {
    Student.uniqueStudentNo(10)
    Student.uniqueStudentNo(10)
  }
}

