package zhangmingxuan.spark.scala_lx

object tostring extends App{
  class person(val name:String,val age:Int,val home:String){
    println("constructing person........")
    override def toString()=name+":"+age+":"+home  //重写toString方法
  }
  val p=new person("join",29,"lundun")
  println(p)
}
