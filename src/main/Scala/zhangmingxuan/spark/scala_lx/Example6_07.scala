package zhangmingxuan.spark.scala_lx

//辅助构造函数中的默认参数
object Example6_07 extends App {
  //定义无参的主构造函数并将其定义为私有
  class Person private{
    private var name:String=null
    private var age:Int=0
    private var sex:Int=0
    //带默认参数的主构造函数
    def this(name:String="",age:Int=18,sex:Int=1){
      //先调用主构造函数
      this()

      this.name=name
      this.age=age
      this.sex=sex
    }
    override def toString={
      val setsex=if(sex==1) "男" else "女"
      s"name=$name,age=$age,sex=$setsex"
    }
  }
  //使用默认参数，调用的是辅助构造函数，相当于调用Person（"join",18,1）
  val p=new Person("join")
  println(p)
  //由于主构造函数为private，此时调用的便是带默认参数的辅助构造函数
  println(new Person)
}
