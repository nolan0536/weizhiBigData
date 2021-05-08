package zhangmingxuan.spark.scala_lx

//辅助构造函数中的默认参数
object Example6_06 extends App {
  class parse{
    private var name:String=null
    private var age:Int=18
    private var sex:Int=0

    //带默认参数的辅助构造函数
    def this(name:String="",age:Int=20,sex:Int=1){
      //先调用主构造函数 否则会报错 调用parse中的参数
      this()
      this.name=name
      this.age=age
      this.sex=sex
    }

    override def toString={
      val sexstr=if(sex==1) "男" else "女"
      s"name=$name,age=$age,sex=$sexstr"
    }
  }
  val p=new parse()
  println(p)
}
