package zhangmingxuan.spark.scala_lx

//辅助构造函数
object Example6_05 extends App {
  class Person{
    private var name:String=null
    private var age:Int=18
    private var sex:Int=1

    def this(name:String){
      this()
      this.name=name
    }
    def this(name:String,age:Int){
      this(name)
      this.age=age
    }
    def this(name:String,age:Int,sex:Int){
      this(name,age)
      this.sex=sex
    }

    override def toString={
      val sexstr=if(sex==1) "男" else "女"
      s"name=$name,age=$age,sex=$sexstr"
    }
  }
  println(new Person("join",18,1))
  println(new Person("bali"))
}
