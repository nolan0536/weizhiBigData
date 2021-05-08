package zhangmingxuan.spark.scala_lx

object hello_scala {
  def main(args: Array[String]): Unit = {
    //    val helloString = "hello word"
    //    val ster:String="hello122323323123"
    //    println(helloString)
    //    print(ster)
    //重新赋值
    //    var test="zmx"
    //    test="zhang123"
    //    println(test)
    //      var sumvalue=1*2
    //      println(sumvalue)
    //for循环
    //for(i<- 1.to(5)) {
    //println("i="+i)
    //}
    //    breakable {
    //      for (i <- 1 to 10) {
    //        if (i > 9) break
    //        println("i=" + i)
    //
    //      }
    //    }
    //    for (i <- 1 to 10 if (i < 9)) {
    //      println("i=" + i)
    //    }
    //    for (i <- 1 to 50 if (i % 5 == 0); if (i % 4 == 0)) {
    //      println("i=" + i)
    //    }
    //    for (i <- 1 to 10 if (i > 7)) {
    //      for (j <- 7 to 9 if (j > 7)) {
    //        println("i=" + i, "j=" + j)
    //      }
    //    }
    //    val mater = mutable.Set(1, 2, 3, 4)
    //    print(mater)
    //    println("\n")
    //    val setAeeat = ArrayBuffer[String]()
    //    setAeeat += ("word")
    //    setAeeat ++= Array("list", "set", "words")
    //    println(setAeeat)
    //
    //
    //    val intarray = ArrayBuffer(1, 2, 4, 6, 78, 8)
    //    intarray.insert(0, 9999, 1111, 2222)
    //    println(intarray)
    //    val multion = List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9))
    //    println(multion)
    //
    //    val nums = List(1, 2, 3, 4, 5, 6, 7)
    //    println(nums.tail.head)
    //    nums.toArray
    //    println(nums)
    //    val ran = List.apply(1, 2, 3)
    //    val ran1=List.range(2,6)
    //    println(ran1)
    //
    //
    //    //映射
    //    val student2=scala.collection.mutable.Map("join"->21,"sess"->22,"tom"->23)
    //    student2.foreach(e=>{val (k,v)=e;println(k+":"+v)})
    //    student2.foreach(e=>println(e._1+":"+e._2))
    //    println(student2.get("join"))
    //
    //    //函数
    ////    def sum(x:Int,y:Int)=x+y
    ////    println(sum(7,8))
    //
    //    val sum=(x:Int,y:Int)=>{
    //      println(x+y)
    //      x+y
    //    }
    //    sum(123,123)
    //
    //    //使用lazy函数修饰函数字面量
    //    lazy val f=(x:Double)=>{
    //      x*20
    //      println(x*20)
    //    }
    //    f(5)

    val arrInt = Array(1, 2, 3, 4, 5)
    //        val increment=(x:Int)=>x+1
    //    arrInt.map((x:Int)=>x+1;println(x))
    //    val j = 0
    //    for (i <- arrInt) {
    //      if (i > 0) {
    //        println(j + i)
    //        println(i + 1)
    //      }
    //    }
    //    val data = for (i <- arrInt) yield i + 1
    //    for (i <- data) {
    //        println(i)
    //    }
    val increment = (x: Int) => x + 1
    val data2 = arrInt.map(increment)
    for (i <- data2) {
      println(i)
    }
    val arrInt2 = Array(1, 2, 3, 4, 5)
    val datas = arrInt2.map(x => x + 1)
    for (data <- datas) {
      println(data)
    }
    val ARRAY = Array("spark", "hive", "hadoop").map(i => i * 2)
    for (i <- ARRAY) {
      println(i)
    }
    val list = List("list1" -> 1, "list2" -> 2, "list3" -> 3)
    val lists1 = list.map { l => l._2 }
    for (i <- lists1) {
      println(i)
    }
    val map = Map("list1" -> 1, "list2" -> 2, "list3" -> 3)
    val maps = map.map { l => l._1 }
    for (i <- maps) {
      println(i)
    }
    val list3 = List(List(1), List(2, 3, 4), List(5, 6, 7, 8))
    val dataes = list3.flatMap(x => x)
    println(dataes)

    def product(x1: Int, x2: Int, x3: Int) = x1 * x2 * x3

    def product_1 = product(_: Int, 2, 3)

    def product_2 = product(_: Int, _: Int, 4)

    def product_3 = product(_: Int, _: Int, _: Int)

    println(product_3(3, 4, 5))
    println(product_1(3))

    def sex(x1: Int, x2: Int, x3: Int) = (x1 + x2) * x3

    val sex_1 = sex(_: Int, 2, 3)
    println(sex_1(5))

    class People {
      var name: String = "zhangsan"

      def setPeole(p: String): Unit = {
        this.name = p;
      }

      def getPeole(): String = {
        return this.name
      }
    }
    var p = new People()
    println(p.name)
    p.setPeole("lisi")
    println(p.getPeole())

    //    var tmp= p.name
    p.name = "wangwu"
    println(p.name)


    import scala.beans.BeanProperty
    class Person1 {
      @BeanProperty var name: String = null
    }

    val s = new Person1()
    s.setName("join")
    println(s.getName)

    //    class Utils{
    //      public static Double PI=3.141529
    //    }
    Student.uniqueStudentNo(5)
  }
}

