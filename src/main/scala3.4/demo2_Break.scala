//Breaks
import util.control.Breaks._
object demo2_Break {
  def main(args: Array[String]): Unit = {
    val array=Array(1,3,10,5,4)
    breakable(
      for (i<-array){
        if (i>5) break //跳出breakable，终止for循环，相当于java中的break
        println(i)
      }
    )

    for (i<-array){
      breakable{
        if (i>5) break //跳出breakable，终止当次循环，相当于continue
        println(i)
      }
      1.to(5)
    }
  }
}
