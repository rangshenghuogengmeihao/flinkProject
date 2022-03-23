import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala._
case class Access(id:Long,name:String,age:Int)
object demo3 {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val input = environment.readTextFile("src/main/resources/2")
    val inputAccess = input.map(
      data => {
        val line1 = data.split(",")(0).split("\\(")(1)
        val line2 = data.split(",")(2).split("\\)")(0)
        (line1,line2)
      }
    )
    val inputAccess2= input
      .map(_.stripPrefix("Access(").stripSuffix(")"))
      .map(_.split(","))
      .map(a=>(a(0),a(1),a(2)))


    inputAccess.print()

  }
}
//class MyFilter extends FilterFunction[Access]{
//  override def filter(value: Access): Boolean = {
//    (value.id,value.name,value.age)
//  }
//}