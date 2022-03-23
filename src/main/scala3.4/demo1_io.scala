object demo1_io {
  def main(args: Array[String]): Unit = {
    for (i <- 1 to 5) println(i)

    for (i <- 1 to 5 by 2) println(i)

    for (i <- 1 to 5; j <- 1 to 3) println(i * j)

    val r = for (i <- Array(1, 2, 3, 4, 5) if i % 2 == 0) yield {
      println(i); i
    }

    import java.io.FileReader
    import java.io.FileNotFoundException
    import java.io.IOException
    try {
      val f = new FileReader("input.txt")
    } catch {
      case ex: FileNotFoundException => println(ex)
      case ex: IOException => println(ex)
    } finally {
//      f.close()
    }
  }
}
