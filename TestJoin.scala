import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangtuntun on 16-3-21.
  */
object TestJoin {
  def main(args: Array[String]) {
    //设置环境
    val conf = new SparkConf().setAppName("union").setMaster("local")
    val sc = new SparkContext(conf)

    //加载数据
    val data_kmeans = sc.textFile("/home/wangtuntun/MLLibPractice/Data/Kmeans_data", 1)
    val data_bayes = sc.textFile("/home/wangtuntun/MLLibPractice/Data/Bayes_data", 1)

    val data_union=data_bayes.union(data_kmeans)
    println("union begin")
    data_union.foreach(println(_))
    println("union end")


    val bayes_rdd_pair=data_bayes.map( x=>(1,x) )

    bayes_rdd_pair.map(tt=> println(tt._2) )
    //    bayes_rdd_pair.foreach()
    def print_right(x:(Int,String))={
      println(x._2)
    }
    bayes_rdd_pair.map(print_right(_))

    sc.stop()

  }

}
