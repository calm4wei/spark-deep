import java.util
import java.util.Comparator

/**
  * Created on 2016/12/29
  *
  * @author feng.wei
  */
object TransactionTest {

    def main(args: Array[String]) {
        val data = Array(10, 20, 1, 3)

        //        val list = sortNum(data)

        for (i <- 0 to data.length - 1){
            println(data(i))
        }
        println("====================")
        for (i <- 0 until(data.length)){
            println(data(i))
        }

//        val list = sortList(data)
//
//        println("================")
//        for (i <- (0 until (list.size())).reverse) {
//            println(list.get(i))
//        }
//
//        println("chushu=" + (47 / 128.0))

    }

    def sortNum(data: Array[Int]): util.ArrayList[Int] = {
        val list = new util.ArrayList[Int]()
        data.map {
            d => {
                println(d)

                if (list.size() == 0) {
                    list.add(d)
                } else if (list.size() == 1) {
                    if (d > list.get(0)) {
                        list.add(1, d)
                    } else {
                        list.add(d)
                    }
                } else {
                    if (d > list.get(0)) {
                        if (d > list.get(1)) {
                            list.remove(0)
                            list.add(1, d)
                        } else {
                            list.add(0, d)
                        }
                    }

                }
            }
        }
        list
    }

    def sortList(data: Array[Int]): util.ArrayList[Int] = {
        val list = new util.ArrayList[Int]()

        data.map {
            d => {
                list.add(d)
            }
        }

        list.sort(new Comparator[Int] {
            override def compare(o1: Int, o2: Int): Int = {
                if (o1 > o2) 1 else -1
            }
        })
        list
    }

}
