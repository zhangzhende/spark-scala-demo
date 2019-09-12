package movieAnalysis.secondsort.scala

class ScalaSecondarySortKey(val first: Double, val second: Double)
  extends Ordered[ScalaSecondarySortKey] with Serializable {
  override def compare(that: ScalaSecondarySortKey): Int = {
    if (this.first - that.first != 0) {
      //      first是时间戳是整数，second是评分是小数
      (this.first - that.first).toInt
    } else {
      if (this.second - that.second > 0) {
        //        大于0向上取整
        Math.ceil(this.second - that.second).toInt
      } else if (this.second - that.second < 0) {
        //        小于0时向下取整
        Math.floor(this.second - that.second).toInt
      } else {
        //        等于0时就是0
        (this.second - that.second).toInt
      }
    }
  }
}
