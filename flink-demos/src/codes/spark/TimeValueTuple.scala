///**
// * Created by JackieLee on 2018/10/18.
// */
//class TimeValueTuple(time : Int, value : Int) extends Comparable[TimeValueTuple] {
//
//  var first = time
//  var second = value
//
//  def TimeValueTuple(first: Int, second: Int): Unit = {
//    (first, second)
//  }
//
//  def getTime() : Int = {
//    return this.first
//  }
//
//  def setTime(x : Int) : Unit = {
//    this.first = x
//  }
//
//  def getValue() : Int = {
//    return this.second
//  }
//
//  def setValue(x : Int) : Unit = {
//    this.second = x
//  }
//
//  override def compareTo(o: TimeValueTuple): Int = {
//    if (first > o.first)
//      return 1
//
//    else if (first < o.first)
//      return -1
//
//    else if (second > o.second)
//      return 1
//
//    else if (second < o.second)
//      return -1
//
//    else
//      return 0
//
//  }
//
//  override def hashCode(): Int = {
//    return 373 * first + 11 * second
//  }
//
//  def equals(obj: TimeValueTuple): Boolean = {
//    if (obj.first == first && obj.second == second)
//      return true
//    else
//      return false
//  }
//
//  override def toString: String = "(" + first + ", " + second + ")"
//}
