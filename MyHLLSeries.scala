package com.stripe.mapbench

import scala.collection.mutable.ArrayBuilder

import com.twitter.algebird._

case class MyHLLSeries(bits: Int, rows: Vector[ArrayMap]) {
  def since(threshold: Long) = {
    val newRows = rows.map(_.filter { (j, ts) => ts >= threshold })
    MyHLLSeries(bits, newRows)
  }

  // def toHLL: HLL = {
  //   if (rows.isEmpty)
  //     SparseHLL(bits, Map())
  //   else
  //     rows.zipWithIndex.map{
  //       case (map, i) =>
  //         SparseHLL(bits, map.mapValues{ ts => Max((i + 1).toByte) }): HLL
  //     }.reduce{ _ + _ }
  // }
}

class MyHyperLogLogSeriesMonoid(val bits: Int) extends Monoid[MyHLLSeries] {
  import HyperLogLog._

  val zero = MyHLLSeries(bits, Vector())

  def create(example: Array[Byte], timestamp: Long): MyHLLSeries = {
    val hashed = hash(example)
    val (j, rhow) = jRhoW(hashed, bits)
    val emptyMap = ArrayMap.empty
    val rows = Vector.fill(rhow - 1)(emptyMap) :+ ArrayMap.create(j, timestamp)
    MyHLLSeries(bits, rows)
  }

  def plus(lhs: MyHLLSeries, rhs: MyHLLSeries): MyHLLSeries = {
    val lRows = lhs.rows
    val rRows = rhs.rows
    if (lRows.size > rRows.size) {
      plus(rhs, lhs)
    } else {
      var i = 0
      val len = math.min(lRows.size, rRows.size)
      val bldr = Vector.newBuilder[ArrayMap]
      while (i < len) {
        val z = lRows(i).merge(rRows(i)) { (x, y) =>
          math.max(x, y)
        }
        bldr += z
        i += 1
      }
      while (i < lRows.size) {
        bldr += lRows(i)
        i += 1
      }
      while (i < rRows.size) {
        bldr += rRows(i)
        i += 1
      }
      MyHLLSeries(bits, bldr.result())
    }
  }
}
