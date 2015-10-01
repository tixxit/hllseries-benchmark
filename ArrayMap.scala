package com.stripe.mapbench

import java.util.Arrays
import scala.collection.mutable.ArrayBuilder

class ArrayMap(val keys: Array[Int], val values: Array[Long]) {
  def get(n: Int): Option[Long] =
    scala.util.Try(apply(n)).toOption

  def apply(n: Int): Long =
    values(Arrays.binarySearch(keys, n))

  def foreach(f: (Int, Long) => Unit): Unit = {
    var i = 0
    while (i < keys.length) {
      f(keys(i), values(i))
      i += 1
    }
  }

  def filter(f: (Int, Long) => Boolean): ArrayMap = {
    var i = 0
    val keysBldr = ArrayBuilder.make[Int]()
    val valsBldr = ArrayBuilder.make[Long]()
    while (i < keys.length) {
      val k = keys(i)
      val v = values(i)
      if (f(k, v)) {
        keysBldr += k
        valsBldr += v
      }
      i += 1
    }
    new ArrayMap(keysBldr.result(), valsBldr.result())
  }

  def merge(that: ArrayMap)(f: (Long, Long) => Long): ArrayMap = {
    val lKeys = keys
    val lVals = values
    val rKeys = that.keys
    val rVals = that.values
    var l = 0
    var r = 0
    val keysBldr = ArrayBuilder.make[Int]()
    val valsBldr = ArrayBuilder.make[Long]()
    while (l < lKeys.size && r < rKeys.size) {
      val lKey = lKeys(l)
      val rKey = rKeys(r)
      if (lKey < rKey) {
        keysBldr += lKey
        valsBldr += lVals(l)
        l += 1
      } else if (rKey < lKey) {
        keysBldr += rKey
        valsBldr += rVals(r)
        r += 1
      } else {
        keysBldr += lKey
        valsBldr += f(lVals(l), rVals(r))
        l += 1
        r += 1
      }
    }
    while (l < lKeys.size) {
      keysBldr += lKeys(l)
      valsBldr += lVals(l)
      l += 1
    }
    while (r < rKeys.size) {
      keysBldr += rKeys(r)
      valsBldr += rVals(r)
      r += 1
    }
    new ArrayMap(keysBldr.result(), valsBldr.result())
  }
}

object ArrayMap {
  def empty: ArrayMap = new ArrayMap(new Array[Int](0), new Array[Long](0))

  def create(key: Int, value: Long): ArrayMap = {
    val keys = new Array[Int](1)
    keys(0) = key
    val vals = new Array[Long](1)
    vals(0) = value
    new ArrayMap(keys, vals)
  }

  def apply(kvs: (Int, Long)*): ArrayMap = {
    val (keys, values) = kvs.sorted.unzip
    new ArrayMap(keys.toArray, values.toArray)
  }
}
