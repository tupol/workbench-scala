package org.tupol.tnp

import java.io.RandomAccessFile

import scala.annotation.tailrec

object FilePartitioner extends App {


  /** Returns the position of the first character of the pattern in the input
   * or -1 if pattern was not found */
  def findFirst[T](pattern: Seq[T], in: Iterator[T]) = {
    @tailrec
    def ffp(src: in.GroupedIterator[T], pos: Int, found: Boolean = false): Int =
      if (found) pos - 1
      else if (!src.hasNext) -1
      else ffp(src, pos + 1, src.next() == pattern)
    ffp(in.sliding(pattern.size), 0)
  }

  val raf: RandomAccessFile = new RandomAccessFile("src/test/resources/sample_100.csv", "r")

  println(s"size in bytes = ${raf.length()}")


  def findParSplits(raf: RandomAccessFile, minSplitSize: Int, pattern: String): Seq[Long] = {
    val size = raf.length()
    val x = new Array[Byte](300)
    def findParSplits(position: Long, acc: Seq[Long]): Seq[Long] = {
      raf.seek(position)
      if(position >= size || position + minSplitSize >= size) size +: acc
      else {
        raf.read(x)
        val res = findFirst(pattern.getBytes, x.iterator)
        if(res == -1) findParSplits(position + x.size, acc)
        else {
          findParSplits(position + res + pattern.size + minSplitSize, (position + res + pattern.size) +: acc)
        }
      }
    }
    findParSplits(minSplitSize, Seq(0))
  }

  val partitionMarkers = findParSplits(raf, 1000, "\n").reverse
  println(partitionMarkers)

  partitionMarkers.sliding(2).foreach{ case from +: to +: Nil =>
    println("===============")
    println(s"$from - $to")
    raf.seek(from)
    val buf = new Array[Byte]((to-from).toInt)
    raf.read(buf)
    println(new String(buf))
  }

}