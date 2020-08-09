package org.tupol.tnp

import java.io.{FileInputStream, FileOutputStream, RandomAccessFile}
import java.nio.file.{Files, Path}

import org.tupol.utils.Bracket
import org.tupol.utils.implicits._

import scala.annotation.tailrec
import scala.util.Try

object FilePartitioner {

  def partitionFile(from: Path, toDir: Path, partitionMarkers: Seq[Long]): Try[Traversable[Path]] =
    Bracket.auto(new FileInputStream(from.toFile).getChannel()) { f =>
      Try(Files.createDirectory(toDir))
      partitionMarkers.sliding(2).zipWithIndex.map { case (sidx +: eidx +: Nil, idx) =>
        val size = eidx - sidx
        val targetPath = Files.createFile(toDir.resolve(f"${from.getFileName.toString}.p$idx%04d"))
        Bracket.auto(new FileOutputStream(targetPath.toFile).getChannel) { t =>
          t.transferFrom(f, 0, size)
          targetPath
        }
      }.toSeq.allOkOrFail
    }.flatten

  def mergeFiles(from: Seq[Path], to: Path): Try[Traversable[Long]] = {
    Bracket.auto(new FileOutputStream(to.toFile).getChannel()) { t =>
      from.map { f =>
        Bracket.auto(new FileInputStream(f.toFile).getChannel()) { s =>
          // println(s"Merging from $f to $to")
          t.transferFrom(s, t.size(), s.size())
        }
      }.allOkOrFail
    }.flatten
  }

  /** Find the indices for split in the given file, such as the split will be done after at least
   * `minSplitSize` and including the given split pattern */
  def findParSplits(raf: RandomAccessFile, minSplitSize: Int, pattern: String): Seq[Long] = {
    val size = raf.length()
    val x = new Array[Byte](300)

    def findParSplits(position: Long, acc: Seq[Long]): Seq[Long] = {
      raf.seek(position)
      if (position >= size || position + minSplitSize >= size) size +: acc
      else {
        raf.read(x)
        val res = findFirst(pattern.getBytes, x.iterator)
        if (res == -1) findParSplits(position + x.size, acc)
        else {
          findParSplits(position + res + pattern.size + minSplitSize, (position + res + pattern.size) +: acc)
        }
      }
    }

    findParSplits(minSplitSize, Seq(0))
  }

  /** Returns the position of the first character of the pattern in the input
   * or -1 if pattern was not found */
  def findFirst[T](pattern: Seq[T], in: Iterator[T]): Int = {
    @tailrec
    def ffp(src: in.GroupedIterator[T], pos: Int, found: Boolean = false): Int =
      if (found) pos - 1
      else if (!src.hasNext) -1
      else ffp(src, pos + 1, src.next() == pattern)

    ffp(in.sliding(pattern.size), 0)
  }

}
