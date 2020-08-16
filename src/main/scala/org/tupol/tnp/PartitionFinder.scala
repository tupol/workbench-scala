package org.tupol.tnp

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{ Path, StandardOpenOption }

import org.tupol.tnp.PartitionFinder.findFirst
import org.tupol.utils.Bracket

import scala.annotation.tailrec
import scala.util.Try

trait PartitionFinder {
  def find(file: FileChannel): Try[Seq[Long]]

  def find(path: Path): Try[Seq[Long]] =
    Bracket
      .auto(FileChannel.open(path, StandardOpenOption.READ)) { file =>
        find(file)
      }
      .flatten
}

object PartitionFinder {

  /**
   * Returns the position of the first character of the delimiter in the input
   * or -1 if delimiter was not found
   */
  private[tnp] def findFirst[T](pattern: Seq[T], in: Iterator[T]): Option[Long] = {
    @tailrec
    def ffp(src: in.GroupedIterator[T], pos: Long, found: Boolean = false): Option[Long] =
      if (found) Some(pos - 1)
      else if (!src.hasNext) None
      else ffp(src, pos + 1, src.next() == pattern)

    if (pattern.isEmpty || in.isEmpty) None
    else ffp(in.sliding(pattern.size), 0)
  }

}

/**
 * Find the indices for split in the given file, such as the split will be done after at most
 * `splitSizeLimit` and including the given split delimiter
 */
class MaxPartSizeFinder(params: PartitioningParams) extends PartitionFinder {
  override def find(file: FileChannel): Try[Seq[Long]] = Try {
    import params._
    val size = file.size()
    require(size > 0, s"Can not partition an empty file")
    val seekBuff = ByteBuffer.allocate(seekBufferSize)
    val delimiterRev = delimiter.reverse

    def findParSplits(cursor: Long, acc: Seq[Long], attempt: Int = 1): Seq[Long] = {
      val seek = cursor - attempt * seekBufferSize
      // println(s"cursor=$cursor; seek=$seek; attempt=$attempt; acc=$acc; maxSearchSize=$maxSearchSize")
      if (maxSearchSize.isDefined && math.abs(attempt * seekBufferSize) > maxSearchSize.get)
        throw new Exception(
          s"Unable to find the delimiter in (${maxSearchSize.get}) bytes going backward, starting from $cursor")
      if (seek < acc.last)
        // We didn't find the delimiter reaching the previous marker, so move the cursor ahead
        findParSplits(cursor + splitSizeLimit, acc)
      else if (cursor >= size)
        // The cursor has reached the end so we have a result
        acc :+ size
      else {
        file.position(seek)
        seekBuff.clear()
        file.read(seekBuff)
        findFirst(delimiterRev, seekBuff.array.reverseIterator) match {
          case None => findParSplits(cursor, acc, attempt + 1)
          case Some(res) =>
            val marker = seek + seekBufferSize - res
            findParSplits(marker + splitSizeLimit, acc :+ marker)
        }
      }
    }
    findParSplits(splitSizeLimit, IndexedSeq(0))
  }
}

/**
 * Find the indices for split in the given file, such as the split will be done after at least
 * `splitSizeLimit` and including the given split delimiter
 */
class MinPartSizeFinder(params: PartitioningParams) extends PartitionFinder {
  override def find(file: FileChannel): Try[Seq[Long]] = Try {
    import params._
    val size = file.size()
    require(size > 0, s"Can not partition an empty file")
    val seekBuff = ByteBuffer.allocate(seekBufferSize)

    def findParSplits(cursor: Long, acc: Seq[Long], attempt: Int = 0): Seq[Long] = {
      val seek = cursor + attempt * seekBufferSize
      // println(s"cursor=$cursor; seek=$seek; attempt=$attempt; acc=$acc; maxSearchSize=$maxSearchSize")
      if (maxSearchSize.isDefined && (seek - cursor) > maxSearchSize.get)
        throw new Exception(s"Unable to find the delimiter in (${maxSearchSize.get}) bytes starting from $cursor")
      if (acc.last == size) acc
      else if (seek >= size) acc :+ size
      else {
        file.position(seek)
        seekBuff.clear()
        file.read(seekBuff)
        findFirst(delimiter, seekBuff.array.iterator) match {
          case None => findParSplits(cursor, acc, attempt + 1)
          case Some(res) =>
            val marker = seek + res + delimiter.size
            if (maxSearchSize.isDefined && (marker - cursor) > maxSearchSize.get)
              throw new Exception(s"Unable to find the delimiter in (${maxSearchSize.get}) bytes starting from $cursor")
            findParSplits(marker + splitSizeLimit, acc :+ marker, 0)
        }
      }
    }

    findParSplits(splitSizeLimit, IndexedSeq(0))
  }
}

