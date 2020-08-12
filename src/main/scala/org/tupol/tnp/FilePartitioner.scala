package org.tupol.tnp

import java.io.{ FileInputStream, FileOutputStream, RandomAccessFile }
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{ Files, Path, StandardOpenOption }

import org.tupol.tnp.FilePartitioner.findFirst
import org.tupol.utils.Bracket
import org.tupol.utils.implicits._

import scala.annotation.tailrec
import scala.util.Try

object FilePartitioner {

  val KB = 1024
  val MB = 1024 * KB
  val GB = 1024 * MB

  def partitionFile(from: Path, toDir: Path, partitionMarkers: Seq[Long]): Try[Traversable[(Path, Long)]] =
    Bracket
      .auto(FileChannel.open(from, StandardOpenOption.READ)) { f =>
        for {
          _ <- Try(Files.createDirectory(toDir))
          results <- partitionMarkers
                      .sliding(2)
                      .zipWithIndex
                      .map {
                        case (sidx +: eidx +: Nil, idx) =>
                          val size       = eidx - sidx
                          val targetPath = partitionPath(from, toDir, idx)
                          Bracket.auto(FileChannel.open(targetPath, StandardOpenOption.WRITE)) { t =>
                            val txr = t.transferFrom(f, 0, size)
                            // println(s"Transfer from $sidx to $eidx ($size) to $targetPath ($txr)")
                            (targetPath, txr)
                          }
                      }
                      .toSeq
                      .allOkOrFail
        } yield results
      }
      .flatten

  def partitionPath(from: Path, toDir: Path, part: Int): Path =
    Files.createFile(toDir.resolve(f"${from.getFileName.toString}.p$part%04d"))

  def mergeFiles(from: Seq[Path], to: Path): Try[Traversable[Long]] =
    Bracket
      .auto(new FileOutputStream(to.toFile).getChannel()) { t =>
        from.map { f =>
          Bracket.auto(new FileInputStream(f.toFile).getChannel()) { s =>
            // println(s"Merging from $f to $to")
            t.transferFrom(s, t.size(), s.size())
          }
        }.allOkOrFail
      }
      .flatten

  def findParSplitsFwd(path: Path, delimiter: Seq[Byte]): Try[Seq[Long]] =
    Bracket
      .auto(FileChannel.open(path, StandardOpenOption.READ)) { file =>
        val minSplitSizeBytes = defaultSplitSize(file.size())
        println(s"minSplitSizeBytes = $minSplitSizeBytes")
        findFwdPartitionsInPath(path, minSplitSizeBytes, delimiter)
      }
      .flatten
  def findParSplitsRev(path: Path, delimiter: Seq[Byte]): Try[Seq[Long]] =
    Bracket
      .auto(FileChannel.open(path, StandardOpenOption.READ)) { file =>
        val maxSplitSizeBytes = defaultSplitSize(file.size())
        println(s"maxSplitSizeBytes = $maxSplitSizeBytes")
        findRevPartitionsInPath(path, maxSplitSizeBytes, delimiter)
      }
      .flatten

  /** Find the indices for split in the given file, such as the split will be done after at least
   * `minSplitSize` and including the given split delimiter */
  def findFwdPartitionsInPath(
    path: Path,
    minSplitSizeBytes: Long,
    delimiter: Seq[Byte],
    seekBufferSize: Int = 1 * KB,
    maxSearchSize: Option[Long] = None
  ): Try[Seq[Long]] =
    Bracket
      .auto(FileChannel.open(path, StandardOpenOption.READ)) { file =>
        findFwdPartitionsInFile(file, minSplitSizeBytes, delimiter, seekBufferSize, maxSearchSize)
      }
      .flatten

  def findFwdPartitionsInFile(
    file: FileChannel,
    minSplitSizeBytes: Long,
    delimiter: Seq[Byte],
    seekBufferSize: Int = 1 * KB,
    maxSearchSize: Option[Long] = None
  ): Try[Seq[Long]] = Try {
    require(
      seekBufferSize < minSplitSizeBytes,
      s"seekBufferSize is $seekBufferSize must be smaller than maxSplitSizeBytes that is $minSplitSizeBytes"
    )
    val size           = file.size()
    val seekBuff       = ByteBuffer.allocate(seekBufferSize)

    def findParSplits(position: Long, acc: Seq[Long]): Seq[Long] = {
      if (maxSearchSize.isDefined && math.abs(acc.last - maxSearchSize.get) >= 0)
        throw new Exception(
          s"Unable to find the delimiter in ($minSplitSizeBytes) bytes starting from ${acc.last}"
        )
      if (acc.last == size) acc
      else if (position >= size) acc :+ size
      else {
        file.position(position)
        file.read(seekBuff)
        findFirst(delimiter, seekBuff.array.iterator) match {
          case None => findParSplits(position + seekBufferSize, acc)
          case Some(res) =>
            val marker = position + res + delimiter.size
            findParSplits(marker + minSplitSizeBytes, acc :+ marker)
        }
      }
    }
    findParSplits(minSplitSizeBytes, IndexedSeq(0))
  }

  /** Find the indices for split in the given file, such as the split will be done after at most
   * `maxSplitSize` and including the given split delimiter */
  def findRevPartitionsInPath(
    path: Path,
    maxSplitSizeBytes: Long,
    delimiter: Seq[Byte],
    seekBufferSize: Int = 1 * KB,
    maxSearchSize: Option[Long] = None
  ): Try[Seq[Long]] = {
    require(
      maxSplitSizeBytes > 0,
      s"maxSplitSizeBytes is $maxSplitSizeBytes but it must be a positive number larger than 0"
    )
    require(
      seekBufferSize <= maxSplitSizeBytes,
      s"seekBufferSize is $seekBufferSize must be smaller or equal to maxSplitSizeBytes that is $maxSplitSizeBytes"
    )
    Bracket
      .auto(FileChannel.open(path, StandardOpenOption.READ)) { file =>
        findRevPartitionsInFile(file, maxSplitSizeBytes, delimiter, seekBufferSize, maxSearchSize)
      }.flatten
  }

  /** Find the indices for split in the given file, such as the split will be done after at most
   * `maxSplitSize` and including the given split delimiter */
  def findRevPartitionsInFile(
    file: FileChannel,
    maxSplitSizeBytes: Long,
    delimiter: Seq[Byte],
    seekBufferSize: Int = 1 * KB,
    maxSearchSize: Option[Long] = None
  ): Try[Seq[Long]] = Try {
    require(
      maxSplitSizeBytes > 0,
      s"maxSplitSizeBytes is $maxSplitSizeBytes but it must be a positive number larger than 0"
    )
    require(
      seekBufferSize <= maxSplitSizeBytes,
      s"seekBufferSize is $seekBufferSize must be smaller or equal to maxSplitSizeBytes that is $maxSplitSizeBytes"
    )

    val size           = file.size()
    val seekBuff       = ByteBuffer.allocate(seekBufferSize)
    val delimiterRev = delimiter.reverse
    def findParSplits(position: Long, acc: Seq[Long]): Seq[Long] = {
      val start = position - seekBufferSize
      if (start < 0)
        throw new Exception(
          s"Unable to find the delimiter in ($maxSplitSizeBytes) bytes going backward, starting from $position"
        )
      if (maxSearchSize.isDefined && math.abs(acc.last - maxSearchSize.get) >= 0)
        throw new Exception(
          s"Unable to find the delimiter in ($maxSplitSizeBytes) bytes going backward, starting from ${acc.last}"
        )

      if (position >= size) acc :+ size
      else {
        file.position(start)
        file.read(seekBuff)
        findFirst(delimiterRev, seekBuff.array.reverseIterator) match {
          case None => findParSplits(start, acc)
          case Some(res) =>
            val marker = start + seekBufferSize - res
            findParSplits(marker + maxSplitSizeBytes, acc :+ marker)
        }
      }
    }
    findParSplits(maxSplitSizeBytes, IndexedSeq(0))
  }

  /** Returns the position of the first character of the delimiter in the input
   * or -1 if delimiter was not found */
  def findFirst[T](pattern: Seq[T], in: Iterator[T]): Option[Long] = {
    @tailrec
    def ffp(src: in.GroupedIterator[T], pos: Long, found: Boolean = false): Option[Long] =
      if (found) Some(pos - 1)
      else if (!src.hasNext) None
      else ffp(src, pos + 1, src.next() == pattern)
    if (pattern.isEmpty || in.isEmpty) None
    else ffp(in.sliding(pattern.size), 0)
  }

  def defaultSplitSize(fileSize: Long) = math.max((fileSize / 10.0).toLong, 50 * KB)

  def defaultMergeFile(targetDir: Path) = targetDir.resolve(targetDir.getFileName.toString)

  def defaultTargetDir(sourcePath: Path): Path = Files.createTempDirectory(sourcePath.getFileName.toString + ".")

  def deleteRec(root: Path): Unit = {
    import scala.collection.JavaConverters._
    if (Files.isDirectory(root)) {
      Files.list(root).iterator().asScala.foreach(deleteRec)
      Files.deleteIfExists(root)
    } else {
      Files.deleteIfExists(root)
    }
  }
}
