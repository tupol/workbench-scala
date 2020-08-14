package org.tupol.tnp

import java.io.{ FileInputStream, FileOutputStream }
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{ Files, Path, StandardOpenOption }
import java.util.UUID

import org.tupol.utils.Bracket
import org.tupol.utils.implicits._

import scala.annotation.tailrec
import scala.util.Try

object FilePartitioner {

  val KB = 1024
  val MB = 1024 * KB
  val GB = 1024 * MB

  /**
   * @param config
   * @param upperPartitionLimit when true, the partition size will never exceed config.splitSizeLimit
   */
  class Partitioner(val config: FilePartitioner.PartitioningParams, upperPartitionLimit: Boolean = true) {

    def partition(from: Path, toDir: Path): Try[Traversable[(Path, Long)]] =
      Bracket
        .auto(FileChannel.open(from, StandardOpenOption.READ)) { file =>
          for {
            partitionMarkers <- if (upperPartitionLimit)
                                 findRevPartitionsInFile(file, config)
                               else
                                 findFwdPartitionsInFile(file, config)
            partitionFiles <- partitionFile(file, toDir, partitionMarkers)
          } yield partitionFiles
        }
        .flatten
  }

  def maxPartitionSizePartitioner(delimiter: Seq[Byte], partitionSize: Long) =
    new Partitioner(PartitioningParams(delimiter, splitSizeLimit = partitionSize), upperPartitionLimit = true)

  def minPartitionSizePartitioner(delimiter: Seq[Byte], partitionSize: Long) =
    new Partitioner(PartitioningParams(delimiter, splitSizeLimit = partitionSize), upperPartitionLimit = false)

  /**
   * File Partitioning parameters
   * @param delimiter The sequence of bytes that separate the partitions
   * @param splitSizeLimit The partition size limit in bytes. Depending on the strategy it can be a lower or an upper limit.
   * @param seekBufferSize The maximum search buffer size
   * @param maxSearchSize The maximum search size
   */
  case class PartitioningParams(
    delimiter: Seq[Byte],
    splitSizeLimit: Long,
    seekBufferSize: Int = 1 * KB,
    maxSearchSize: Option[Long] = None
  ) {
    require(!delimiter.isEmpty, "The delimiter can not be empty")
    require(seekBufferSize > 0, s"The seekBufferSize is $seekBufferSize but it must be a positive number larger than 0")
    require(
      splitSizeLimit >= seekBufferSize,
      s"The splitSizeLimit is $splitSizeLimit and it must be equal or greater than seekBufferSize that is $seekBufferSize"
    )
    maxSearchSize.map(
      mss =>
        require(
          mss >= seekBufferSize,
          s"The maxSearchSize is $mss and it must be equal or greater than seekBufferSize that is $seekBufferSize"
        )
    )
  }

  def partitionFile(from: Path, toDir: Path, partitionMarkers: Seq[Long]): Try[Traversable[(Path, Long)]] =
    Bracket
      .auto(FileChannel.open(from, StandardOpenOption.READ)) { file =>
        partitionFile(file, toDir, partitionMarkers, from.getFileName.toString)
      }
      .flatten

  def partitionFile(
    file: FileChannel,
    toDir: Path,
    partitionMarkers: Seq[Long],
    name: String = UUID.randomUUID().toString
  ): Try[Traversable[(Path, Long)]] =
    for {
      _ <- Try(Files.createDirectory(toDir))
      results <- partitionMarkers
                  .sliding(2)
                  .zipWithIndex
                  .map {
                    case (start +: end +: Nil, part) =>
                      val size       = end - start
                      val targetPath = createPartitionPath(name, toDir, part)
                      Bracket.auto(FileChannel.open(targetPath, StandardOpenOption.WRITE)) { t =>
                        val txr = t.transferFrom(file, 0, size)
                        // println(s"Transfer from $start to $end ($size) to $targetPath ($txr)")
                        (targetPath, txr)
                      }
                  }
                  .toSeq
                  .allOkOrFail
    } yield results

  def createPartitionPath(from: Path, toDir: Path, part: Int): Path =
    createPartitionPath(from.getFileName.toString, toDir, part)

  def createPartitionPath(name: String, toDir: Path, part: Int): Path =
    Files.createFile(toDir.resolve(f"$name.p$part%04d"))

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
        val splitSizeLimit = defaultSplitSize(file.size())
//        println(s"splitSizeLimit = $splitSizeLimit")
        findFwdPartitionsInPath(path, PartitioningParams(delimiter, splitSizeLimit))
      }
      .flatten
  def findParSplitsRev(path: Path, delimiter: Seq[Byte]): Try[Seq[Long]] =
    Bracket
      .auto(FileChannel.open(path, StandardOpenOption.READ)) { file =>
        val splitSizeLimit = defaultSplitSize(file.size())
//        println(s"splitSizeLimit = $splitSizeLimit")
        findRevPartitionsInPath(path, PartitioningParams(delimiter, splitSizeLimit))
      }
      .flatten

  /**
   * Find the indices for split in the given file, such as the split will be done after at least
   * `splitSizeLimit` and including the given split delimiter
   */
  def findFwdPartitionsInPath(path: Path, params: PartitioningParams): Try[Seq[Long]] =
    Bracket
      .auto(FileChannel.open(path, StandardOpenOption.READ)) { file =>
        findFwdPartitionsInFile(file, params)
      }
      .flatten

  def findFwdPartitionsInFile(file: FileChannel, params: PartitioningParams): Try[Seq[Long]] = Try {
    import params._
    val size = file.size()
    require(size > 0, s"Can not partition an empty file")
    val seekBuff = ByteBuffer.allocate(seekBufferSize)

    def findParSplits(cursor: Long, acc: Seq[Long]): Seq[Long] = {
      if (maxSearchSize.isDefined && math.abs(acc.last - maxSearchSize.get) >= 0)
        throw new Exception(s"Unable to find the delimiter in ($splitSizeLimit) bytes starting from ${acc.last}")
      if (acc.last == size) acc
      else if (cursor >= size) acc :+ size
      else {
        file.position(cursor)
        file.read(seekBuff)
        findFirst(delimiter, seekBuff.array.iterator) match {
          case None => findParSplits(cursor + seekBufferSize, acc)
          case Some(res) =>
            val marker = cursor + res + delimiter.size
            findParSplits(marker + splitSizeLimit, acc :+ marker)
        }
      }
    }
    findParSplits(splitSizeLimit, IndexedSeq(0))
  }

  /**
   * Find the indices for split in the given file, such as the split will be done after at most
   * `splitSizeLimit` and including the given split delimiter
   */
  def findRevPartitionsInPath(path: Path, params: PartitioningParams): Try[Seq[Long]] =
    Bracket
      .auto(FileChannel.open(path, StandardOpenOption.READ)) { file =>
        findRevPartitionsInFile(file, params)
      }
      .flatten

  /**
   * Find the indices for split in the given file, such as the split will be done after at most
   * `splitSizeLimit` and including the given split delimiter
   */
  def findRevPartitionsInFile(file: FileChannel, params: PartitioningParams): Try[Seq[Long]] = Try {
    import params._
    val size = file.size()
    require(size > 0, s"Can not partition an empty file")
    val seekBuff     = ByteBuffer.allocate(seekBufferSize)
    val delimiterRev = delimiter.reverse
    def findParSplits(cursor: Long, acc: Seq[Long], attempt: Int = 1): Seq[Long] = {
      val seek = cursor - attempt * seekBufferSize

//      println(s"cursor=$cursor; seek=$seek; attempt=$attempt; acc=$acc; maxSearchSize=$maxSearchSize")

      if (maxSearchSize.isDefined && math.abs(acc.last + maxSearchSize.get) < cursor)
        throw new Exception(
          s"Unable to find the delimiter in ($splitSizeLimit) bytes going backward, starting from ${acc.last}"
        )
      if (seek < acc.last)
        // We didn't find the delimiter reaching the previous marker, so move the cursor ahead
        findParSplits(cursor + splitSizeLimit, acc)
      else if (cursor >= size)
        // The cursor has reached the end so we have a result
        acc :+ size
      else {
        file.position(seek)
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

  /**
   * Returns the position of the first character of the delimiter in the input
   * or -1 if delimiter was not found
   */
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
