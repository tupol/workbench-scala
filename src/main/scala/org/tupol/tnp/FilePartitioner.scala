package org.tupol.tnp

import java.io.{FileInputStream, FileOutputStream, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}

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
    Bracket.auto(FileChannel.open(from, StandardOpenOption.READ)) { f =>
      for {
        _ <- Try(Files.createDirectory(toDir))
        results <- partitionMarkers.sliding(2).zipWithIndex.map { case (sidx +: eidx +: Nil, idx) =>
          val size = eidx - sidx
          val targetPath = partitionPath(from, toDir, idx)
          Bracket.auto(FileChannel.open(targetPath, StandardOpenOption.WRITE)) { t =>
            val txr = t.transferFrom(f, 0, size)
            // println(s"Transfer from $sidx to $eidx ($size) to $targetPath ($txr)")
            (targetPath, txr)
          }
        }.toSeq.allOkOrFail
      } yield results
    }.flatten

  def partitionPath(from: Path, toDir: Path, part: Int): Path =
    Files.createFile(toDir.resolve(f"${from.getFileName.toString}.p$part%04d"))

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

  def findParSplitsFwd(path: Path, pattern: String): Try[Seq[Long]] = {
    Bracket.auto(new RandomAccessFile(path.toFile, "r")) { raf =>
      val minSplitSizeBytes = defaultSplitSize(raf.length())
      println(s"minSplitSizeBytes = $minSplitSizeBytes")
      findParSplitsFwd(path, minSplitSizeBytes, pattern)
    }.flatten
  }
  def findParSplitsRev(path: Path, pattern: String): Try[Seq[Long]] = {
    Bracket.auto(new RandomAccessFile(path.toFile, "r")) { raf =>
      val maxSplitSizeBytes = defaultSplitSize(raf.length())
      println(s"maxSplitSizeBytes = $maxSplitSizeBytes")
      findParSplitsRev(raf, maxSplitSizeBytes, pattern)
    }
  }
  /** Find the indices for split in the given file, such as the split will be done after at least
   * `minSplitSize` and including the given split pattern */
  def findParSplitsFwd(path: Path, minSplitSizeBytes: Long, pattern: String): Try[Seq[Long]] =
    Bracket.auto(new RandomAccessFile(path.toFile, "r")) { raf =>
    val size = raf.length()
    val seekBuff = new Array[Byte](1 * KB)

    def findParSplits(position: Long, acc: Seq[Long]): Seq[Long] = {
      raf.seek(position)
      if (position >= size || position + minSplitSizeBytes >= size) size +: acc
      else {
        raf.read(seekBuff)
        findFirst(pattern.getBytes, seekBuff.iterator) match {
          case None => findParSplits(position + seekBuff.size, acc)
          case Some(res) =>
            val marker = position + res + pattern.size
            findParSplits(marker + minSplitSizeBytes, marker +: acc)
        }
      }
    }
    findParSplits(minSplitSizeBytes, Seq(0))
  }

  /** Find the indices for split in the given file, such as the split will be done after at most
   * `maxSplitSize` and including the given split pattern */
  def findParSplitsRev(raf: RandomAccessFile, maxSplitSizeBytes: Long, pattern: String): Seq[Long] = {
    val size = raf.length()
    val seekBuff = new Array[Byte](1 * KB)

    def findParSplits(position: Long, acc: Seq[Long]): Seq[Long] = {
      val start = position - seekBuff.size
      println(s"start from $start")
      raf.seek(start)
      if (start >= size || position + maxSplitSizeBytes >= size) {
        println(s"start >= size = ${start >= size}")
        println(s"position + maxSplitSizeBytes >= size = ${position + maxSplitSizeBytes >= size}")
        size +: acc
      }
      else {
        raf.read(seekBuff)
        findFirst(pattern.getBytes, seekBuff.iterator) match {
          case None => findParSplits(start, acc)
          case Some(res) =>
            val marker = start + res + pattern.size
            println(s"  marker = $marker")
            findParSplits(marker + maxSplitSizeBytes, marker +: acc)
        }
      }
    }
    findParSplits(maxSplitSizeBytes, Seq(0))
  }

  /** Returns the position of the first character of the pattern in the input
   * or -1 if pattern was not found */
  def findFirst[T](pattern: Seq[T], in: Iterator[T]): Option[Long] = {
    @tailrec
    def ffp(src: in.GroupedIterator[T], pos: Long, found: Boolean = false): Option[Long] =
      if (found) Some(pos - 1)
      else if (!src.hasNext) None
      else ffp(src, pos + 1, src.next() == pattern)
    if(pattern.isEmpty || in.isEmpty) None
    else ffp(in.sliding(pattern.size), 0)
  }

  def defaultSplitSize(fileSize: Long) = math.max((fileSize / 10.0).toLong,  10 * MB)

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
