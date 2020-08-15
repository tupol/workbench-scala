package org.tupol.tnp

import java.io.{ FileInputStream, FileOutputStream }
import java.nio.channels.FileChannel
import java.nio.file.{ Files, Path, StandardOpenOption }
import java.util.UUID

import org.tupol.utils.Bracket
import org.tupol.utils.implicits._

import scala.util.Try

class FilePartitioner(partitionFinder: PartitionFinder) {
  import FilePartitioner.PartInfo
  def partition(from: Path, toDir: Path): Try[Traversable[PartInfo]] =
    Bracket
      .auto(FileChannel.open(from, StandardOpenOption.READ)) { file =>
        for {
          partitionMarkers <- partitionFinder.find(file)
          partitionFiles <- FilePartitioner.partitionFile(file, toDir, partitionMarkers)
        } yield partitionFiles
      }
      .flatten
}

object FilePartitioner {

  /** Path and size of a partition */
  type PartInfo = (Path, Long)

  def partitionFile(
    from: Path,
    toDir: Path,
    partitionMarkers: Seq[Long]): Try[Traversable[PartInfo]] =
    Bracket
      .auto(FileChannel.open(from, StandardOpenOption.READ)) { file =>
        partitionFile(file, toDir, partitionMarkers, from.getFileName.toString)
      }.flatten

  def partitionFile(
    file: FileChannel,
    toDir: Path,
    partitionMarkers: Seq[Long],
    name: String = UUID.randomUUID().toString): Try[Traversable[PartInfo]] =
    for {
      _ <- Try(Files.createDirectory(toDir))
      results <- partitionMarkers
        .sliding(2)
        .zipWithIndex
        .map {
          case (start +: end +: Nil, part) =>
            val size = end - start
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

  def createPartitionPath(name: String, toDir: Path, part: Int): Path =
    Files.createFile(toDir.resolve(f"$name.p$part%04d"))

  def createPartitionPath(from: Path, toDir: Path, part: Int): Path =
    createPartitionPath(from.getFileName.toString, toDir, part)

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

}
