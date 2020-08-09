package org.tupol.tnp

import java.io.RandomAccessFile
import java.nio.file.{Files, Path, Paths}

import org.tupol.utils.timeCode

import scala.concurrent.duration
import scala.concurrent.duration.MILLISECONDS

object FilePartitionerApp extends App {

  import FilePartitioner._

  val sourcePath = Paths.get("src/test/resources/sample_10G.csv")
  val raf: RandomAccessFile = new RandomAccessFile(sourcePath.toFile, "r")
  val targetFolder = Paths.get(s"/tmp/test1")
  val targetFile = targetFolder.resolve("file_00")

  println("============================================================")
  deleteRec(targetFolder)
  val (partitionMarkers, rt10) = timeCode(findParSplits(raf, 1000000000, "\n").reverse)
  println("findParSplits: " + duration.FiniteDuration(rt10, MILLISECONDS))
  val (partFiles, rt11) = timeCode(partitionFile(sourcePath, targetFolder, partitionMarkers).get)
  println("partitionFile: " + duration.FiniteDuration(rt11, MILLISECONDS))
  val (_, rt12) = timeCode(mergeFiles(partFiles.toSeq, targetFile).get)
  println("mergeFiles:    " + duration.FiniteDuration(rt12, MILLISECONDS))
  val (_, rt20) = timeCode(Files.copy(sourcePath, targetFolder.resolve(s"original-${sourcePath.getFileName}")))
  println("copyFiles:     " + duration.FiniteDuration(rt20, MILLISECONDS))

  println("============================================================")


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
