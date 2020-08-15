package org.tupol.tnp

import java.nio.file.{ Files, Path, Paths }

import org.tupol.tnp.FilesCopyVsCP.tempFilePrefix
import org.tupol.utils.timeCode

import scala.concurrent.duration.{ Duration, MILLISECONDS }
import scala.concurrent.{ Await, Future, duration }

object FilePartitionerApp extends App {

  import FilePartitioner._

  val sourcePath = Paths.get("src/test/resources/sample_1G.csv")
  //generateTextFile(Files.createTempFile(tempFilePrefix, ""), 1 * GB)
  val targetFolder1 = Paths.get(s"/tmp/test1")
  val targetFolder2 = Paths.get(s"/tmp/test2")

  val partitioningParams = PartitioningParams(delimiter = "\n".getBytes(), splitSizeLimit = 100 * MB, seekBufferSize = 100 * KB, maxSearchSize = Some(100 * KB))

  println("============================================================")
  partmerge(new MinPartSizeFinder(partitioningParams), sourcePath, targetFolder1)
  println("============================================================")
  partmerge(new MaxPartSizeFinder(partitioningParams), sourcePath, targetFolder2)
  println("============================================================")

  def partmerge(partitionFinder: PartitionFinder, from: Path, toDir: Path) = {
    val targetFile = defaultMergeFile(toDir)
    deleteAll(toDir)
    println(s"input file:        $from")
    println(s"partitions folder: $toDir")
    println(s"merged file:       $targetFile")

    val (partMarkers, rt1) = timeCode(partitionFinder.find(from))
    println("find partitions:   " + duration.FiniteDuration(rt1, MILLISECONDS))
    println("partitions found:  " + partMarkers.get.size)

    val (partFiles, rt2) = timeCode(FilePartitioner.partitionFile(from, toDir, partMarkers.get))
    println("partition files:   " + duration.FiniteDuration(rt2, MILLISECONDS))

    val (_, rt3) = timeCode(mergeFiles(partFiles.get.map(_._1).toSeq, targetFile).get)
    println("merge files:       " + duration.FiniteDuration(rt3, MILLISECONDS))

    // Interesting nugget: The Files.copy is using a FileSystemProvider, specific to the OS, however,
    // on MacOS it seems to be  2 - 4 times slower than the `cp` command
    val (_, rt4) = timeCode(Files.copy(from, toDir.resolve(s"original-${from.getFileName}")))
    println("copy files:        " + duration.FiniteDuration(rt4, MILLISECONDS))
  }

}
