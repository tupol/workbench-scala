package org.tupol.tnp

import java.nio.file.{ Files, Path, Paths }

import org.tupol.tnp.FilesCopyVsCP.{ tempFilePrefix, testFile }
import org.tupol.utils.timeCode

import scala.concurrent.duration.{ Duration, MILLISECONDS }
import scala.concurrent.{ Await, Future, duration }

object FilePartitionerApp extends App {

  import FilePartitioner._

  val sourcePath = generateTextFile(Files.createTempFile(tempFilePrefix, ""), 10 * MB)

  val targetFolder1 = Paths.get(s"/tmp/test1")
  val targetFolder2 = Paths.get(s"/tmp/test2")

  val partitioningParams = PartitioningParams(delimiter = "\n".getBytes(), splitSizeLimit = 1 * MB,
    seekBufferSize = 10 * KB, maxSearchSize = Some(200 * KB))

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
    println("find partitions:   " + rt1)
    println("partitions found:  " + partMarkers.get.size)

    val (partFiles, rt2) = timeCode(FilePartitioner.partitionFile(from, toDir, partMarkers.get))
    println("partition files:   " + rt2)

    val (_, rt3) = timeCode(mergeFiles(partFiles.get.map(_._1).toSeq, targetFile).get)
    println("merge files:       " + rt3)

    // Interesting nugget: The Files.copy is using a FileSystemProvider, specific to the OS, however,
    // on MacOS it seems to be  2 - 4 times slower than the `cp` command
    val (_, rt4) = timeCode(Files.copy(from, toDir.resolve(s"original-${from.getFileName}")))
    println("copy files:        " + rt4)
  }

}
