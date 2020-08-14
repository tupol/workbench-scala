package org.tupol.tnp

import java.nio.file.{ Files, Path, Paths }

import org.tupol.utils.timeCode

import scala.concurrent.duration.{ Duration, MILLISECONDS }
import scala.concurrent.{ duration, Await, Future }

object FilePartitionerApp extends App {

  import FilePartitioner._

  val sourcePath    = Paths.get("src/test/resources/sample_10k.csv")
  val targetFolder1 = Paths.get(s"/tmp/test1")
  val targetFolder2 = Paths.get(s"/tmp/test2")

  val delimiter = "\n".getBytes()

  val partitioner1 = minPartitionSizePartitioner(delimiter, 1 * KB)
  val partitioner2 = maxPartitionSizePartitioner(delimiter, 1 * KB)

  partmerge(partitioner1, sourcePath, targetFolder1)
  partmerge(partitioner2, sourcePath, targetFolder2)

  def partmerge(partitioner: Partitioner, from: Path, toDir: Path) = {
    println("============================================================")
    println(s"input file:        $from")
    println(s"partitions folder: $toDir")
    val targetFile = defaultMergeFile(toDir)
    println(s"merged file:       $targetFile")
    deleteRec(toDir)

    val (partFiles, rt1) = timeCode(partitioner.partition(from, toDir))
    println("partitionFile: " + duration.FiniteDuration(rt1, MILLISECONDS))


    val (_, rt2) = timeCode(mergeFiles(partFiles.get.map(_._1).toSeq, targetFile).get)
    println("mergeFiles:    " + duration.FiniteDuration(rt2, MILLISECONDS))

    val (_, rt3) = timeCode(Files.copy(from, toDir.resolve(s"original-${from.getFileName}")))
    println("copyFiles:     " + duration.FiniteDuration(rt3, MILLISECONDS))

    println("============================================================")

  }

}
