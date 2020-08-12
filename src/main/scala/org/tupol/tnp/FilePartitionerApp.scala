package org.tupol.tnp

import java.nio.file.{ Files, Path, Paths }

import org.tupol.tnp.FilePartitionerApp.partitionMarkers1
import org.tupol.utils.timeCode

import scala.concurrent.duration.{ Duration, MILLISECONDS }
import scala.concurrent.{ duration, Await, Future }

object FilePartitionerApp extends App {

  import FilePartitioner._

  val sourcePath    = Paths.get("src/test/resources/sample_10k.csv")
  val targetFolder1 = Paths.get(s"/tmp/test1")
  val targetFolder2 = Paths.get(s"/tmp/test2")

  val (partitionMarkers1, rt1) = timeCode(findParSplitsFwd(sourcePath, "\n".getBytes).get)
  println("findParSplits: " + duration.FiniteDuration(rt2, MILLISECONDS))

  val (partitionMarkers2, rt2) = timeCode(findParSplitsRev(sourcePath, "\n".getBytes).get)
  println("findParSplits: " + duration.FiniteDuration(rt1, MILLISECONDS))

  partmerge(sourcePath, targetFolder1, partitionMarkers1)
  partmerge(sourcePath, targetFolder2, partitionMarkers2)

  def partmerge(from: Path, toDir: Path, partitionMarkers: Seq[Long]) = {
    println("============================================================")
    println(s"input file:             $from")
    println(s"output splits folder:   $toDir")
    deleteRec(toDir)

    partitionMarkers.sliding(2).zipWithIndex.foreach {
      case (sidx +: eidx +: Nil, idx) =>
        println(f"$idx%2d | $sidx%9d - $eidx%9d | ${eidx - sidx}%9d < ${124885} ? ${(eidx - sidx) < 124885}")
    }

    val (partFiles, rt1) = timeCode(partitionFile(from, toDir, partitionMarkers).get)
    println("partitionFile: " + duration.FiniteDuration(rt1, MILLISECONDS))

    val targetFile = defaultMergeFile(toDir)
    println(s"output merged file:     $targetFile")
    val (_, rt2) = timeCode(mergeFiles(partFiles.map(_._1).toSeq, targetFile).get)
    println("mergeFiles:    " + duration.FiniteDuration(rt2, MILLISECONDS))
    val (_, rt3) = timeCode(Files.copy(from, toDir.resolve(s"original-${from.getFileName}")))
    println("copyFiles:     " + duration.FiniteDuration(rt3, MILLISECONDS))
    println("============================================================")
  }
}
