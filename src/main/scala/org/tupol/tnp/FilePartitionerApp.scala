package org.tupol.tnp

import java.io.RandomAccessFile
import java.nio.file.{Files, Path, Paths}

import org.tupol.utils.timeCode

import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, Future, duration}

object FilePartitionerApp extends App {

  import FilePartitioner._

  val sourcePath = Paths.get("src/test/resources/sample_10k.csv")
//  val raf: RandomAccessFile = new RandomAccessFile(sourcePath.toFile, "r")
  val targetFolder = Paths.get(s"/tmp/test1")
  val targetFile = defaultMergeFile(targetFolder)
  println("============================================================")
  println(s"input file:             $sourcePath")
  println(s"output splits folder:   $targetFolder")
  println(s"output merged file:     $targetFile")
  println("============================================================")
  val (partitionMarkers, rt10) = timeCode(findParSplits(sourcePath, "\n").get.reverse)
  println("findParSplits: " + duration.FiniteDuration(rt10, MILLISECONDS))
  deleteRec(targetFolder)
  val (partFiles, rt11) = timeCode(partitionFile(sourcePath, targetFolder, partitionMarkers).get)
  println("partitionFile: " + duration.FiniteDuration(rt11, MILLISECONDS))
  val (_, rt12) = timeCode(mergeFiles(partFiles.map(_._1).toSeq, targetFile).get)
  println("mergeFiles:    " + duration.FiniteDuration(rt12, MILLISECONDS))
  val (_, rt20) = timeCode(Files.copy(sourcePath, targetFolder.resolve(s"original-${sourcePath.getFileName}")))
  println("copyFiles:     " + duration.FiniteDuration(rt20, MILLISECONDS))



}
