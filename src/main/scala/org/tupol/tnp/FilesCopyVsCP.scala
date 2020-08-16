package org.tupol.tnp

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{ Files, Path, Paths, StandardCopyOption, StandardOpenOption }
import java.util.UUID

import org.tupol.utils._

import scala.concurrent.duration.{ FiniteDuration, MILLISECONDS }
import scala.util.Random

object FilesCopyVsCP extends App {

  val tempFilePrefix = "niocp_vs_osscp-"

  type CopyFunction = (Path, Path) => Unit

  def averageCopySpeed(copyFun: CopyFunction, from: Path, toDir: Path, samples: Int = 10): Throughput = {
    println(s"Copying $from to folder $toDir")
    val totalRuntimeMillis = (0 until samples).map { _ =>
      val to = Files.createTempFile(toDir, UUID.randomUUID().toString, "")
      val (_, t) = timeCode(copyFun(from, to));
      // println(s"Copied $from to $to in ${FiniteDuration(t, MILLISECONDS)}")
      Files.delete(to)
      t
    }.reduce(_ + _)
    Throughput(Files.size(from), totalRuntimeMillis / samples)
  }

  case class Throughput(bytes: Long, duration: FiniteDuration) {
    def KBps = bytes.toDouble / KB / (duration.toMillis / 1000.0)
    def MBps = bytes.toDouble / MB / (duration.toMillis / 1000.0)
    def GBps = bytes.toDouble / GB / (duration.toMillis / 1000.0)
  }

  /** OS specific copy function */
  val osscp: CopyFunction = (from: Path, to: Path) => { sys.runtime.exec(s"cp $from $to").waitFor(); Unit }
  /** Java NIO copy function */
  val niocp: CopyFunction = Files.copy(_, _, StandardCopyOption.REPLACE_EXISTING)

  val testFile = generateTextFile(Files.createTempFile(tempFilePrefix, ""), 100 * MB)
  println(s"Created $testFile of size ${Files.size(testFile)}")

  val osscpThroughput = averageCopySpeed(osscp, testFile, Files.createTempDirectory(tempFilePrefix))

  println(s"OS specific copy")
  println(s"  - ${osscpThroughput}")
  println(s"  - ${osscpThroughput.MBps} MB/s")

  val niocpThroughput = averageCopySpeed(niocp, testFile, Files.createTempDirectory(tempFilePrefix))

  println(s"Java NIO copy")
  println(s"  - ${niocpThroughput}")
  println(s"  - ${niocpThroughput.MBps} MB/s")

}
