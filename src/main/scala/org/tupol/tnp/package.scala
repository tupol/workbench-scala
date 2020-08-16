package org.tupol

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{ Files, Path, StandardOpenOption }

import org.tupol.utils.Bracket

import scala.util.Random

package object tnp {

  val KB = 1024
  val MB = 1024 * KB
  val GB = 1024 * MB

  def fileSize(path: Path) = Bracket.auto(FileChannel.open(path, StandardOpenOption.READ))(_.size()).get

  def defaultSplitSize(fileSize: Long) = math.max((fileSize / 10.0).toLong, 50 * KB)

  def defaultMergeFile(targetDir: Path) = targetDir.resolve(targetDir.getFileName.toString)

  def defaultTargetDir(sourcePath: Path): Path = Files.createTempDirectory(sourcePath.getFileName.toString + ".")

  def deleteAll(root: Path): Unit = {
    import scala.collection.JavaConverters._
    if (Files.isDirectory(root)) {
      Files.list(root).iterator().asScala.foreach(deleteAll)
      Files.deleteIfExists(root)
    } else {
      Files.deleteIfExists(root)
    }
  }

  def generateTextFile(path: Path, size: Long): Path = {
    val bbsize = 100 * KB
    val buffer = ByteBuffer.allocate(bbsize)

    val randomString = new String((0 until (bbsize - 1)).map(_ => Random.nextPrintableChar()).toArray)
    buffer.put((randomString + "\n").getBytes)

    val reminder = (size % bbsize).toInt
    val parts = (size / bbsize).toInt
    val lastBuf = ByteBuffer.wrap(buffer.array().take(reminder))
    Bracket
      .auto(FileChannel.open(path, StandardOpenOption.WRITE)) { file =>
        (0 until parts).foreach { _ => buffer.rewind(); file.write(buffer) }
        if (reminder != 0) file.write(lastBuf)
      }.get
    path
  }
}
