package org.tupol.tnp

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{ Files, Path, Paths, StandardOpenOption }

import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }
import org.tupol.utils.Bracket

import scala.util.{ Failure, Success }

class MaxPartSizeFinderSpec extends WordSpec with Matchers with BeforeAndAfterEach {

  private var _file: Path = _

  override def beforeEach(): Unit = _file = Files.createTempFile(getClass.getSimpleName, "")

  override def afterEach(): Unit = Files.deleteIfExists(_file)

  "MaxPartSizeFinder" should {
    "fail" when {
      "the file is empty" in {
        val path = utils.writeStringToFile(_file, "")
        val params = PartitioningParams("\n".getBytes, 1, 1)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe a[Failure[_]]
      }
      "the delimiter can not be found in maxSearchSize" in {
        val text = "12345abcde|"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("X".getBytes, 5, 2, Some(2))
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe a[Failure[_]]
      }
      "the delimiter can not be found in maxSearchSize in the second partition" in {
        val text = "1234|abcdefgh"
        val path = utils.writeStringToFile(_file, text)
        val fSize = fileSize(path)
        val params = PartitioningParams("|".getBytes, 5, 1, Some(3))
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe a[Failure[_]]
      }
    }
    "return a single partition" when {
      "the partition size is equal to the file size" in {
        val text = "12345\nabcde"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("\n".getBytes, 11, 7)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 11))
      }
      "the partition size is larger than the file size" in {
        val text = "12345\nabcde"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("\n".getBytes, 12, 7)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 11))
      }
      "the file does not contain the delimiter" in {
        val text = "12345\nabcde"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("X".getBytes, 6, 5)
        val result = new MaxPartSizeFinder(params).find(path)
        result.get
        result shouldBe Success(Seq(0, 11))
      }
    }
    "return two partitions" when {
      "the delimiter is found in the middle" in {
        val text = "12345\nabcde"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("\n".getBytes, 7, 5)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 6, 11))
      }
      "the long delimiter is found in the middle" in {
        val text = "12345XYZ67890"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("XYZ".getBytes, 8, 5)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 8, 13))
      }
      "the delimiter is found in the middle and at the end" in {
        val text = "12345\nabcde\n"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("\n".getBytes, 10, 7)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 6, 12))
      }
      "having the split close to maxSplitSize" in {
        val text = "12345\nabcde\n"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("\n".getBytes, 6, 4)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 6, 12))
      }
    }
    "return three partitions" in {
      val maxSplitByteSize = 6
      val text = "12345\nabcde\n67890\n"
      val path = utils.writeStringToFile(_file, text)
      val params = PartitioningParams("\n".getBytes, maxSplitByteSize, 3)
      val result = new MaxPartSizeFinder(params).find(path)
      result shouldBe Success(Seq(0, 6, 12, 18))
      result.get.sliding(2).foreach { case (sidx +: eidx +: Nil) => (eidx - sidx) <= maxSplitByteSize shouldBe true }
    }
  }

}
