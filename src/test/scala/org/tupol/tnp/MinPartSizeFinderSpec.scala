package org.tupol.tnp

import java.nio.file.{ Files, Path, Paths }

import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }

import scala.util.{ Failure, Success }

class MinPartSizeFinderSpec extends WordSpec with Matchers with BeforeAndAfterEach {

  private var _file: Path = _

  override def beforeEach(): Unit = _file = Files.createTempFile(getClass.getSimpleName, "")

  override def afterEach(): Unit = Files.deleteIfExists(_file)

  "MinPartSizeFinder" should {
    "fail" when {
      "the file is empty" in {
        val path = utils.writeStringToFile(_file, "")
        val params = PartitioningParams("\n".getBytes, 1, 1)
        val result = new MinPartSizeFinder(params).find(path)
        result shouldBe a[Failure[_]]
      }
      "the delimiter can not be found in maxSearchSize" in {
        val text = "12345abcde|"
        val path = utils.writeStringToFile(_file, text)
        val fSize = fileSize(path)
        val params = PartitioningParams("|".getBytes, 3, 1, Some(3))
        val result = new MinPartSizeFinder(params).find(path)
        result shouldBe a[Failure[_]]
      }
      "the delimiter can not be found in maxSearchSize in the second partition" in {
        val text = "1234|abcdefgh"
        val path = utils.writeStringToFile(_file, text)
        val fSize = fileSize(path)
        val params = PartitioningParams("|".getBytes, 3, 1, Some(3))
        val result = new MinPartSizeFinder(params).find(path)
        result shouldBe a[Failure[_]]
      }
    }
    "return a single partition" when {
      "the partition size is equal to the file size" in {
        val text = "12345\nabcde"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("\n".getBytes, text.size, text.size / 4)
        val result = new MinPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, text.size))
      }
      "the partition size is larger than the file size" in {
        val text = "12345\nabcde"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("\n".getBytes, text.size + 1, text.size / 4)
        val result = new MinPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, text.size))
      }
      "the file does not contain the delimiter" in {
        val text = "12345\nabcde"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("X".getBytes, text.size / 2, text.size / 4)
        val result = new MinPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, text.size))
      }
    }
    "return one partition" when {
      "the delimiter is only found at the end" in {
        val text = "12345abcde\n"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("\n".getBytes, 5, 3)
        val result = new MinPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, text.size))
      }
    }
    "return two partitions" when {
      "the delimiter is found in the middle" in {
        val text = "12345\nabcde"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("\n".getBytes, 5, 3)
        val result = new MinPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 6, 11))
      }
      "the delimiter is found in the middle and at the end" in {
        val text = "12345\nabcde\n"
        val path = utils.writeStringToFile(_file, text)
        val params = PartitioningParams("\n".getBytes, 5, 2)
        val result = new MinPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 6, 12))
      }
    }
    "return three partitions" in {
      val minSplitByteSize = 5
      val text = "12345\nabcde\n67890\n"
      val path = utils.writeStringToFile(_file, text)
      val params = PartitioningParams("\n".getBytes, 5, 2)
      val result = new MinPartSizeFinder(params).find(path)
      result shouldBe Success(Seq(0, 6, 12, 18))
      result.get.sliding(2).foreach { case (sidx +: eidx +: Nil) => (eidx - sidx) >= minSplitByteSize shouldBe true }
    }
  }

}
