package org.tupol.tnp

import java.nio.file.Paths

import org.scalatest.{ Matchers, WordSpec }

import scala.util.{ Failure, Success }

class MaxPartSizeFinderSpec extends WordSpec with Matchers {

  "MaxPartSizeFinder" should {
    "fail" when {
      "the file is empty" in {
        val path = Paths.get("src/test/resources/test-file-00.txt")
        val params = PartitioningParams("\n".getBytes, 1, 1)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe a[Failure[_]]
      }
      "the delimiter can not be found in maxSearchSize" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val params = PartitioningParams("X".getBytes, fSize / 2, 2, Some(2))
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe a[Failure[_]]
      }
    }
    "return a single partition" when {
      "the partition size is equal to the file size" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val params = PartitioningParams("\n".getBytes, fSize, 5)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, fSize))
      }
      "the partition size is larger than the file size" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val params = PartitioningParams("\n".getBytes, fSize + 1, 5)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, fSize))
      }
      "the file does not contain the delimiter" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val params = PartitioningParams("X".getBytes, fSize / 2, 5)
        val result = new MaxPartSizeFinder(params).find(path)
        result.get
        result shouldBe Success(Seq(0, fSize))
      }
    }
    "return two partitions" when {
      "the delimiter is found in the middle" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val params = PartitioningParams("\n".getBytes, 13, 5)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 11, 21))
      }
      "the long delimiter is found in the middle" in {
        val path = Paths.get("src/test/resources/test-file-01-XYZ.txt")
        val params = PartitioningParams("XYZ".getBytes, 13, 5)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 13, 23))
      }
      "the delimiter is found in the middle and at the end" in {
        val path = Paths.get("src/test/resources/test-file-01-ends-in-delimiter.txt")
        val params = PartitioningParams("\n".getBytes, 12, 5)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 11, 22))
      }
      "having the split close to maxSplitSuze" in {
        val path = Paths.get("src/test/resources/test-file-02.txt")
        val fSize = fileSize(path)
        val params = PartitioningParams("\n".getBytes, 50, (fSize / 2).toInt)
        val result = new MaxPartSizeFinder(params).find(path)
        result shouldBe Success(Seq(0, 50, 90))
      }
    }
    "return three partitions" in {
      val maxSplitByteSize = 30
      val path = Paths.get("src/test/resources/test-file-02.txt")
      val params = PartitioningParams("\n".getBytes, maxSplitByteSize, 9)
      val result = new MaxPartSizeFinder(params).find(path)
      result shouldBe Success(Seq(0, 30, 60, 90))
      result.get.sliding(2).foreach { case (sidx +: eidx +: Nil) => (eidx - sidx) <= maxSplitByteSize shouldBe true }
    }
  }
}
