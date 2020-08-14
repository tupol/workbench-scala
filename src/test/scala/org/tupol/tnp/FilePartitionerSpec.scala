package org.tupol.tnp

import java.nio.channels.FileChannel
import java.nio.file.{ Files, Path, Paths, StandardOpenOption }

import org.scalatest.{ FunSuite, Matchers, WordSpec }
import org.tupol.tnp.FilePartitioner.PartitioningParams
import org.tupol.utils.Bracket

import scala.util.{ Failure, Success }

class FilePartitionerSpec extends WordSpec with Matchers {

  import FilePartitioner._

  "findFirst" should {
    "find None" when {
      "empty delimiter and empty input" in {
        findFirst(Seq.empty[Int], Iterator.empty) shouldBe None
      }
      "empty delimiter" in {
        findFirst(Seq.empty[Int], Iterator(1, 2, 3)) shouldBe None
      }
      "empty input" in {
        findFirst(Seq(1, 2, 3), Iterator.empty) shouldBe None
      }
      "delimiter is not in input" in {
        findFirst(Seq(4), Iterator(1, 2, 3)) shouldBe None
      }
    }
    "find Some simple delimiter" when {
      "delimiter equals input" in {
        findFirst(Seq(4), Iterator(4)) shouldBe Some(0)
      }
      "delimiter appears last in input" in {
        findFirst(Seq(2), Iterator(1, 2)) shouldBe Some(1)
      }
      "delimiter appears many times in input" in {
        findFirst(Seq(2), Iterator(1, 2, 1, 2, 1, 2, 1, 2)) shouldBe Some(1)
      }
    }
    "find Some long delimiter" when {
      "delimiter equals input" in {
        findFirst(Seq(1, 2), Iterator(1, 2)) shouldBe Some(0)
      }
      "delimiter appears last in input" in {
        findFirst(Seq(1, 2), Iterator(0, 1, 2)) shouldBe Some(1)
      }
      "delimiter appears many times in input" in {
        findFirst(Seq(1, 2), Iterator(0, 1, 2, 0, 1, 2, 0, 1, 2)) shouldBe Some(1)
      }
      "delimiter appears in input" in {
        findFirst(Seq(1, 2).reverse, Seq(0, 1, 2, 3).reverse.iterator) shouldBe Some(1)
      }
    }
  }
  "PartitioningParams" should {
    "fail when the delimiter is empty / not defined" in {
      an[IllegalArgumentException] shouldBe thrownBy(PartitioningParams(Seq.empty[Byte], 2, 2, Some(2)))
    }
    "fail when the seekBufferSize is smaller 0" in {
      an[IllegalArgumentException] shouldBe thrownBy(PartitioningParams("\n".getBytes, 2, -1, Some(2)))
    }
    "fail when the seekBufferSize is 0" in {
      an[IllegalArgumentException] shouldBe thrownBy(PartitioningParams("\n".getBytes, 2, 0, Some(2)))
    }
    "fail when the splitSizeLimit is smaller than the seekBufferSize" in {
      an[IllegalArgumentException] shouldBe thrownBy(PartitioningParams("\n".getBytes, 1, 2, Some(3)))
    }
    "fail when the maxSearchSize is smaller than the seekBufferSize" in {
      an[IllegalArgumentException] shouldBe thrownBy(PartitioningParams("\n".getBytes, 2, 2, Some(1)))
    }
  }
  "findFwdPartitionsInPath" should {
    "fail" when {
      "the file is empty" in {
        val path = Paths.get("src/test/resources/test-file-00.txt")
        val params = PartitioningParams("\n".getBytes, 1, 1)
        val result = findFwdPartitionsInPath(path, params)
        result shouldBe a[Failure[_]]
      }
      "the delimiter can not be found in maxSearchSize" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val params = PartitioningParams("X".getBytes, fSize / 2, 2, Some(3))
        val result = findFwdPartitionsInPath(path, params)
        result shouldBe a[Failure[_]]
      }
    }
    "return a single partition" when {
      "the partition size is equal to the file size" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val params = PartitioningParams("\n".getBytes, fSize, 5)
        val result = findFwdPartitionsInPath(path, params)
        result shouldBe Success(Seq(0, fSize))
      }
      "the partition size is larger than the file size" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val params = PartitioningParams("\n".getBytes, fSize + 1, 5)
        val result = findFwdPartitionsInPath(path, params)
        result shouldBe Success(Seq(0, fSize))
      }
      "the file does not contain the delimiter" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val params = PartitioningParams("X".getBytes, fSize / 2, 5)
        val result = findFwdPartitionsInPath(path, params)
        result shouldBe Success(Seq(0, fSize))
      }
    }
    "return one partition" when {
      "the delimiter is only found at the end" in {
        val path = Paths.get("src/test/resources/test-file-01-ends-in-delimiter.txt")
        val fSize = fileSize(path)
        val result = findFwdPartitionsInPath(path, PartitioningParams("\n".getBytes, fSize - 5, 5))
        result shouldBe Success(Seq(0, fSize))
      }
    }
    "return two partitions" when {
      "the delimiter is found in the middle" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val result = findFwdPartitionsInPath(path, PartitioningParams("\n".getBytes, 10, 5))
        result shouldBe Success(Seq(0, 11, 21))
      }
      "the delimiter is found in the middle and at the end" in {
        val path = Paths.get("src/test/resources/test-file-01-ends-in-delimiter.txt")
        val result = findFwdPartitionsInPath(path, PartitioningParams("\n".getBytes, 10, 5))
        result shouldBe Success(Seq(0, 11, 22))
      }
    }
    "return three partitions" in {
      val minSplitByteSize = 25
      val path = Paths.get("src/test/resources/test-file-02.txt")
      val result = findFwdPartitionsInPath(path, PartitioningParams("\n".getBytes, minSplitByteSize, 5))
      result shouldBe Success(Seq(0, 30, 60, 90))
      result.get.sliding(2).foreach { case (sidx +: eidx +: Nil) => (eidx - sidx) >= minSplitByteSize shouldBe true }
    }
  }

  "findRevPartitionsInPath" should {
    "fail" when {
      "the file is empty" in {
        val path = Paths.get("src/test/resources/test-file-00.txt")
        val result = findRevPartitionsInPath(path, PartitioningParams("\n".getBytes, 1, 1))
        result shouldBe a[Failure[_]]
      }
      "the delimiter can not be found in maxSearchSize" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val result = findRevPartitionsInPath(path, PartitioningParams("X".getBytes, fSize / 2, 2, Some(2)))
        result shouldBe a[Failure[_]]
      }
    }
    "return a single partition" when {
      "the partition size is equal to the file size" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val result = findRevPartitionsInPath(path, PartitioningParams("\n".getBytes, fSize, 5))
        result shouldBe Success(Seq(0, fSize))
      }
      "the partition size is larger than the file size" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val result = findRevPartitionsInPath(path, PartitioningParams("\n".getBytes, fSize + 1, 5))
        result shouldBe Success(Seq(0, fSize))
      }
      "the file does not contain the delimiter" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val fSize = fileSize(path)
        val result = findRevPartitionsInPath(path, PartitioningParams("X".getBytes, fSize / 2, 5))
        result.get
        result shouldBe Success(Seq(0, fSize))
      }
    }
    "return two partitions" when {
      "the delimiter is found in the middle" in {
        val path = Paths.get("src/test/resources/test-file-01.txt")
        val result = findRevPartitionsInPath(path, PartitioningParams("\n".getBytes, 13, 5))
        result shouldBe Success(Seq(0, 11, 21))
      }
      "the long delimiter is found in the middle" in {
        val path = Paths.get("src/test/resources/test-file-01-XYZ.txt")
        val result = findRevPartitionsInPath(path, PartitioningParams("XYZ".getBytes, 13, 5))
        result shouldBe Success(Seq(0, 13, 23))
      }
      "the delimiter is found in the middle and at the end" in {
        val path = Paths.get("src/test/resources/test-file-01-ends-in-delimiter.txt")
        val result = findRevPartitionsInPath(path, PartitioningParams("\n".getBytes, 12, 5))
        result shouldBe Success(Seq(0, 11, 22))
      }
      "having the split close to maxSplitSuze" in {
        val path = Paths.get("src/test/resources/test-file-02.txt")
        val fSize = fileSize(path)
        val result = findRevPartitionsInPath(path, PartitioningParams("\n".getBytes, 50, (fSize / 2).toInt))
        result shouldBe Success(Seq(0, 50, 90))
      }
    }
    "return three partitions" in {
      val maxSplitByteSize = 30
      val path = Paths.get("src/test/resources/test-file-02.txt")
      val result = findRevPartitionsInPath(path, PartitioningParams("\n".getBytes, maxSplitByteSize, 9))
      result shouldBe Success(Seq(0, 30, 60, 90))
      result.get.sliding(2).foreach { case (sidx +: eidx +: Nil) => (eidx - sidx) <= maxSplitByteSize shouldBe true }
    }
  }

  def fileSize(path: Path) = Bracket.auto(FileChannel.open(path, StandardOpenOption.READ))(_.size()).get

}
