package org.tupol.tnp

import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import org.scalatest.{FunSuite, Matchers, WordSpec}
import org.tupol.utils.Bracket

import scala.util.{Failure, Success}

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
  "findFwdPartitionsInPath" should {
    "fail when the delimiter can not be found in maxSearchSize" in {
        val path   = Paths.get("src/test/resources/test-file-01.txt")
        val fSize  = fileSize(path)
        val result = findFwdPartitionsInPath(path, fSize / 2, "X".getBytes, 5, Some(3))
      result shouldBe a[Failure[_]]
    }
    "return a single partition" when {
      "the file is empty" in {
        val path   = Paths.get("src/test/resources/test-file-00.txt")
        val result = findFwdPartitionsInPath(path, 1, "\n".getBytes)
      }
      "the partition size is equal to the file size" in {
        val path   = Paths.get("src/test/resources/test-file-01.txt")
        val fSize  = fileSize(path)
        val result = findFwdPartitionsInPath(path, fSize, "\n".getBytes, 5)
        result shouldBe Success(Seq(0, fSize))
      }
      "the partition size is larger than the file size" in {
        val path   = Paths.get("src/test/resources/test-file-01.txt")
        val fSize  = fileSize(path)
        val result = findFwdPartitionsInPath(path, fSize + 1, "\n".getBytes, 5)
        result shouldBe Success(Seq(0, fSize))
      }
      "the file does not contain the delimiter" in {
        val path   = Paths.get("src/test/resources/test-file-01.txt")
        val fSize  = fileSize(path)
        val result = findFwdPartitionsInPath(path, fSize / 2, "X".getBytes, 5)
        result shouldBe Success(Seq(0, fSize))
      }
    }
    "return one partition" when {
      "the delimiter is only found at the end" in {
        val path   = Paths.get("src/test/resources/test-file-01-ends-in-delimiter.txt")
        val fSize  = fileSize(path)
        val result = findFwdPartitionsInPath(path, fSize - 5, "\n".getBytes, 5)
        result shouldBe Success(Seq(0, fSize))
      }
    }
    "return two partitions" when {
      "the delimiter is found in the middle" in {
        val path   = Paths.get("src/test/resources/test-file-01.txt")
        val result = findFwdPartitionsInPath(path, 10, "\n".getBytes, 5)
        result shouldBe Success(Seq(0, 11, 21))
      }
      "the delimiter is found in the middle and at the end" in {
        val path   = Paths.get("src/test/resources/test-file-01-ends-in-delimiter.txt")
        val result = findFwdPartitionsInPath(path, 10, "\n".getBytes, 5)
        result shouldBe Success(Seq(0, 11, 22))
      }
    }
    "return three partitions" in {
      val minSplitByteSize = 25
      val path   = Paths.get("src/test/resources/test-file-02.txt")
      val result = findFwdPartitionsInPath(path, minSplitByteSize, "\n".getBytes, 5)
      result shouldBe Success(Seq(0, 30, 60, 90))
      result.get.sliding(2).foreach { case (sidx +: eidx +: Nil) => (eidx - sidx) >= minSplitByteSize shouldBe true  }
    }
  }

  "findRevPartitionsInPath" should {
    "fail" when {
      "the delimiter can not be found in maxSearchSize" in {
        val path   = Paths.get("src/test/resources/test-file-01.txt")
        val fSize  = fileSize(path)
        val result = findRevPartitionsInPath(path, fSize / 2, "X".getBytes, 5, Some(8))
        result shouldBe a[Failure[_]]
      }
      "the file does not contain the delimiter" in {
        val path   = Paths.get("src/test/resources/test-file-01.txt")
        val fSize  = fileSize(path)
        val result = findRevPartitionsInPath(path, fSize / 2, "X".getBytes, 5)
        result shouldBe a[Failure[_]]
      }
      "the maximum partition does not contain the delimiter" in {
        val path   = Paths.get("src/test/resources/test-file-01.txt")
        val fSize  = fileSize(path)
        val result = findRevPartitionsInPath(path, fSize / 2, "X".getBytes, 5)
        result shouldBe a[Failure[_]]
      }
    }
    "return a single partition" when {
      "the file is empty" in {
        val path   = Paths.get("src/test/resources/test-file-00.txt")
        val result = findRevPartitionsInPath(path, 1, "\n".getBytes, 1)
      }
      "the partition size is equal to the file size" in {
        val path   = Paths.get("src/test/resources/test-file-01.txt")
        val fSize  = fileSize(path)
        val result = findRevPartitionsInPath(path, fSize, "\n".getBytes, 5)
        result shouldBe Success(Seq(0, fSize))
      }
      "the partition size is larger than the file size" in {
        val path   = Paths.get("src/test/resources/test-file-01.txt")
        val fSize  = fileSize(path)
        val result = findFwdPartitionsInPath(path, fSize + 1, "\n".getBytes, 5)
        result shouldBe Success(Seq(0, fSize))
      }
    }
    "return two partitions" when {
      "the delimiter is found in the middle" in {
        val path   = Paths.get("src/test/resources/test-file-01.txt")
        val result = findRevPartitionsInPath(path, 13, "\n".getBytes, 5)
        result shouldBe Success(Seq(0, 11, 21))
      }
      "the long delimiter is found in the middle" in {
        val path   = Paths.get("src/test/resources/test-file-01-XYZ.txt")
        val result = findRevPartitionsInPath(path, 13, "XYZ".getBytes, 5)
        result shouldBe Success(Seq(0, 13, 23))
      }
      "the delimiter is found in the middle and at the end" in {
        val path   = Paths.get("src/test/resources/test-file-01-ends-in-delimiter.txt")
        val result = findRevPartitionsInPath(path, 12, "\n".getBytes, 5)
        result shouldBe Success(Seq(0, 11, 22))
      }
      "having the split close to maxSplitSuze" in {
        val path   = Paths.get("src/test/resources/test-file-02.txt")
        val fSize  = fileSize(path)
        val result = findRevPartitionsInPath(path, 50, "\n".getBytes, (fSize/2).toInt)
        result shouldBe Success(Seq(0, 50, 90))
      }
    }
    "return three partitions" in {
      val maxSplitByteSize = 30
      val path   = Paths.get("src/test/resources/test-file-02.txt")
      val result = findRevPartitionsInPath(path, maxSplitByteSize, "\n".getBytes, 9)
      result shouldBe Success(Seq(0, 30, 60, 90))
      result.get.sliding(2).foreach { case (sidx +: eidx +: Nil) => (eidx - sidx) <= maxSplitByteSize shouldBe true  }
    }
  }

  def fileSize(path: Path) = Bracket.auto(FileChannel.open(path, StandardOpenOption.READ))(_.size()).get

}
