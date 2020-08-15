package org.tupol.tnp

import org.scalatest.{ Matchers, WordSpec }

class PartitionFinderSpec extends WordSpec with Matchers {

  import PartitionFinder._

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
}
