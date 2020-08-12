package org.tupol.tnp

import org.scalatest.{FunSuite, Matchers, WordSpec}


class FilePartitionerSpec  extends WordSpec with Matchers {

  import FilePartitioner._

  "findFirst" should {

    "find None" when {
      "empty pattern and empty input" in {
        findFirst(Seq.empty[Int], Iterator.empty) shouldBe None
      }
      "empty pattern" in {
        findFirst(Seq.empty[Int], Iterator(1, 2, 3)) shouldBe None
      }
      "empty input" in {
        findFirst( Seq(1, 2, 3), Iterator.empty) shouldBe None
      }
      "pattern is not in input" in {
        findFirst( Seq(4), Iterator(1, 2, 3)) shouldBe None
      }
    }
    "find Some simple pattern" when {
      "pattern equals input" in {
        findFirst( Seq(4), Iterator(4) ) shouldBe Some(0)
      }
      "pattern appears last in input" in {
        findFirst( Seq(2), Iterator(1, 2) ) shouldBe Some(1)
      }
      "pattern appears many times in input" in {
        findFirst( Seq(2), Iterator(1, 2, 1, 2, 1, 2, 1, 2) ) shouldBe Some(1)
      }
    }
    "find Some long pattern" when {
      "pattern equals input" in {
        findFirst( Seq(1, 2), Iterator(1, 2) ) shouldBe Some(0)
      }
      "pattern appears last in input" in {
        findFirst( Seq(1, 2), Iterator(0, 1, 2) ) shouldBe Some(1)
      }
      "pattern appears many times in input" in {
        findFirst( Seq(1, 2), Iterator(0, 1, 2, 0, 1, 2, 0, 1, 2) ) shouldBe Some(1)
      }
    }
  }

}
