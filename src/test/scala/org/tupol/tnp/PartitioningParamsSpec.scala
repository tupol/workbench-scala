package org.tupol.tnp

import org.scalatest.{ Matchers, WordSpec }

class PartitioningParamsSpec extends WordSpec with Matchers {

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

}
