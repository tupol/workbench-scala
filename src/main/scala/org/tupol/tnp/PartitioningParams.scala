package org.tupol.tnp

/**
 * File Partitioning parameters
 *
 * @param delimiter      The sequence of bytes that separate the partitions
 * @param splitSizeLimit The partition size limit in bytes. Depending on the strategy it can be a lower or an upper limit.
 * @param seekBufferSize The maximum search buffer size
 * @param maxSearchSize  The maximum search size
 */
case class PartitioningParams(
  delimiter: Seq[Byte],
  splitSizeLimit: Long,
  seekBufferSize: Int = 1 * KB,
  maxSearchSize: Option[Long] = None) {
  require(!delimiter.isEmpty, "The delimiter can not be empty")
  require(seekBufferSize > 0, s"The seekBufferSize is $seekBufferSize but it must be a positive number larger than 0")
  require(
    splitSizeLimit >= seekBufferSize,
    s"The splitSizeLimit is $splitSizeLimit and it must be equal or greater than seekBufferSize that is $seekBufferSize")
  maxSearchSize.map(
    mss =>
      require(
        mss >= seekBufferSize,
        s"The maxSearchSize is $mss and it must be equal or greater than seekBufferSize that is $seekBufferSize"))
}
