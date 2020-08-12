package org.tupol.tnp

trait Partitioner[T] {
  def partition: Seq[T]
}
