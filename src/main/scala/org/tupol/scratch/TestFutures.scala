package org.tupol.scratch

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

/**
 * Just garbage
 */

object TestFutures extends App {

  implicit val ec = scala.concurrent.ExecutionContext.global

  def futures = (0 to 100).map(x => () => body(x))

  def body(x: Int) = Future {
    val sleep = Random.nextInt(1500) + 500
    println(s"Start $x | $sleep")
    Thread.sleep(sleep)
    println(s"Ended $x after $sleep")
    x
  }


  def batchFutures[T, U](items: Iterable[T], toFuture: T => Future[U], batchSize: Integer)(
    implicit executionContext: ExecutionContext
  ): Future[Seq[U]] = {
    val emptyFuture                                   = Future.successful(Seq[U]())
    def batchOfFutures(batch: Seq[T]): Future[Seq[U]] = Future.sequence(batch.map(toFuture))

    items
      .grouped(batchSize)
      .zipWithIndex
      .foldLeft(emptyFuture) { (acc, bax) =>
        for {
          a <- acc
          b <- batchOfFutures(bax._1.toSeq)
          _ = println(s"### Finished batch ${bax._2}: ${a ++ b}")
        } yield (a ++ b)
      }
  }

  println(Await.result(batchFutures((0 to 20), body, 5), 100.seconds))

}
