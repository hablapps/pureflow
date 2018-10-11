//package org.hablapps.etl
//package rdd
//package writer
//package instances
//
//import org.apache.spark.rdd.RDD
//import org.apache.hadoop.hbase.spark.HBaseContext
//import org.apache.phoenix.spark._
//
//import cats.MonadReader, cats.syntax.functor._
//
//import scala.reflect.runtime.universe.TypeTag
//import cats.MonadReader
//import org.apache.hadoop.hbase.spark.HBaseContext
//import org.hablapps.etl.rdd.writer.Writer
//
//class HdfsWriter[P[_]: MonadReader[?[_], HBaseContext], T <: Product : TypeTag] extends writer.Writer[P, T]{
//
//  override def write(dataset: RDD[T], destination: String): P[Unit] = {
//
//
//  }
//
//}
