package org.hablapps

package object etl extends TransformDefs{

  import org.apache.spark.sql.SQLContext
  import org.apache.spark.SparkContext

  implicit def sqlContextFrom(implicit sc: SparkContext) =
    new SQLContext(sc)

  //

  import naturally.{SurfaceGetter, SurfaceLens}
  import cats.data._, cats._

  implicit def MR_Kleisli[P[_]: Monad, E2, E1](implicit
      S: SurfaceGetter[P, E2, E1]) =
    new MonadReader[Kleisli[P, E2, ?], E1]{

      def ask: Kleisli[P, E2, E1] = S.apply(Kleisli.ask[P, E1])

      // Needs a modifier
      def local[A](f: E1 => E1)(fa: Kleisli[P, E2, A]): Kleisli[P, E2, A] = ???

      def pure[A](a: A) = Monad[Kleisli[P, E2, ?]].pure(a)

      def flatMap[A,B](p: Kleisli[P, E2, A])(f: A => Kleisli[P, E2, B]) =
        MonadReader[Kleisli[P, E2, ?], E2].flatMap(p)(f)

      def tailRecM[A, B](a: A)(f: A => Kleisli[P, E2, Either[A,B]]): Kleisli[P, E2, B] = ???

    }


  implicit def MR_State[P[_]: Monad, E2, E1](implicit
      S: SurfaceLens[P, E2, E1]) =
    new MonadReader[StateT[P, E2, ?], E1]{

      def ask: StateT[P, E2, E1] = S.apply(StateT.get[P, E1])

      // Needs a modifier
      def local[A](f: E1 => E1)(fa: StateT[P, E2, A]): StateT[P, E2, A] =
        ???

      def pure[A](a: A) = Monad[StateT[P, E2, ?]].pure(a)

      def flatMap[A,B](p: StateT[P, E2, A])(f: A => StateT[P, E2, B]) =
        MonadState[StateT[P, E2, ?], E2].flatMap(p)(f)

      def tailRecM[A, B](a: A)(f: A => StateT[P, E2, Either[A,B]]): StateT[P, E2, B] =
        ???

    }


  // Utilities for Schemas

  import scala.reflect.runtime.universe.TypeTag
  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.catalyst.ScalaReflection

  def product[A <: Product : TypeTag]: StructType =
    ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
}
