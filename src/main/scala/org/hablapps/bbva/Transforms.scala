package org.hablapps
package bbva

import org.apache.spark.broadcast.Broadcast

trait Transforms[Collection[_], P[_]]{

  def Enrich360(rawP: Collection[RetRet], bcForeignExchange: Broadcast[Map[String, String]]): P[Collection[Ret360Enriched]]
}
