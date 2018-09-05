package org.hablapps
package bbva

trait Transforms[Collection[_], P[_]]{

  type SharedVariable[_]

  def Enrich360(rawP: Collection[RetRet], bcForeignExchange: SharedVariable[Map[String, String]]): P[Collection[Ret360Enriched]]
}
