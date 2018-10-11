package org.hablapps
package bbva

trait Transforms[Collection[_], P[_], BC]{

  def Enrich360(rawP: Collection[RetRet], bcForeignExchange: BC): P[Collection[Ret360Enriched]]
}
