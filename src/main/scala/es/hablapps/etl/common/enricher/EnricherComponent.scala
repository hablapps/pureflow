package es.hablapps.etl.common.enricher

import es.hablapps.etl.common.IAmAEnricherComponent

trait EnricherComponent[K, R, E] {
  def enricher: IAmAEnricherComponent[K, R, E]
}
