package es.hablapps.etl.common.enricher

trait IAmAEnricherTransaction[R, E] {
  def enrich: R => E
}
