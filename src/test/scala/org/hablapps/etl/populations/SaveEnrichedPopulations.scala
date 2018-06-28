package org.hablapps
package populations
package test

import org.hablapps.etl.rdd._

import workflow.EnrichedPopulation

object SaveEnrichedPopulations extends MapWriter[EnrichedPopulation]