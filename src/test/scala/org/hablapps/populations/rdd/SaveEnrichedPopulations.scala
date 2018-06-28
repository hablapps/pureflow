package org.hablapps
package populations
package rdd
package test

import org.hablapps.etl.rdd.writer.instances._

import workflow.EnrichedPopulation

object SaveEnrichedPopulations extends MapWriter[EnrichedPopulation]
