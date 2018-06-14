package org.hablapps.etl
package populations

import cats.data.State

package object test{  
  type Program[t] = State[MapWriter.Env,t]
}

