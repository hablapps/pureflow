package org.hablapps
package etl
package df

package object writer{

  type Writer[P[_],T] = etl.Writer[DataPhrame,P,T]
}
