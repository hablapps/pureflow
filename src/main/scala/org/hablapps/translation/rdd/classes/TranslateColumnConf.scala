package org.hablapps
package translation
package classes

case class TranslateColumnConf(
  inputColumn: String,
  lookupSrc: String,
  lookupKeyColumn: String,
  lookupValueColumn: String,
  outputColumn: String)
