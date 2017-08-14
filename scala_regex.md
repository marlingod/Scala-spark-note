### snippet of code rgarding regex in scala

from  https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
http://www.scala-lang.org/api/2.12.1/scala/util/matching/Regex.html

**expression***: `val date = """(\d\d\d\d)-(\d\d)-(\d\d)""".r`
```scala
"2004-01-20" match {
  case date(year, month, day) => s"$year was a good year for PLs."
}
```
return >res23: String = 2004 was a good year for PLs.

