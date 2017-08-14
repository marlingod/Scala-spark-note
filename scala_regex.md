### snippet of code rgarding regex in scala

from  https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
http://www.scala-lang.org/api/2.12.1/scala/util/matching/Regex.html

**extraction***:
```scala
val date = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
"2004-01-20" match {
  case date(year, month, day) => s"$year was a good year for PLs."
}
> res23: String = 2004 was a good year for PLs.
```
**Find Matches**:
Two main method:
###### findFirstMatchIn ######
```scala
val allYears = for (m <- date.findAllMatchIn(dates)) yield m.group(1)
```
###### findAllMatchIn ######

```scala
val allYears = for (m <- date.findAllMatchIn(dates)) yield m.group(1)
```

###### findAllMatchIn ######
```scala
val num = """(\d+)""".r
val all = num.findAllIn("123").toList
```

**Replace Text**
text replacement can be performed unconditionally or as a function of the current match:
```scala
val redacted    = date.replaceAllIn(dates, "XXXX-XX-XX")
val yearsOnly   = date.replaceAllIn(dates, m => m.group(1))
val months      = (0 to 11).map { i => val c = Calendar.getInstance; c.set(2014, i, 1); f"$c%tb" }
val reformatted = date.replaceAllIn(dates, _ match { case date(y,m,d) => f"${months(m.toInt - 1)} $d, $y" })
```

