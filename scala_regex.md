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
