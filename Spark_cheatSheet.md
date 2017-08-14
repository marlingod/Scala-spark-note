# Basic Struutred Operations
**Spark: The Definitive Guide**
#### Schema ####
#### StructType ####
#### Columns and Expressions ####
##### Columns #####
```scala
import org.apache.spark.sql.functions.{col,column}
col("someColumnName")
column("someColumnName")
$”column
‘column
```

#####  Expressions #####
```scala
import org.apache.spark.sql.functions.{expr, col}
import org.apache.spark.sql.functions.expr
expr("(((someCol + 5) * 200) - 6) < otherCol")
```
4. **Records and Rows:**
```scala
import org.apache.spark.sql.Row
```
#### DataFrame Transformations ####
##### Creating schema: #####
```scala
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

val myManualSchema = new StructType(Array(
  new StructField("some", StringType, true),
  new StructField("col", StringType, true),
  new StructField("names", LongType, false)))
```	
##### Select and SelectExpr #####
```scala
SelectExpr =select(expr(“column”))
 Different way to express a column: 
import org.apache.spark.sql.functions.{expr, col, column}

df.select(
  df.col("DEST_COUNTRY_NAME"),
  col("DEST_COUNTRY_NAME"),
  column("DEST_COUNTRY_NAME"),
  'DEST_COUNTRY_NAME,
  $"DEST_COUNTRY_NAME",
  expr("DEST_COUNTRY_NAME")
).show(2)
```
_Another example_**
df.selectExpr(
```scala
  "*", // all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
Adding Columns: df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
Renaming column: df.withColumnRenamed("DEST_COUNTRY_NAME", "dest")
Removing columns:df.drop("ORIGIN_COUNTRY_NAME")
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
Change column type:df.withColumn("count", col("count").cast("int"))
filter rows: val colCondition = df.filter(col("count") < 2).take(2)
  val conditional = df.where("count < 2").take(2)
unique rows:
df.select("ORIGIN_COUNTRY_NAME").distinct()
Append and Union two dataframe:
To union two DataFrames, you have to be sure that they have the same schema and number of columns, else the union will fail.
df.union(newDF)
  .where("count = 1")
  .where($"ORIGIN_COUNTRY_NAME" =!= "United States")

Repartition and Colaence
Repartition will incur a full shuffle of the data, regardless of whether or not one is necessary. 
df.rdd.getNumPartitions
df.repartition(col("DEST_COUNTRY_NAME"))
df.repartition(5, col("DEST_COUNTRY_NAME"))
  Coalesce on the other hand will not incur a full shuffle and will try to combine partitions
```
**Filter:**
```scala
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")

df.where(col("StockCode").isin("DOT"))
  .where(priceFilter.or(descripFilter))
val DOTCodeFilter = col("StockCode") === "DOT"
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")

df.withColumn("isExpensive",
    DOTCodeFilter.and(priceFilter.or(descripFilter)))
  .where("isExpensive")
  .select("unitPrice", "isExpensive")
  .show(5)
```
**Working with numbers**
```scala
import org.apache.spark.sql.functions.{expr, pow}

val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5

df.select(
    expr("CustomerId"),
    fabricatedQuantity.alias("realQuantity"))
df.selectExpr(
  "CustomerId",
  "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity")
import org.apache.spark.sql.functions.{corr}

df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()
import org.apache.spark.sql.functions.{count, mean, stddev_pop, min, max}
There are a number of statistical functions available in the StatFunctions Package.
http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameStatFunctions
http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameNaFunctions

df.stat.approxQuantile("UnitPrice", quantileProbs, relError)
```
**working with Strings:**
```scala
import org.apache.spark.sql.functions.{lower, upper, initcap}
df.select(
    col("Description"),
    lower(col("Description")),
    upper(lower(col("Description"))))

import org.apache.spark.sql.functions.{lit, ltrim, rtrim, rpad, lpad, trim}

```
**Regular Expressions: **

```scala
import org.apache.spark.sql.functions.regexp_replace

val simpleColors = Seq("black", "white", "red", "green", "blue")
val regexString = simpleColors.map(_.toUpperCase).mkString("|")
// the | signifies `OR` in regular expression syntax

df.select(
     regexp_replace(col("Description"), regexString, "COLOR")
      .alias("color_cleaned"),
     col("Description"))


import org.apache.spark.sql.functions.translatedf.select(     translate(col("Description"), "LEET", "1337"),     col("Description"))  .show(2)

val containsBlack = col("Description").contains("BLACK")
val containsWhite = col("DESCRIPTION").contains("WHITE")

df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
  .filter("hasSimpleColor")
  .select("Description")
```

**Working with Dates and Timestamps **

```scala
import org.apache.spark.sql.functions.{current_date, current_timestamp}

val dateDF = spark.range(10)
  .withColumn("today", current_date())
  .withColumn("now", current_timestamp())

dateDF.createOrReplaceTempView("dateTable")

import org.apache.spark.sql.functions.{date_add, date_sub}

dateDF
  .select(
    date_sub(col("today"), 5),
    date_add(col("today"), 5))
  .show(1)
import org.apache.spark.sql.functions.{datediff, months_between, to_date}

dateDF
  .withColumn("week_ago", date_sub(col("today"), 7))
  .select(datediff(col("week_ago"), col("today")))
  .show(1)

dateDF
  .select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))
  .select(months_between(col("start"), col("end")))
  .show(1)
```
By using the unix_timestamp we can parse our date into a bigInt that specifies the Unix timestamp in seconds. We can then cast that to a literal timestamp before passing that into the to_date format which accepts timestamps, strings, and other dates.

```scala
import org.apache.spark.sql.functions.{unix_timestamp, from_unixtime}

val dateFormat = "yyyy-dd-MM"

val cleanDateDF = spark.range(1)
  .select(
    to_date(unix_timestamp(lit("2017-12-11"), dateFormat).cast("timestamp"))
      .alias("date"),
    to_date(unix_timestamp(lit("2017-20-12"), dateFormat).cast("timestamp"))
      .alias("date2"))

```
**Working with Nulls **

```scala
df.na.drop()
df.na.drop("any")

df.na.drop("all", Seq("StockCode", "InvoiceNo"))
df.na.fill("all", subset=["StockCode", "InvoiceNo"])
```

**Working with Complex Types**

```scala
import org.apache.spark.sql.functions.struct

val complexDF = df
  .select(struct("Description", "InvoiceNo").alias("complex"))
import org.apache.spark.sql.functions.split

df.select(split(col("Description"), " ")).show(2)
import org.apache.spark.sql.functions.array_contains

df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

```

**Explode:**

The explode function takes a column that consists of arrays and creates one row (with the rest of the values duplicated) per value in the array. The following figure illustrates the process.

```scala
import org.apache.spark.sql.functions.{split, explode}

df.withColumn("splitted", split(col("Description"), " "))
  .withColumn("exploded", explode(col("splitted")))
  .select("Description", "InvoiceNo", "exploded")
```


###### Aggregations: ######

Aggregation Functions: 
+ *countDistinct (sql.functions) :* `df.select(countDistinct("StockCode"))`
+ *Approximate Count Distinct(sql.functions):* `df.select(approx_count_distinct("StockCode", 0.1))`
+ *First and Last Value:* `df.select(first("StockCode"), last("StockCode"))`
+ *Min and Max*
+ *sumDisctinct*
+ *Average or mean*
+ *Variance and Standard Deviation:* `var_pop, stddev_pop, var_samp, stddev_samp`
+ *Skewness and Kurtosis:*  `sql.functions.{skewness, kurtosis}`
+ *Covariance  and  Correlation :* `sql.functions.{corr, covar_pop, covar_samp}`
+ *Aggregating to Complex Types:* `collect_set, collect_list`

Grouping:
Grouping with Expression:

```scala
	df.groupBy("InvoiceNo")
  	  .agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)"))
```
    
*Grouping with Maps:*
```scala
df.groupBy("InvoiceNo")
  .agg(
    "Quantity" ->"avg",
    "Quantity" -> "stddev_pop")
```
*Window Functions:*

```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
val windowSpec = Window  .partitionBy("CustomerId", "date")  .orderBy(col("Quantity").desc)  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

//Example: we want to use an aggregation function to learn more about each specific customer. For instance we might want to know the max //purchase quantity over all time

val maxPurchaseQuantity = max(col("Quantity"))
  .over(windowSpec)
 //we will use the dense_rank rank function to determine which date had the max purchase quantity for every customer.

import org.apache.spark.sql.functions.{dense_rank, rank}

val purchaseDenseRank = dense_rank()
  .over(windowSpec)
val purchaseRank = rank()
  .over(windowSpec)
import org.apache.spark.sql.functions.col

dfWithDate
  .where("CustomerId IS NOT NULL")
  .orderBy("CustomerId")
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity"))

```

***%sql***

```sql
SELECT
  CustomerId,
  date,
  Quantity,
  rank(Quantity) OVER (PARTITION BY CustomerId, date
                       ORDER BY Quantity DESC NULLS LAST
                       ROWS BETWEEN
                         UNBOUNDED PRECEDING AND
                         CURRENT ROW) as rank,

  dense_rank(Quantity) OVER (PARTITION BY CustomerId, date
                             ORDER BY Quantity DESC NULLS LAST
                             ROWS BETWEEN
                               UNBOUNDED PRECEDING AND
                               CURRENT ROW) as dRank,

  max(Quantity) OVER (PARTITION BY CustomerId, date
                      ORDER BY Quantity DESC NULLS LAST
                      ROWS BETWEEN
                        UNBOUNDED PRECEDING AND
                        CURRENT ROW) as maxPurchase
FROM
  dfWithDate
WHERE
  CustomerId IS NOT NULL
ORDER BY
  CustomerId
```

*RolluUps:*

This rollup will look across time (with our new date column) and space (with the Country column) and will create a new DataFrame that includes the grand total over all dates, the grand total for each date in the DataFrame, and the sub total for each country on each date in the dataFrame.

```scala
val rolledUpDF = dfWithDate.rollup("Date", "Country")
  .agg(sum("Quantity"))
  .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
  .orderBy("Date")
```
*Cube:*

The grand total across all dates and countries

The grand total for each date across all countries

The grand total for each country on each date

The grand total for each country across all dates

```scala
dfWithDate.cube("Date", "Country")
  .agg(sum(col("Quantity")))
  .select("Date", "Country", "sum(Quantity)")
  .orderBy("Date")
```

*Pivot:*
Pivots allow you to convert a row into a column. With a pivot we can aggregate according to some function for each of those given countries:
```scala
val pivoted = dfWithDate
  .groupBy("date")
  .pivot("Country")
  .agg("quantity" -> "sum")
```

Information retrieved from 
*Spark: The Definitive Guide*
by B. Chambers, M. Zaharia

