#Out of Memory Errors on driver
 - Occur during big workload (Spark SQl, Spark Streaming): default 512M
 - Increase driver memory
 #Out of Mmory-GC overhead limit exceeded
   -increase executor heap-size  : --executor-memory 4096m --num-executors 20 
   - change the GC policy: -XX:G1GC
 #Suppoprt JDBC via Spark Thrift Server: 
  -spark can act as distributed query engine using jdbc/odbc interface
   -server:  % $SPARK_HOME/sbin/start-thriftserver.sh --driver-memory 12g --verbose \
      --master yarn --executor-memory 16g --num-executors 100 --executor-cores 8 \
      --conf spark.hadoop.yarn.timeline-service.enabled=false \
      --conf spark.yarn.executor.memoryOverhead=8192 --conf spark.driver.maxResultSize=5g
   -client: Beeline : $SPARK_HOME/bin/beeline -u "jdbc:hive2://node460.xyz.com:10013/my1tbdb" \
   -n spark --force=true -f /test/ query_00_01_96.sql
   #can modify exceutors set up:
     ---executor-memory 6g --num-executors 80 –executor-cores 2 
  #Spark "Scratch space:
  -- ‘spark-defaults.conf’ file: spark.local.dir /data/disk1/tmp,/data/disk2/tmp,/data/disk3/tmp,/data/disk4/tmp,…
  #Max  result size exceed:
  --spark-defaults.conf’ file: spark.driver.maxResultSize 5g
  #Catalyst Errors:
   -- --conf spark.sql.broadcastTimeout 1200
  # Network time out
  --spark-defaults.conf’ file: spark.network.timeout 700
  #Out of space data nodes:
  --dfs.datanode.balance.max.concurrent.moves 
  
  # In-flight capturing of executor thread & heap dumps
   jstack
   jstack
   jmap

# use --verbose to get info on settings
 spark-submit --driver-memory 10g **--verbose --master yarn --executor-memory



docs from :
https://www.slideshare.net/jcmia1/a-beginners-guide-on-troubleshooting-spark-applications
