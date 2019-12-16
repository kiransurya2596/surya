package hack
import org.apache.spark.{SparkConf,SparkContext};
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql._;
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.inceptez.scala.hack.allmethods;
object test {
  def main(args: Array[String]): Unit = {
    println("tets")
    
    
 
    
    
 
      println("hello main ");
      
  val sparkconf = new SparkConf().setAppName("Inceptez Spark Hackathon").setMaster("local[2]");
  

  
  val sc =  new SparkContext(sparkconf);
    val spark=SparkSession.builder().appName("Sample sql app").master("local[*]").getOrCreate();

  sc.setLogLevel("error")

 
case class InsuranceClass2(IssuerId: String, IssuerId2: String, BusinessDate: String, StateCode:String,SourceName:String,NetworkName:String ,NetworkURL:String,custnum:String,MarketCoverage:String,DentalOnlyPlan:String)
// Reading insuranceinfo1.csv
val insuranceRDD1 = sc.textFile("file:///home/hduser/sparkhack2/insuranceinfo1.csv")
// Splitting the correct data from csv 
val headerAndRows = insuranceRDD1.map(p => p.split(",").map(_.trim)).filter(p => p.length == 10 )
// Fetching header from csv   
val header = headerAndRows.first
// Fetching remaining data by removing header
val data = headerAndRows.filter(_(0) != header(0))

//inserting the data to case class
val classdata=data.map(p => InsuranceClass2(p(0), p(1), p(2),p(3), p(4),p(5),p(6), p(7),p(8),p(9)))

// classdata.saveAsTextFile("file:///home/hduser/sparkhack2/insuranceinfo1_output.csv")
// Splitting the in-correct data from csv
val headerAndRows1 = insuranceRDD1.map(p => p.split(",").map(_.trim)).filter(p => p.length != 10 )
// Fetching header from csv 
val header2 = headerAndRows1.first
// Fetching remaining data by removing header
val data2 = headerAndRows1.filter(_(0) != header2(0))

//inserting the data to case class
case class InsuranceClass4(IssuerId: String, IssuerId2: String, BusinessDate: String, StateCode:String,SourceName:String,NetworkName:String ,NetworkURL:String,custnum:String,MarketCoverage:String,DentalOnlyPlan:String)

val insuranceRDD4 = sc.textFile("file:///home/hduser/sparkhack2/insuranceinfo2.csv")



val headerAndRows4 = insuranceRDD4.map(p => p.split(",").map(_.trim)).filter(p => p.length == 10 )
val header4 = headerAndRows4.first
val data4 = headerAndRows4.filter(_(0) != header4(0))
val classdata4=data4.map(p => InsuranceClass4(p(0), p(1), p(2),p(3), p(4),p(5),p(6), p(7),p(8),p(9)))
//classdata4.saveAsTextFile("file:///home/hduser/sparkhack2/insuranceinfo2_output.csv")

val headerAndRows5 = insuranceRDD4.map(p => p.split(",").map(_.trim)).filter(p => p.length != 10 )
val header5 = headerAndRows5.first

val data5 = headerAndRows5.filter(_(0) != header5(0))


val mergeRdd1 = data.union(data4);

val rddunion = data.union(data4)
     .map(p=>(p(0), p(1), p(2),p(3), p(4),p(5),p(6), p(7),p(8),p(9)))
     
println(rddunion.count)
val mergeRdd1dist=rddunion.distinct();
println(mergeRdd1dist.count)
//val joinRdd = classdata4.join(classdata)
// partition
println("actual partition="+mergeRdd1dist.partitions.size);
val insuredatarepart = mergeRdd1dist.coalesce(8)
println("increased partition="+insuredatarepart.glom().collect)

 //val rdd_20191001 = mergeRdd1dist.filter(x => (x(2).contains("10/1/2019")))
//val rddsplit=data.map(x=>x.split(","))
  // val rddexerjump = rddsplit.filter(x => x(4).toUpperCase.contains("EXERCISE") || x(5).toUpperCase.startsWith("JUMP"))
   
  val rdd_20191001 = mergeRdd1.filter(x => x(2).contains("10/1/2019"))
 val rdd_20191002 = mergeRdd1.filter(x => x(2).contains("10/2/2019"))
 //.map(x => Row(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
  
   try {
     // stroing rejecte data for file 1
     data2.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/file1rejectdata1")
     // stroing rejecte data for file 2
     data5.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/file1rejectdata2")
     
  rdd_20191001.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/rdd_20191001")
  rdd_20191002.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/rdd_20191002")
  
  
  //val insuredaterepartdf  = insuredatarepart.toDF()
  
  }
   catch {
     case ex: org.apache.hadoop.mapred.FileAlreadyExistsException =>
          {
            println("Hadoop File already exist")
           /* val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:54310"),sc.hadoopConfiguration)
            fs.delete(new org.apache.hadoop.fs.Path("/user/hduser/sparkhack2"),true)
            broadcastrdd.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/")*/
   }
   }
   
//use case3
val insuranceRDD6 = sc.textFile("file:///home/hduser/sparkhack2/insuranceinfo1.csv")

//val headerAndRows6 = insuranceRDD6.map(p => p.split(",").map(_.trim)).filter(p => p.length == 10 )
val headerAndRows61 = insuranceRDD6.map(x => x.split(",").map(_.trim)).filter(x => x.length == 10)

val header6 = headerAndRows61.first

val data6 = headerAndRows61.filter(_(0) != header6(0))

val headerAndRows6=data6.map(x => Row(x(0).toInt,x(1).toInt,x(2).to,x(3),x(4),x(5),x(6),x(7),x(8),x(9)))

//Second file
val insuranceRDD7 = sc.textFile("file:///home/hduser/sparkhack2/insuranceinfo2.csv")

//val headerAndRows6 = insuranceRDD6.map(p => p.split(",").map(_.trim)).filter(p => p.length == 10 )
val headerAndRows71 = insuranceRDD7.map(x => x.split(",").map(_.trim)).filter(x => x.length == 10).filter(x => x.length > 0)

val header7 = headerAndRows71.first
println("header="+header7(0))
val data7 = headerAndRows71.filter(_(0) != header7(0))
//println(data7)
//data7.saveAsTextFile("file:///home/hduser/sparkhack2/insuranc.csv")

val data71= data7.filter(_(0).length()!=0)

val headerAndRows7=data71.map(x => Row(x(0).toInt,x(1).toInt,x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))


/*val rddrow = filerdd.map(x => x.split(",")).filter(x => x.length == 5)
.map(x => Row(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))*/

/*case class InsuranceClass6(IssuerId: Integer, IssuerId2: Integer, BusinessDate:String , StateCode:String,SourceName:String,NetworkName:String ,
    NetworkURL:String,custnum:String,MarketCoverage:String,DentalOnlyPlan:String)*/
//var sqlctx =spark.sqlContext
 val sqlc = new SQLContext(sc)

val structypeschema = StructType (List(StructField("IssuerId", IntegerType, true),
        StructField("IssuerId2", IntegerType, true),
          StructField("BusinessDate",StringType, true),
        StructField("StateCode", StringType, true),
        StructField("SourceName", StringType, true),
        StructField("NetworkName", StringType, true),
        StructField("NetworkURL", StringType, true),
        StructField("custnum", StringType, true),
        StructField("MarketCoverage", StringType, true),
        StructField("DentalOnlyPlan", StringType, true)
        ))
        
            import org.apache.spark.sql.functions._

        
          val finalData = headerAndRows6.union(headerAndRows7)
          println(finalData.count())
 //val dfcreatedataframe = sqlc.createDataFrame(data6, structypeschema)
  val dfcreatedataframe = sqlc.createDataFrame(finalData, structypeschema)
  
     dfcreatedataframe.select("*").filter("IssuerId=94562").show(10); 
    val dfRnamedaStctaframe = dfcreatedataframe.withColumnRenamed("StateCode", "stc")

  val dfRnamedataframe = dfRnamedaStctaframe.withColumnRenamed("SourceName", "srcnme")
  
  val dfRename=  dfRnamedataframe.withColumn("Fullid",concat(col("IssuerId"),col("IssuerId2")))
dfRename.select("*").filter("NetworkName='PDP PLUS'").show(10); 
    
    val afterDrop = dfRename.drop("DentalOnlyPlan").drop("IssuerId").drop("IssuerId2");
    afterDrop.select("*").filter("NetworkName='PDP PLUS'").show(10); 
val rddColadd = afterDrop.withColumn("sysdt", current_date())withColumn("systs",current_timestamp())

val rddnonull = rddColadd.na.drop();

rddnonull.select("*").filter("Fullid='5745157451'").show(10); 
        //to_date(col("Date"),"MM-dd-yyyy").as("to_date")
// println("fdfcreatedataframe1="+dfcreatedataframe1.count())

println("Before creating insurance view");
    rddnonull.createOrReplaceTempView("insurance")
println("after creating insurance view");

    
val udfObj=new allmethods();

//val dsludf = udf(trimupperdsl _)
   // dfcsv.withColumn("udfapplied",dsludf('_c1)).show(5,false)
    spark.udf.register("spcharudf",udfObj.remspecialchar _)
/*val udfimpl = sqlc.sql("""select spcharudf(NetworkName) as newnetwork , 
             from insurance  limit 10""").show(10,false)  */
             
rddnonull.select("*").filter("Fullid='5745157451'").show(10); 
println("after creating spcharudf");
//sqlc.sql("""select * from insurance """).write.mode("overwrite").json("file:/home/hduser/sparkhack2/insurenceid.json");
//json write
sqlc.sql("""select * from insurance """).write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/hadoop/insurenceid.json");
//csv write
rddnonull.coalesce(1).write.mode("overwrite").option("header","true").option("timestampFormat","yyyy-MM-dd HH:mm:ss")
.option("nullValue","nullval")
.csv("hdfs://localhost:54310/user/hduser/hadoop/csvdataoutinsurence")

println("Writing to hive")



val sparkhive=SparkSession.builder().appName("Sample sql app").master("local[*]")
.config("hive.metastore.uris","thrift://localhost:9083")
.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
.enableHiveSupport().getOrCreate();

//HIVE READ/WRITE
/*println("Writing to hive")

rddnonull.createOrReplaceTempView("dfdbview")

spark.sql("drop table if exists dfdbviewhive")
spark.sql("create external table default.insuremysql(BusinessDate string,StateCode string,SourceName string,NetworkName string ,NetworkURL string,custnum string,MarketCoverage string,Fullid string,sysdt string,systmp string)")

rddnonull.write.mode("overwrite").saveAsTable("default.insuremysql")    
//dsdb.write.mode("overwrite").saveAsTable("default.customermysqlds")

println("Displaying hive data")

val hivedf=spark.read.table("default.insuremysql")
hivedf.show(5,false)

spark.sql("select * from default.insuremysql").show(5,false)*/
//Operation not allowed: CREATE EXTERNAL TABLE must be accompanied by LOCATION(line 1, pos 0)

//usecase 4 
println("rDDusecase4")
val rDDusecase4 = sc.textFile("file:///home/hduser/sparkhack2/custs_states.csv")
val rDDcustfilter = rDDusecase4.map(p => p.split(",").map(_.trim)).filter(p => p.length == 5 )
//println("rDDcustfilter"+rDDcustfilter.collect())
//rDDcustfilter.take(10).foreach(println)
val rDDstatesfilter = rDDusecase4.map(p => p.split(",").map(_.trim)).filter(p => p.length == 2 )
println("rDDstatesfilter"+rDDstatesfilter.count())



//case class custcaseclass(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)
//case class custsscaseclass(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)

    
    //step 3: create case class based on the structure of the data
//val rdd1 = filerdd.map(x => x.split(",")).filter(x => x.length == 5)
//.map(x => custsstatescaseclass(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    
println("Creating Dataframe and DataSet")

/*    val filedf = rdd1.toDF()
    val fileds=rdd1.toDS();*/
    
    //dataframe=dataset[row]
    //dataset=dataset[caseclass]
    
    //implicits is not required for the createDataFrame function.
   // val filedf1 = sqlc.createDataFrame(rdd1)
    //val fileds1 = sqlc.createDataset(rdd1)
    
    //filedf.printSchema()
    //filedf.show(10,false)
    
    
//val custstatesdf=spark.read.option("delimiter",",").option("allowNumericLeadingZeros","true").
//option("mode","failfast").option("inferschema","true").option("header","true").csv("file:///home/hduser/sparkhack2/custs_states.csv");
val custstatesdf=spark.read.option("delimiter",",").option("inferschema","true").option("nullValue","na").option("nanValue","0").
option("nullValue","0").option("nanValue","0").csv("file:///home/hduser/sparkhack2/custs_states.csv").toDF();


 custstatesdf.printSchema();
 custstatesdf.show(5,false)
//custstatesdf.show(10);
println("rDDstatesfilter show");
//custstatesdf.show(5,false)
println("custstatesdf"+custstatesdf.count())

    custstatesdf.printSchema();
//val statesfilterdftmp  = custstatesdf.filter(x => x(3)==0)
    val statesfilterdftmp  = custstatesdf.filter(col("_c3").isNotNull)
    println("statesfilterdftmp"+statesfilterdftmp.count())
statesfilterdftmp.show(5,false)
    val custfilterdf   = custstatesdf.filter(col("_c3").isNotNull) 
        println("custfilterdf"+custfilterdf.count())
custfilterdf.show(5,false)
    val renamecolstaes = statesfilterdftmp.withColumnRenamed("_c0", "statecode")
            println("renamecolstaes"+renamecolstaes.count())
renamecolstaes.show(5,false)

    val statesfilterdf= renamecolstaes.withColumnRenamed("_c1", "statename").drop("_c2","_c3","_c4")
    println("AFter rename "+statesfilterdf.show(5));
     statesfilterdf.show(5,false)
println("*****************cust VIEW*****************");

 custfilterdf.createOrReplaceTempView("custview")
 spark.sql("""select * from custview """).show(5,false)
 println("*****************state VIEW*****************");

  statesfilterdf.createOrReplaceTempView("statesview")
 spark.sql("""select * from statesview """).show(5,false)
 
  println("*****************insurance VIEW*****************");

    // rddnonull.createOrReplaceTempView("insurance")
 spark.sql("""select * from insurance """).show(5,false)
 val udf2=(x:String)=>udfObj.remspecialchar(x)
 // insurance
 spark.udf.register("remspecialcharudf",udf2)
    // rddnonull.withColumn("udfapplied",dsludf('_c1)).show(5,false)


    sqlc.sql("""select remspecialcharudf(NetworkName) as cleannetworkname,current_date as curdt, current_timestamp as curts
             from insurance limit 10""").show(10,false)    
    
    

  
  
  }
}