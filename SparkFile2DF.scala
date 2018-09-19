package SQL
import org.apache.spark.sql.SparkSession

object SparkFile2DF {

  //Create a case class globally to be used inside the main method
  //Inferring the Schema Using Reflection.Automatically converting an RDD containing case classes to a DataFrame.
  // The case class defines the schema of the table. The names of the arguments to the case class are read using reflection
  // and become the names of the columns.
  case class User(id:Long,name:String,age:Long)
  case class Transport(transport_mode:String,cost_per_unit:Long)
  case class Holidays(id:Int,source:String,destination:String,transport_mode:String,distance:Long,year:Long)

  // Main method - The execution entry point for the program
  def main(args: Array[String]): Unit  = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark file2DF basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //Set the log level as warning
    spark.sparkContext.setLogLevel("WARN")

//    println("Spark Session Object created")

    //// For implicit conversions like converting RDDs and sequences  to DataFrames
    import spark.implicits._

// Reading a file for the User schema created above & splitting on comma character (,)
// trim is used to eliminate leading.trailing whitespace
    val userDF = spark.sparkContext
      .textFile("C:\\Users\\ssahay\\Documents\\Acadgild\\Completed_Assignments\\Day_20\\Dataset_User_details.txt")
      .map(_.split(","))
      .map(x => User(x(0).trim.toLong,x(1),x(2).trim.toInt))
      .toDF()


    // Reading a file for the Transport schema created above & splitting on comma character (,)
    // trim is used to eliminate leading.trailing whitespace
    val transportDF = spark.sparkContext
      .textFile("C:\\Users\\ssahay\\Documents\\Acadgild\\Completed_Assignments\\Day_20\\Dataset_Transport.txt")
      .map(_.split(","))
      .map(x => Transport(x(0), x(1).trim.toLong))
      .toDF()

    // Reading a file for the Holidays schema created above & splitting on comma character (,)
    // trim is used to eliminate leading.trailing whitespace
    val holidaysDF = spark.sparkContext
      .textFile("C:\\Users\\ssahay\\Documents\\Acadgild\\Completed_Assignments\\Day_20\\Dataset_Holidays.txt")
      .map(_.split(","))
      .map(x => Holidays(x(0).trim.toInt,x(1),x(2),x(3),x(4).trim.toLong,x(5).trim.toLong))
      .toDF()

/*    println("User Data Frame:")
    userDF.show()
    println("Transport Data Frame:")
    transportDF.show()
    println("Holidays Data Frame:")
    holidaysDF.show()*/
    //val trave_distri_perYear = spark.sql("")

    //Converting each of the above created schemas into an SQL view
    userDF.createOrReplaceTempView("user")
    transportDF.createOrReplaceTempView("transport")
    holidaysDF.createOrReplaceTempView("holiday")
/*  val merged_tab = spark.sql("select u.name,h.* from user u join holiday h on h.id=u.id")
    //val build1 = spark.sql("select h.*, b.country, b.hvacproduct from building b join hvac1 h on b.buildid = h.buildingid")
    merged_tab.show

    merged_tab.createOrReplaceTempView("merge")*/

//    1) What is the distribution of the total number of air-travelers per year

    // To find distribution of Air travellers/year we need to find count of id or year for each using a group by method.
    val a1 = spark.sql("select year, count(year) as num_of_travelers from holiday group by year")
    a1.show


//    2) What is the total air distance covered by each user per year

    //selecting each id for each user, year for grouping each year & adding total distance to find the total air by selecting
    //transport_mode as 'airplane' only.
    val a2 = spark.sql("select id,year, sum(distance) as total_distance from holiday where transport_mode = 'airplane' group by id,year order by id")
    a2.show


//  Selecting id of all the users & adding up the total distance from holiday view
    val a3_top = spark.sql("select id, sum(distance) as largest_distance from holiday group by id order by largest_distance desc")

    //Temp view created for saving the result of the query above
    a3_top.createOrReplaceTempView("a3_top")

    //a3_top.show

// selecting only those id's from from the view (a3_top) created above to select the user id's who have travelled max distance till date
//     3) Which user has travelled the largest distance till date
    val a3 = spark.sql("select id,largest_distance as Largest_Dist_Travelled from a3_top where largest_distance = (select Max(largest_distance) as total_distance from a3_top)")
    a3.show


    //Selecting destination & count of destination & providing an alias Dest_Visited from holiday view & creating an Alias
    val a4_top = spark.sql("select destination, count(destination) as Dest_Visited from holiday group by destination")
    //a4_top.show

    //Temp view created for saving the result of the query above
    a4_top.createOrReplaceTempView("a4_top")

//    4) What is the most preferred destination for all users.
    //Selecting the maximum of all the destination grouped above in the view.
    val a4 = spark.sql("select destination as `Most Visited Destination` from a4_top where Dest_Visited IN(select max(Dest_Visited) from a4_top)")
    a4.show


    // Joining two views holiday & transport to fetch cost_per_unit from transport & all rows from holiday.
    val holi_trans = spark.sql("select h.*, t.cost_per_unit from holiday h join transport t on h.transport_mode = t.transport_mode")

    //Temp view created for saving the result of the query above
    holi_trans.createOrReplaceTempView("holi_trans")

    //From the holi_trans view above selecting the route & mutiplying the count of trnsport_mode with it's respective cost/unit
    //to fetch the revenue generated by a particular route each year.
    val max_revenue = spark.sql("select source,destination,year, (count(transport_mode) * cost_per_unit) as Revenue from holi_trans group by source,destination,year,transport_mode,cost_per_unit order by Revenue desc")

    //Temp view created for saving the result of the query above
    max_revenue.createOrReplaceTempView("max_revenue")


//    5)Which route is generating the most revenue per year
    //selecting the route that genarates maximum revenue each year from max_revenue view & storing the result in a5.
    val a5 = spark.sql("select source,destination,year,Revenue from max_revenue group by source,destination,year,Revenue HAVING Revenue IN (select max(Revenue) from max_revenue)")
    a5.show


    // Joining two views holiday & transport to fetch cost_per_unit from transport for Air_travellers only & all rows from holiday.
    val holi_trans1 = spark.sql("select h.*, t.cost_per_unit from holiday h join transport t on h.transport_mode = t.transport_mode where t.transport_mode = 'airplane'")

    //Temp view created for saving the result of the query above
    holi_trans1.createOrReplaceTempView("holi_trans1")


//    6) What is the total amount spent by every user on air-travel per year
    //To fetch the total amount spent by a particular user each year grouping by id,year & counting by each id,year
    // & multiplying it by cost.
    val a6 = spark.sql("select id,year,(count(id,year) * cost_per_unit) as `Total Amount Spent` from holi_trans1 group by id,year,cost_per_unit")
    a6.show


//    7) Considering age groups of < 20 , 20-35, 35 > ,Which age group is travelling the most every year.

    // Create an RDD of  from a text file User_details.txt.
    val userDF1 = spark.sparkContext.textFile("C:\\Users\\ssahay\\Documents\\Acadgild\\Completed_Assignments\\Day_20\\Dataset_User_details.txt").map(x => (x.split(",")(0).trim.toInt,x.split(",")(1),x.split(",")(2).trim.toInt))

    // Create an RDD of  from a text file Dataset Holidays.txt.
    val holidaysDF1 = spark.sparkContext.textFile("C:\\Users\\ssahay\\Documents\\Acadgild\\Completed_Assignments\\Day_20\\Dataset_Holidays.txt").map(x => (x.split(",")(0).trim.toInt,x.split(",")(1),x.split(",")(2),x.split(",")(3),x.split(",")(4).trim.toLong,x.split(",")(5).trim.toLong))



    // create an RDD AgeGroup from user to get different age-groups from age column.
    val AgeGroup = userDF1.map(x => x._1 -> {if(x._3<20) "20" else if(x._3>35) "35" else "20-35" })

    // create an RDD year_holiday from travel to map id as key and (distance and year) as value
    val year_holiday = holidaysDF1.map(x => x._1 -> (x._6,x._5))

    // create an RDD holi_age to join AgeGroup and travelMap
    val holi_age = AgeGroup.join(year_holiday)

    // create an RDD age_travel_map to map (year and age-groups) as key and distance as a value
    val age_travel_map = holi_age.map(x => (x._2._1,x._2._2._1) -> x._2._2._2)

    // create an RDD add_dist to aggregate the keys year and age-groups
    val add_dist = age_travel_map.reduceByKey((x,y) => x+y)

    //convert the RDD add_dist into a Dataframe with RDD name as toDF_data
    val toDF_data = add_dist.map(x => (x._1._2,x._1._1,x._2)).toDF

    /* Now we have a dataframe toDF_data with data in below format
        |  _1|   _2|  _3|
          +----+-----+----+
          |1990|   20| 200|
          |1990|20-35|1000|
          |1991|20-35| 800|    */

    //Now we use spark-sql to get the desired output.

    //Creating an Schema with column names as "Year","ageGroup","Travelled"
    val colName = Seq("Year","ageGroup","Travelled")

    //Schema of toDF_data is (._1,._2,._3),convert it into (year,ageGroup,Distance)	in yearGroupSortNew by specying
    //above three columns as column names.
    val tab_data = toDF_data.toDF(colName:_*)

    // Register the DataFrame as a temporary view tab_data
    tab_data.createOrReplaceTempView("tab_data")

    //The final SQL statements to get the desired result from view tab_data. Selecting all from tab_data & inner join to self
    //on year & where travelled distanace is max by giving a self alias to tab_data view as a & b respectively.
    val a7 = spark.sql("select a.* from tab_data a inner join(select Year,max(Travelled) as Max from tab_data group by Year) b on a.Year = b.Year and a.Travelled = b.Max")

//Displaying final output as the age group that is travelling the most every year.
    //    7) Considering age groups of < 20 , 20-35, 35 > ,Which age group is travelling the most every year.
    a7.show

  }
}