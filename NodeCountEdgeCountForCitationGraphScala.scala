import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object NodeCountEdgeCountForCitationGraphScala {
    def main(args: Array[String]): Unit = {

        val sc = SparkSession.builder().master("spark://cheyenne:30915").getOrCreate().sparkContext

//          val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
//          val sc = spark.sparkContext

        //  to run with yarn
        //  when running on the cluster make sure to use "--master yarn" option
        //    val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext


        val publishedDatesText = sc.textFile(args(0))
        val citationsText = sc.textFile(args(1))

        val citationsAsPairRDD = citationsText.filter(x => !x.startsWith("#"))
          .map(line => (line.split("\t")(0).toInt, line.split("\t")(1).toInt))

        //Getting all of nodes from published-dates.txt in the 90s and getting their <true_id>
        def getNodesPublishedInAGivenYearInThe90sThatDontStartWith11(year: String) : RDD[String]= {
            return publishedDatesText.filter(x => !x.startsWith("#"))
              .filter(x => x.split("\t")(1).split("-")(0).equals(year))
              .map(line => line.split("\t")(0))
              .filter(x => !x.startsWith("11"))
        }

        def getNodesPublishedInAGivenYearInThe90sThatStartWithHave11(year: String) : RDD[String]= {
            return publishedDatesText.filter(x => !x.startsWith("#"))
              .filter(x => x.split("\t")(1).split("-")(0).equals(year))
              .map(line => line.split("\t")(0))
              .filter(x => x.startsWith("11"))
              .map(x => x.tail.tail)

        }
        //Getting all of nodes from published-dates.txt in 2000 and getting their <true_id>... this had to be treated differently because some of the cross listed pairs start with 011 instead of 11
        def getNodesPublishedIn2000ThatDontStartWith11() : RDD[String]= {
            return publishedDatesText.filter(x => !x.startsWith("#"))
              .filter(x => x.split("\t")(1).split("-")(0).equals("2000"))
              .map(line => line.split("\t")(0))
              .filter(x => !x.startsWith("11"))
              .filter(x => !x.startsWith("011"))
              .map(_.trim.toInt)
              .map(_.toString)

        }

        def getNodesPublishedIn2000ThatStartWith11() : RDD[String]= {
            return publishedDatesText.filter(x => !x.startsWith("#"))
              .filter(x => x.split("\t")(1).split("-")(0).equals("2000"))
              .map(line => line.split("\t")(0))
              .filter(x => x.startsWith("11") || x.startsWith("011")) //need to do them in the same step for starting with 11
              .map(_.trim.toInt)
              .map(_.toString)
              .map(x => x.tail.tail)
        }
        //Getting all of nodes from published-dates.txt from 2001 and 2002 and getting their <true_id>.. technically the same as the 90s
        def getNodesPublishedIn2001Or2002ThatDontStartWith11(year: String) : RDD[String]= {
            return publishedDatesText.filter(x => !x.startsWith("#"))
              .filter(x => x.split("\t")(1).split("-")(0).equals(year))
              .map(line => line.split("\t")(0))
              .filter(x => !x.startsWith("11"))
              .map(_.trim.toInt)
              .map(_.toString)
        }

        def getNodesPublishedIn2001Or2002ThatStartWith11(year: String) : RDD[String]= {
            return publishedDatesText.filter(x => !x.startsWith("#"))
              .filter(x => x.split("\t")(1).split("-")(0).equals(year))
              .map(line => line.split("\t")(0))
              .filter(x => x.startsWith("11"))
              .map(x => x.tail.tail)
        }

//        //@Test method
//        def verifyStringLength90s(rddWithStringsOfLength7: RDD[String]): Boolean= {
//            val startingCount = rddWithStringsOfLength7.count()
//            val endingCount = rddWithStringsOfLength7.filter(x => x.length == 7).count()
//            return startingCount == endingCount
//        }
//
//        //@Test method
//        def verifyStringLength2000s(rddWithStringsOfLength4Or5: RDD[String]): Boolean= {
//            val startingCount = rddWithStringsOfLength4Or5.count()
//            val stringsOfLength4 = rddWithStringsOfLength4Or5.filter(x => x.length == 4).count()
//            val stringsOfLength5 = rddWithStringsOfLength4Or5.filter(x => x.length == 5).count()
//            val stringsOfLength6 = rddWithStringsOfLength4Or5.filter(x => x.length == 6).count()
//
//            return (stringsOfLength4 + stringsOfLength5 + stringsOfLength6) == startingCount
//        }
//
//        //@Test method
//        def verifyStringLength2001And2002(rddWithStringsOfLength6: RDD[String]): Boolean= {
//            val startingCount = rddWithStringsOfLength6.count()
//            val endingCount = rddWithStringsOfLength6.filter(x => x.length == 6).count()
//            return startingCount == endingCount
//        }
//
//        //@Test method
//        def getNodesPublishedInAGivenYear(year: String) : RDD[String]= {
//            return publishedDatesText.filter(x => !x.startsWith("#"))
//              .filter(x => x.split("\t")(1).split("-")(0).equals(year))
//              .map(line => line.split("\t")(0))
//
//         }


        //I get all nodes, including cross listed, but just get their true_id instead of cross-listed id (so, no more "11" in the front), and rule out ids that are duplicates (cross listed and normal listed with different dates)
        val nodesPublishedIn1992No11s =  getNodesPublishedInAGivenYearInThe90sThatDontStartWith11("1992")
        val nodesPublishedIn1992With11s =  getNodesPublishedInAGivenYearInThe90sThatStartWithHave11("1992")
        val nodesPublishedIn1992No11sNoDuplicatesKeyAsInteger = nodesPublishedIn1992No11s.union(nodesPublishedIn1992With11s).distinct().map(x => (x.toInt, 1))

        val nodesPublishedIn1993No11s =  getNodesPublishedInAGivenYearInThe90sThatDontStartWith11("1993")
        val nodesPublishedIn1993With11s =  getNodesPublishedInAGivenYearInThe90sThatStartWithHave11("1993")
        val nodesPublishedIn1993No11sNoDuplicatesKeyAsInteger = nodesPublishedIn1993No11s.union(nodesPublishedIn1993With11s).distinct().map(x => (x.toInt, 1))

        val nodesPublishedIn1994No11s =  getNodesPublishedInAGivenYearInThe90sThatDontStartWith11("1994")
        val nodesPublishedIn1994With11s =  getNodesPublishedInAGivenYearInThe90sThatStartWithHave11("1994")
        val nodesPublishedIn1994No11sNoDuplicatesKeyAsInteger = nodesPublishedIn1994No11s.union(nodesPublishedIn1994With11s).distinct().map(x => (x.toInt, 1))

        val nodesPublishedIn1995No11s =  getNodesPublishedInAGivenYearInThe90sThatDontStartWith11("1995")
        val nodesPublishedIn1995With11s =  getNodesPublishedInAGivenYearInThe90sThatStartWithHave11("1995")
        val nodesPublishedIn1995No11sNoDuplicatesKeyAsInteger = nodesPublishedIn1995No11s.union(nodesPublishedIn1995With11s).distinct().map(x => (x.toInt, 1))

        val nodesPublishedIn1996No11s =  getNodesPublishedInAGivenYearInThe90sThatDontStartWith11("1996")
        val nodesPublishedIn1996With11s =  getNodesPublishedInAGivenYearInThe90sThatStartWithHave11("1996")
        val nodesPublishedIn1996No11sNoDuplicatesKeyAsInteger = nodesPublishedIn1996No11s.union(nodesPublishedIn1996With11s).distinct().map(x => (x.toInt, 1))

        val nodesPublishedIn1997No11s =  getNodesPublishedInAGivenYearInThe90sThatDontStartWith11("1997")
        val nodesPublishedIn1997With11s =  getNodesPublishedInAGivenYearInThe90sThatStartWithHave11("1997")
        val nodesPublishedIn1997No11sNoDuplicatesKeyAsInteger = nodesPublishedIn1997No11s.union(nodesPublishedIn1997With11s).distinct().map(x => (x.toInt, 1))

        val nodesPublishedIn1998No11s =  getNodesPublishedInAGivenYearInThe90sThatDontStartWith11("1998")
        val nodesPublishedIn1998With11s =  getNodesPublishedInAGivenYearInThe90sThatStartWithHave11("1998")
        val nodesPublishedIn1998No11sNoDuplicatesKeyAsInteger = nodesPublishedIn1998No11s.union(nodesPublishedIn1998With11s).distinct().map(x => (x.toInt, 1))

        val nodesPublishedIn1999No11s =  getNodesPublishedInAGivenYearInThe90sThatDontStartWith11("1999")
        val nodesPublishedIn1999With11s =  getNodesPublishedInAGivenYearInThe90sThatStartWithHave11("1999")
        val nodesPublishedIn1999No11sNoDuplicatesKeyAsInteger = nodesPublishedIn1999No11s.union(nodesPublishedIn1999With11s).distinct().map(x => (x.toInt, 1))

        val nodesPublishedIn2000No11s =  getNodesPublishedIn2000ThatDontStartWith11()
        val nodesPublishedIn2000With11s =  getNodesPublishedIn2000ThatStartWith11()
        val nodesPublishedIn2000No11sNoDuplicatesKeyAsInteger = nodesPublishedIn2000No11s.union(nodesPublishedIn2000With11s).distinct().map(x => (x.toInt, 1))

        val nodesPublishedIn2001No11s =  getNodesPublishedIn2001Or2002ThatDontStartWith11("2001")
        val nodesPublishedIn2001With11s =  getNodesPublishedIn2001Or2002ThatStartWith11("2001")
        val nodesPublishedIn2001No11sNoDuplicatesKeyAsInteger = nodesPublishedIn2001No11s.union(nodesPublishedIn2001With11s).distinct().map(x => (x.toInt, 1))

        val nodesPublishedIn2002No11s =  getNodesPublishedIn2001Or2002ThatDontStartWith11("2002")
        val nodesPublishedIn2002With11s =  getNodesPublishedIn2001Or2002ThatStartWith11("2002")
        val nodesPublishedIn2002No11sNoDuplicatesKeyAsInteger = nodesPublishedIn2002No11s.union(nodesPublishedIn2002With11s).distinct().map(x => (x.toInt, 1))


        //I go through the citations rdd and make sure I order from new to old, because old papers can't cite newer papers (this is needed for calculating edge count growth by year)
        val citationsPairRDDWithKeyAndValueOnlyFromThe90sLeftBiggerThanRight = citationsAsPairRDD.filter(line => line._1.toString.length.equals(7) && line._2.toString.length.equals(7))
          .filter(line => line._1 > line._2)

        val citationsPairRDDWithKeyAndValueOnlyFromThe90sRightBiggerThanLeftSwitched = citationsAsPairRDD.filter(line => line._1.toString.length.equals(7) && line._2.toString.length.equals(7))
          .filter(line => line._1 < line._2)
          .map(_.swap)

        val citationsPairRDDWithKeyFrom2000sValueFrom90sLeftBiggerThanRight = citationsAsPairRDD.filter(line => !line._1.toString.length.equals(7) && line._2.toString.length.equals(7)) //2000s should be on the left

        val citationsPairRDDWithKeyFrom90sValueFrom2000sRightBiggerThanLeftSwitched = citationsAsPairRDD.filter(line => line._1.toString.length.equals(7) && !line._2.toString.length.equals(7)) //2000s should be on the left
          .map(_.swap)

        val citationsPairRDDWithKeyAndValueOnlyFromThe2000sLeftBiggerThanRight = citationsAsPairRDD.filter(line => !line._1.toString.length.equals(7) && !line._2.toString.length.equals(7))
          .filter(line => line._1 > line._2)

        val citationsPairRDDWithKeyAndValueOnlyFromThe2000sRightBiggerThanLeftSwitched = citationsAsPairRDD.filter(line => !line._1.toString.length.equals(7) && !line._2.toString.length.equals(7))
          .filter(line => line._1 < line._2)
          .map(_.swap)

        val peopleWhoCitedThemselves = citationsAsPairRDD.filter(line => line._1 == line._2)

        //This is the final rdd I use for creating an adjacency list and growing my undirected graph.. it includes people who cited themselves
        val citationsPairRDDLeftNewerThanRight = citationsPairRDDWithKeyAndValueOnlyFromThe90sLeftBiggerThanRight.union(citationsPairRDDWithKeyAndValueOnlyFromThe90sRightBiggerThanLeftSwitched)
          .union(citationsPairRDDWithKeyFrom2000sValueFrom90sLeftBiggerThanRight)
          .union(citationsPairRDDWithKeyFrom90sValueFrom2000sRightBiggerThanLeftSwitched)
          .union(citationsPairRDDWithKeyAndValueOnlyFromThe2000sLeftBiggerThanRight)
          .union(citationsPairRDDWithKeyAndValueOnlyFromThe2000sRightBiggerThanLeftSwitched)
          .union(peopleWhoCitedThemselves)


        //    //@Test variable calculations
        //    val no11sAddWith11sToMakeTotal1992 = if (fromNodesPublishedIn1992No11sWithDuplicates.count() == getNodesPublishedInAGivenYear("1992").count()) true else false
        //    val no11sAddWith11sToMakeTotal1993 = if (fromNodesPublishedIn1993No11sWithDuplicates.count() == getNodesPublishedInAGivenYear("1993").count()) true else false
        //    val no11sAddWith11sToMakeTotal1994 = if (fromNodesPublishedIn1994No11sWithDuplicates.count() == getNodesPublishedInAGivenYear("1994").count()) true else false
        //    val no11sAddWith11sToMakeTotal1995 = if (fromNodesPublishedIn1995No11sWithDuplicates.count() == getNodesPublishedInAGivenYear("1995").count()) true else false
        //    val no11sAddWith11sToMakeTotal1996 = if (fromNodesPublishedIn1996No11sWithDuplicates.count() == getNodesPublishedInAGivenYear("1996").count()) true else false
        //    val no11sAddWith11sToMakeTotal1997 = if (fromNodesPublishedIn1997No11sWithDuplicates.count() == getNodesPublishedInAGivenYear("1997").count()) true else false
        //    val no11sAddWith11sToMakeTotal1998 = if (fromNodesPublishedIn1998No11sWithDuplicates.count() == getNodesPublishedInAGivenYear("1998").count()) true else false
        //    val no11sAddWith11sToMakeTotal1999 = if (fromNodesPublishedIn1999No11sWithDuplicates.count() == getNodesPublishedInAGivenYear("1999").count()) true else false
        //    val no11sAddWith11sToMakeTotal2000 = if (fromNodesPublishedIn2000No11sWithDuplicates.count() == getNodesPublishedInAGivenYear("2000").count()) true else false
        //    val no11sAddWith11sToMakeTotal2001 = if (fromNodesPublishedIn2001No11sWithDuplicates.count() == getNodesPublishedInAGivenYear("2001").count()) true else false
        //    val no11sAddWith11sToMakeTotal2002 = if (fromNodesPublishedIn2002No11sWithDuplicates.count() == getNodesPublishedInAGivenYear("2002").count()) true else false
        //
        //    //@Test variable calculations
        //    val isStringLength7ForAllIn1992 = verifyStringLength90s(fromNodesPublishedIn1992No11sWithDuplicates)
        //    val isStringLength7ForAllIn1993 = verifyStringLength90s(fromNodesPublishedIn1993No11sWithDuplicates)
        //    val isStringLength7ForAllIn1994 = verifyStringLength90s(fromNodesPublishedIn1994No11sWithDuplicates)
        //    val isStringLength7ForAllIn1995 = verifyStringLength90s(fromNodesPublishedIn1995No11sWithDuplicates)
        //    val isStringLength7ForAllIn1996 = verifyStringLength90s(fromNodesPublishedIn1996No11sWithDuplicates)
        //    val isStringLength7ForAllIn1997 = verifyStringLength90s(fromNodesPublishedIn1997No11sWithDuplicates)
        //    val isStringLength7ForAllIn1998 = verifyStringLength90s(fromNodesPublishedIn1998No11sWithDuplicates)
        //    val isStringLength7ForAllIn1999 = verifyStringLength90s(fromNodesPublishedIn1999No11sWithDuplicates)
        //    val isStringLength4Or5ForAllIn2000 = verifyStringLength2000s(fromNodesPublishedIn2000No11sWithDuplicates)
        //    val isStringLength6ForAllIn2001 = verifyStringLength2001And2002(fromNodesPublishedIn2001No11sWithDuplicates)
        //    val isStringLength6ForAllIn2002 = verifyStringLength2001And2002(fromNodesPublishedIn2002No11sWithDuplicates)
        //
        //    //@Test variable calculation
        //    val allPublishedDatesCountedEqualsTotalFromOriginalRDD = if (fromNodesPublishedIn1992No11sWithDuplicates.count() +
        //                                                                fromNodesPublishedIn1993No11sWithDuplicates.count() +
        //                                                                fromNodesPublishedIn1994No11sWithDuplicates.count() +
        //                                                                fromNodesPublishedIn1995No11sWithDuplicates.count() +
        //                                                                fromNodesPublishedIn1996No11sWithDuplicates.count() +
        //                                                                fromNodesPublishedIn1997No11sWithDuplicates.count() +
        //                                                                fromNodesPublishedIn1998No11sWithDuplicates.count() +
        //                                                                fromNodesPublishedIn1999No11sWithDuplicates.count() +
        //                                                                fromNodesPublishedIn2000No11sWithDuplicates.count() +
        //                                                                fromNodesPublishedIn2001No11sWithDuplicates.count() +
        //                                                                fromNodesPublishedIn2002No11sWithDuplicates.count() == publishedDatesText.count() - 1 ) true else false
        //
        //
        //    //@Test
        //    val allCitationsAccountedFor = if (citationsPairRDDLeftNewerThanRight.count() == citationsAsPairRDD.count()) true else false
        //
        //    //Print @Test results
        //    println("all published dates in 1992 are accounted for ..." + no11sAddWith11sToMakeTotal1992)
        //    println("all published dates in 1993 are accounted for ..." + no11sAddWith11sToMakeTotal1993)
        //    println("all published dates in 1994 are accounted for ..." + no11sAddWith11sToMakeTotal1994)
        //    println("all published dates in 1995 are accounted for ..." + no11sAddWith11sToMakeTotal1995)
        //    println("all published dates in 1996 are accounted for ..." + no11sAddWith11sToMakeTotal1996)
        //    println("all published dates in 1997 are accounted for ..." + no11sAddWith11sToMakeTotal1997)
        //    println("all published dates in 1998 are accounted for ..." + no11sAddWith11sToMakeTotal1998)
        //    println("all published dates in 1999 are accounted for ..." + no11sAddWith11sToMakeTotal1999)
        //    println("all published dates in 2000 are accounted for ..." + no11sAddWith11sToMakeTotal2000)
        //    println("all published dates in 2001 are accounted for ..." + no11sAddWith11sToMakeTotal2001)
        //    println("all published dates in 2002 are accounted for ..." + no11sAddWith11sToMakeTotal2002)
        //    println("all published dates accounted for ..." + allPublishedDatesCountedEqualsTotalFromOriginalRDD)
        //    println()
        //    println("String length 1992s accounted for ..." + isStringLength7ForAllIn1992)
        //    println("String length 1993s accounted for ..." + isStringLength7ForAllIn1993)
        //    println("String length 1994s accounted for ..." + isStringLength7ForAllIn1994)
        //    println("String length 1995s accounted for ..." + isStringLength7ForAllIn1995)
        //    println("String length 1996s accounted for ..." + isStringLength7ForAllIn1996)
        //    println("String length 1997s accounted for ..." + isStringLength7ForAllIn1997)
        //    println("String length 1998s accounted for ..." + isStringLength7ForAllIn1998)
        //    println("String length 1999s accounted for ..." + isStringLength7ForAllIn1999)
        //    println("String lengths 2000s accounted for ..." + isStringLength4Or5ForAllIn2000)
        //    println("String length 2001 accounted for ..." + isStringLength6ForAllIn2001)
        //    println("String length 2002 accounted for ..." + isStringLength6ForAllIn2002)
        //    println()
        //    println("verifying all citations are accounted for, with newer nodes on the left..." + allCitationsAccountedFor)


        //Assignment Part 1 Results
        println("Node counts added by year... ")
        println("1992..." +     nodesPublishedIn1992No11sNoDuplicatesKeyAsInteger.count())
        println("1993..." +     nodesPublishedIn1993No11sNoDuplicatesKeyAsInteger.count())
        println("1994..." +     nodesPublishedIn1994No11sNoDuplicatesKeyAsInteger.count())
        println("1995..." +     nodesPublishedIn1995No11sNoDuplicatesKeyAsInteger.count())
        println("1996..." +     nodesPublishedIn1996No11sNoDuplicatesKeyAsInteger.count())
        println("1997..." +     nodesPublishedIn1997No11sNoDuplicatesKeyAsInteger.count())
        println("1998..." +     nodesPublishedIn1998No11sNoDuplicatesKeyAsInteger.count())
        println("1999..." +     nodesPublishedIn1999No11sNoDuplicatesKeyAsInteger.count())
        println("2000..." +     nodesPublishedIn2000No11sNoDuplicatesKeyAsInteger.count())
        println("2001..." +     nodesPublishedIn2001No11sNoDuplicatesKeyAsInteger.count())
        println("2002..." +     nodesPublishedIn2002No11sNoDuplicatesKeyAsInteger.count())

        //I get new edges by year by joining 1992-2002 nodes from the published-dates rdd and joining with the citations new-->old rdd which will guarantee I'm only getting edges for a certain year and previous years
        //that it had cited, since my citations rdd is guaranteed to be new-->old
        println("Edges added by year... ")
        println("1992..." + nodesPublishedIn1992No11sNoDuplicatesKeyAsInteger.join(citationsPairRDDLeftNewerThanRight).count())
        println("1993..." + nodesPublishedIn1993No11sNoDuplicatesKeyAsInteger.join(citationsPairRDDLeftNewerThanRight).count())
        println("1994..." + nodesPublishedIn1994No11sNoDuplicatesKeyAsInteger.join(citationsPairRDDLeftNewerThanRight).count())
        println("1995..." + nodesPublishedIn1995No11sNoDuplicatesKeyAsInteger.join(citationsPairRDDLeftNewerThanRight).count())
        println("1996..." + nodesPublishedIn1996No11sNoDuplicatesKeyAsInteger.join(citationsPairRDDLeftNewerThanRight).count())
        println("1997..." + nodesPublishedIn1997No11sNoDuplicatesKeyAsInteger.join(citationsPairRDDLeftNewerThanRight).count())
        println("1998..." + nodesPublishedIn1998No11sNoDuplicatesKeyAsInteger.join(citationsPairRDDLeftNewerThanRight).count())
        println("1999..." + nodesPublishedIn1999No11sNoDuplicatesKeyAsInteger.join(citationsPairRDDLeftNewerThanRight).count())
        println("2000..." + nodesPublishedIn2000No11sNoDuplicatesKeyAsInteger.join(citationsPairRDDLeftNewerThanRight).count())
        println("2001..." + nodesPublishedIn2001No11sNoDuplicatesKeyAsInteger.join(citationsPairRDDLeftNewerThanRight).count())
        println("2002..." + nodesPublishedIn2002No11sNoDuplicatesKeyAsInteger.join(citationsPairRDDLeftNewerThanRight).count())



        //Now Assignment Part 2
        //Just getting nodes up to 1997
        val allNodesPublishedBetween1992And1997 = nodesPublishedIn1992No11sNoDuplicatesKeyAsInteger.union(nodesPublishedIn1993No11sNoDuplicatesKeyAsInteger)
          .union(nodesPublishedIn1994No11sNoDuplicatesKeyAsInteger)
          .union(nodesPublishedIn1995No11sNoDuplicatesKeyAsInteger)
          .union(nodesPublishedIn1996No11sNoDuplicatesKeyAsInteger)
          .union(nodesPublishedIn1997No11sNoDuplicatesKeyAsInteger)

        val citationsPairRDDLeftNewerThanRightWithoutPeopleWhoCitedThemselves = citationsPairRDDWithKeyAndValueOnlyFromThe90sLeftBiggerThanRight.union(citationsPairRDDWithKeyAndValueOnlyFromThe90sRightBiggerThanLeftSwitched)
          .union(citationsPairRDDWithKeyFrom2000sValueFrom90sLeftBiggerThanRight)
          .union(citationsPairRDDWithKeyFrom90sValueFrom2000sRightBiggerThanLeftSwitched)
          .union(citationsPairRDDWithKeyAndValueOnlyFromThe2000sLeftBiggerThanRight)
          .union(citationsPairRDDWithKeyAndValueOnlyFromThe2000sRightBiggerThanLeftSwitched)


        val allCitationsFrom1992To1997WithoutPeopleWhoCitedThemself = allNodesPublishedBetween1992And1997.join(citationsPairRDDLeftNewerThanRightWithoutPeopleWhoCitedThemselves)
          .map(line => (line._1, line._2._2)) //joining gives a structure of (from, (1, to))

        val allCitationsFrom1992To1997WithoutPeopleWhoCitedThemselfFromToSwapped = allCitationsFrom1992To1997WithoutPeopleWhoCitedThemself.map(_.swap)

        //Final RDD to use for most of my calculations
        val allCitationsFrom1992To1997WithoutPeopleWhoCitedThemselfUndirectedGraph = allCitationsFrom1992To1997WithoutPeopleWhoCitedThemself.union(allCitationsFrom1992To1997WithoutPeopleWhoCitedThemselfFromToSwapped)

        //From now on, I won't add the "Without People Who Cited Themself" name to variables, even though it's true for my RDD
        //Here is where I ensure I only get the citations from citations.txt that were from a year or previous year, and were citing only papers from a year or a previous year...
        //ex, 1994 would be every line in citations.txt that had a 92,93, or 94 in both the "from" and "to" column
        //These are all the adjacency lists up to certain years.
        val validNeighborsUpTo1992 = allCitationsFrom1992To1997WithoutPeopleWhoCitedThemselfUndirectedGraph
          .filter(line => line._1.toString.startsWith("92") && line._2.toString.startsWith("92"))
          .map(line => (line._1, List(line._2)))
          .reduceByKey(_:::_)//need the reduce by key to form adjacency lists

        val validNeighborsUpTo1993 = allCitationsFrom1992To1997WithoutPeopleWhoCitedThemselfUndirectedGraph
          .filter(line => (line._1.toString.startsWith("92") || line._1.toString.startsWith("93")) && (line._2.toString.startsWith("92") || line._2.toString.startsWith("93")))
          .map(line => (line._1, List(line._2)))
          .reduceByKey(_:::_)

        val validNeighborsUpTo1994 = allCitationsFrom1992To1997WithoutPeopleWhoCitedThemselfUndirectedGraph
          .filter(line => (line._1.toString.startsWith("92") || line._1.toString.startsWith("93") || line._1.toString.startsWith("94")) && (line._2.toString.startsWith("92") || line._2.toString.startsWith("93") || line._2.toString.startsWith("94")))
          .map(line => (line._1, List(line._2)))
          .reduceByKey(_:::_)

        val validNeighborsUpTo1995 = allCitationsFrom1992To1997WithoutPeopleWhoCitedThemselfUndirectedGraph
          .filter(line => (line._1.toString.startsWith("92") || line._1.toString.startsWith("93") || line._1.toString.startsWith("94") || line._1.toString.startsWith("95")) && (line._2.toString.startsWith("92") || line._2.toString.startsWith("93") || line._2.toString.startsWith("94") || line._2.toString.startsWith("95")))
          .map(line => (line._1, List(line._2)))
          .reduceByKey(_:::_)

        val validNeighborsUpTo1996 = allCitationsFrom1992To1997WithoutPeopleWhoCitedThemselfUndirectedGraph
          .filter(line => (line._1.toString.startsWith("92") || line._1.toString.startsWith("93") || line._1.toString.startsWith("94") || line._1.toString.startsWith("95") || line._1.toString.startsWith("96")) && (line._2.toString.startsWith("92") || line._2.toString.startsWith("93") || line._2.toString.startsWith("94") || line._2.toString.startsWith("95") || line._2.toString.startsWith("96")))
          .map(line => (line._1, List(line._2)))
          .reduceByKey(_:::_)

        val validNeighborsUpTo1997 = allCitationsFrom1992To1997WithoutPeopleWhoCitedThemselfUndirectedGraph
          .filter(line => (line._1.toString.startsWith("92") || line._1.toString.startsWith("93") || line._1.toString.startsWith("94") || line._1.toString.startsWith("95") || line._1.toString.startsWith("96") || line._1.toString.startsWith("97")) && (line._2.toString.startsWith("92") || line._2.toString.startsWith("93") || line._2.toString.startsWith("94") || line._2.toString.startsWith("95") || line._2.toString.startsWith("96") || line._2.toString.startsWith("97")))
          .map(line => (line._1, List(line._2)))
          .reduceByKey(_:::_)



//        //@Test
//        val doesCountForAllNodesPublishedBetween1992And1998EqualIndividualCountsBetween1992And1997 =  if (nodesPublishedIn1992No11sNoDuplicatesKeyAsInteger.count() +
//                                                          nodesPublishedIn1993No11sNoDuplicatesKeyAsInteger.count() +
//                                                          nodesPublishedIn1994No11sNoDuplicatesKeyAsInteger.count() +
//                                                          nodesPublishedIn1995No11sNoDuplicatesKeyAsInteger.count() +
//                                                          nodesPublishedIn1996No11sNoDuplicatesKeyAsInteger.count() +
//                                                          nodesPublishedIn1997No11sNoDuplicatesKeyAsInteger.count() == allNodesPublishedBetween1992And1997.count) true else false
//
//        //@Test
//        val peopleWhoCitedThemselfFrom92to97 = citationsAsPairRDD.filter(line => line._1 == line._2)
//                                                                .filter(line => line._1.toString.startsWith("92") ||
//                                                                  line._1.toString.startsWith("93") ||
//                                                                  line._1.toString.startsWith("94") ||
//                                                                  line._1.toString.startsWith("95") ||
//                                                                  line._1.toString.startsWith("96") ||
//                                                                  line._1.toString.startsWith("97"))
//
//
//        //@Test
//        val areAllCitationsBetween92And97AreAccountedForAndGoFromNewToOld = if (allCitationsFrom1992To1997WithoutPeopleWhoCitedThemself.count().equals(allNodesPublishedBetween1992And1997.join(citationsPairRDDLeftNewerThanRightWithoutPeopleWhoCitedThemselves).count())) true else false
//
//        //@Test
//        val areAllCitationsFrom92To97FromNode = if (allCitationsFrom1992To1997WithoutPeopleWhoCitedThemself.filter(line => !line._1.toString.startsWith("9")).count() == 0) true else false
//
//        //@Test
//        val areAllCitationsFrom92To97ToNode = if (allCitationsFrom1992To1997WithoutPeopleWhoCitedThemself.filter(line => !line._2.toString.startsWith("9")).count() == 0) true else false
//
//        //@Test
//        val areAllFromNodesNewerThanToNodes = if (allCitationsFrom1992To1997WithoutPeopleWhoCitedThemself.filter(line => line._1 < line._2).count() == 0) true else false
//
//        //Test
//        val areCitationsBidirectional = if (allCitationsFrom1992To1997WithoutPeopleWhoCitedThemselfUndirectedGraph.count() == allCitationsFrom1992To1997WithoutPeopleWhoCitedThemself.count() * 2) true else false
//
//
//        //Test
//        val shouldBeCommonKeysBetween1992And19921993RDDs = if (validNeighborsUpTo1992.join(validNeighborsUpTo1993).count() != 0) true else false
//
//        //Test
//        val shouldBeCommonKeysBetween19921993And1992199319941995RDDs = if (validNeighborsUpTo1993.join(validNeighborsUpTo1995).count() != 0) true else false
//
//
//
//        //Print @Test results
//        println("verifying aggregate count equals sum of individual counts ..." + doesCountForAllNodesPublishedBetween1992And1998EqualIndividualCountsBetween1992And1997)
//        println("verifying didn't accidentally remove any when organizing the citations between 92 and 97..." + areAllCitationsBetween92And97AreAccountedForAndGoFromNewToOld)
//        println("verifying all citation from nodes are from 1990s..." + areAllCitationsFrom92To97FromNode)
//        println("verifying all citation to nodes are from 1990s..." + areAllCitationsFrom92To97ToNode)
//        println("verifying all from nodes are newer than all to nodes..." + areAllFromNodesNewerThanToNodes)
//        println("verifying bidrectional citations is the correct length ..." + areCitationsBidirectional)
//        println("verifying common keys between 1992 rdd and 19921993 rdd ..." + shouldBeCommonKeysBetween1992And19921993RDDs)
//        println("verifying common keys between 19921993 rdd and 1992199319941995 rdd ..." + shouldBeCommonKeysBetween19921993And1992199319941995RDDs)








        //ValidNeighborsUpTo199x are undirected adjacency lists
        val validNeighborsUpToYear = validNeighborsUpTo1993
        validNeighborsUpToYear.persist(StorageLevel.MEMORY_ONLY)

        //Flatmap to pull all nodes out of adjacency lists, and remember you swapped all the nodes from the citations rdd to make sure you have an undirected, not directed, graph
        val initialUndirectedCitationGraphPathOf1 = validNeighborsUpToYear.flatMap(line => line._2.map((line._1, _)))

        val shortestPathsUpTo1 = initialUndirectedCitationGraphPathOf1.map(line => ((line._1, line._2), List(0)))
              .filter(line => !(line._1._1 == line._1._2)) //need this in case a shortest path gets found between something and itself







        val validNeighborsPathsOf2 = initialUndirectedCitationGraphPathOf1
          .join(validNeighborsUpToYear) //ending of pair, (starting of pair, new list)
          .map(line => (line._2._1, line._1, line._2._2)) //starting of pair, ending of pair, new list
          .flatMap(line => line._3.map((line._1, line._2, _))) //now multiple rows of starting of pair, ending of pair, new elements
          .map(line => ((line._1, line._3), List(line._2)))

        //I order all of them so I can delete duplicate pairs that exist (1,4 and 4,1 are the same, so I order them both as 1,4 to kill off duplicates)
        val pathOf2SubtractedFirstSmaller = validNeighborsPathsOf2
          .filter(line => line._1._1 < line._1._2)
        val pathOf2SubtractedFirstBigger = validNeighborsPathsOf2
          .filter(line => line._1._1 > line._1._2)
          .map(line => ((line._1._2, line._1._1), line._2))
        val pathOf2Subtracted = pathOf2SubtractedFirstSmaller.union(pathOf2SubtractedFirstBigger)
          .reduceByKey(_:::_)
          .subtractByKey(shortestPathsUpTo1)
          .filter(line => !(line._1._1 == line._1._2))

        val shortestPathsUpTo2 = shortestPathsUpTo1.union(pathOf2Subtracted)







        val pathOf2SubtractedWithKeysSwapped = pathOf2Subtracted.map(line => ((line._1._2, line._1._1), line._2))
        val pathOf2SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf2SubtractedWithKeysSwapped.union(pathOf2Subtracted)

        val validNeighborsPathsOf3 = pathOf2SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
          .map(line => (line._1, (line._2, line._3)))
          .join(validNeighborsUpToYear)
          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
          .map(line => ((line._1, line._4), List(line._3)))

        val pathOf3SubtractedFirstSmaller = validNeighborsPathsOf3
          .filter(line => line._1._1 < line._1._2)
        val pathOf3SubtractedFirstBigger = validNeighborsPathsOf3
          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
        val pathOf3Subtracted = pathOf3SubtractedFirstSmaller.union(pathOf3SubtractedFirstBigger)
          .reduceByKey(_:::_)
          .subtractByKey(shortestPathsUpTo2)
          .filter(line => !(line._1._1 == line._1._2))

        val shortestPathsUpTo3 = shortestPathsUpTo2.union(pathOf3Subtracted)






        val pathOf3SubtractedWithKeysSwapped = pathOf3Subtracted.map(line => ((line._1._2, line._1._1), line._2))
        val pathOf3SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf3SubtractedWithKeysSwapped.union(pathOf3Subtracted)

        val validNeighborsPathsOf4 = pathOf3SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
          .map(line => (line._1, (line._2, line._3)))
          .join(validNeighborsUpToYear) //ending, ((middle list, starting), new list)
          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2)) //starting, middle list, ending, new list
          .flatMap(line => line._4.map((line._1, line._2, line._3, _))) //now multiple rows of (starting, middle list, ending, new element)
          .map(line => ((line._1, line._4), List(line._3)))

        val pathOf4SubtractedFirstSmaller = validNeighborsPathsOf4
          .filter(line => line._1._1 < line._1._2)
        val pathOf4SubtractedFirstBigger = validNeighborsPathsOf4
          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
        val pathOf4Subtracted = pathOf4SubtractedFirstSmaller.union(pathOf4SubtractedFirstBigger)
          .reduceByKey(_:::_)
          .subtractByKey(shortestPathsUpTo3)
          .filter(line => !(line._1._1 == line._1._2))

        val shortestPathsUpTo4 = shortestPathsUpTo3.union(pathOf4Subtracted)






        val pathOf4SubtractedWithKeysSwapped = pathOf4Subtracted.map(line => ((line._1._2, line._1._1), line._2))
        val pathOf4SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf4SubtractedWithKeysSwapped.union(pathOf4Subtracted)

        val validNeighborsPathsOf5 = pathOf4SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
          .map(line => (line._1, (line._2, line._3)))
          .join(validNeighborsUpToYear)
          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
          .map(line => ((line._1, line._4), List(line._3)))

        val pathOf5SubtractedFirstSmaller = validNeighborsPathsOf5
          .filter(line => line._1._1 < line._1._2)
        val pathOf5SubtractedFirstBigger = validNeighborsPathsOf5
          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
        val pathOf5Subtracted = pathOf5SubtractedFirstSmaller.union(pathOf5SubtractedFirstBigger)
          .reduceByKey(_:::_)
          .subtractByKey(shortestPathsUpTo4)
          .filter(line => !(line._1._1 == line._1._2))

        val shortestPathsUpTo5 = shortestPathsUpTo4.union(pathOf5Subtracted)







        val pathOf5SubtractedWithKeysSwapped = pathOf5Subtracted.map(line => ((line._1._2, line._1._1), line._2))
        val pathOf5SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf5SubtractedWithKeysSwapped.union(pathOf5Subtracted)

        val validNeighbors1992AsRDDFlatmappedPathsOf6 = pathOf5SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2)) //starting, middle list, ending
          .map(line => (line._1, (line._2, line._3)))
          .join(validNeighborsUpToYear)
          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
          .map(line => ((line._1, line._4), List(line._3)))

        val pathOf6SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf6
          .filter(line => line._1._1 < line._1._2)
        val pathOf6SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf6
          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
        val pathOf6Subtracted = pathOf6SubtractedFirstSmaller.union(pathOf6SubtractedFirstBigger)
          .reduceByKey(_:::_)
          .subtractByKey(shortestPathsUpTo5)
          .filter(line => !(line._1._1 == line._1._2))

        val shortestPathsUpTo6 = shortestPathsUpTo5.union(pathOf6Subtracted)






        val pathOf6SubtractedWithKeysSwapped = pathOf6Subtracted.map(line => ((line._1._2, line._1._1), line._2))
        val pathOf6SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf6SubtractedWithKeysSwapped.union(pathOf6Subtracted)

        val validNeighbors1992AsRDDFlatmappedPathsOf7 = pathOf6SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
          .map(line => (line._1, (line._2, line._3)))
          .join(validNeighborsUpToYear)
          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
          .map(line => ((line._1, line._4), List(line._3)))

        val pathOf7SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf7
          .filter(line => line._1._1 < line._1._2)
        val pathOf7SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf7
          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
        val pathOf7Subtracted = pathOf7SubtractedFirstSmaller.union(pathOf7SubtractedFirstBigger)
          .reduceByKey(_:::_)
          .subtractByKey(shortestPathsUpTo6)
          .filter(line => !(line._1._1 == line._1._2))

        val shortestPathsUpTo7 = shortestPathsUpTo6.union(pathOf7Subtracted)






        val pathOf7SubtractedWithKeysSwapped = pathOf7Subtracted.map(line => ((line._1._2, line._1._1), line._2))
        val pathOf7SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf7SubtractedWithKeysSwapped.union(pathOf7Subtracted)

        val validNeighbors1992AsRDDFlatmappedPathsOf8 = pathOf7SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
          .map(line => (line._1, (line._2, line._3)))
          .join(validNeighborsUpToYear)
          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
          .map(line => ((line._1, line._4), List(line._3)))

        val pathOf8SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf8
          .filter(line => line._1._1 < line._1._2)
        val pathOf8SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf8
          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
        val pathOf8Subtracted = pathOf8SubtractedFirstSmaller.union(pathOf8SubtractedFirstBigger)
          .reduceByKey(_:::_)
          .subtractByKey(shortestPathsUpTo7)
          .filter(line => !(line._1._1 == line._1._2))

        val shortestPathsUpTo8 = shortestPathsUpTo7.union(pathOf8Subtracted)





        val pathOf8SubtractedWithKeysSwapped = pathOf8Subtracted.map(line => ((line._1._2, line._1._1), line._2))
        val pathOf8SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf8SubtractedWithKeysSwapped.union(pathOf8Subtracted)

        val validNeighbors1992AsRDDFlatmappedPathsOf9 = pathOf8SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
          .map(line => (line._1, (line._2, line._3)))
          .join(validNeighborsUpToYear)
          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
          .map(line => ((line._1, line._4), List(line._3)))
        val pathOf9SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf9
          .filter(line => line._1._1 < line._1._2)
        val pathOf9SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf9
          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))

        val pathOf9Subtracted = pathOf9SubtractedFirstSmaller.union(pathOf9SubtractedFirstBigger)
          .reduceByKey(_:::_)
          .subtractByKey(shortestPathsUpTo8)
          .filter(line => !(line._1._1 == line._1._2))

        val shortestPathsUpTo9 = shortestPathsUpTo8.union(pathOf9Subtracted)








        val pathOf9SubtractedWithKeysSwapped = pathOf9Subtracted.map(line => ((line._1._2, line._1._1), line._2))
        val pathOf9SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf9SubtractedWithKeysSwapped.union(pathOf9Subtracted)

        val validNeighbors1992AsRDDFlatmappedPathsOf10 = pathOf9SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
          .map(line => (line._1, (line._2, line._3)))
          .join(validNeighborsUpToYear)
          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
          .map(line => ((line._1, line._4), List(line._3)))

        val pathOf10SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf10
          .filter(line => line._1._1 < line._1._2)
        val pathOf10SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf10
          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
        val pathOf10Subtracted = pathOf10SubtractedFirstSmaller.union(pathOf10SubtractedFirstBigger)
          .reduceByKey(_:::_)
          .subtractByKey(shortestPathsUpTo9)
          .filter(line => !(line._1._1 == line._1._2))

        val shortestPathsUpTo10 = shortestPathsUpTo9.union(pathOf10Subtracted)









        val pathOf10SubtractedWithKeysSwapped = pathOf10Subtracted.map(line => ((line._1._2, line._1._1), line._2))
        val pathOf10SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf10SubtractedWithKeysSwapped.union(pathOf10Subtracted)

        val validNeighbors1992AsRDDFlatmappedPathsOf11 = pathOf10SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
          .map(line => (line._1, (line._2, line._3)))
          .join(validNeighborsUpToYear)
          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
          .map(line => ((line._1, line._4), List(line._3)))

        val pathOf11SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf11
          .filter(line => line._1._1 < line._1._2)
        val pathOf11SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf11
          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
        val pathOf11Subtracted = pathOf11SubtractedFirstSmaller.union(pathOf11SubtractedFirstBigger)
          .reduceByKey(_:::_)
          .subtractByKey(shortestPathsUpTo10)
          .filter(line => !(line._1._1 == line._1._2))

        val shortestPathsUpTo11 = shortestPathsUpTo10.union(pathOf11Subtracted)








        val pathOf11SubtractedWithKeysSwapped = pathOf11Subtracted.map(line => ((line._1._2, line._1._1), line._2))
        val pathOf11SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf11SubtractedWithKeysSwapped.union(pathOf11Subtracted)

        val validNeighbors1992AsRDDFlatmappedPathsOf12 = pathOf11SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
          .map(line => (line._1, (line._2, line._3)))
          .join(validNeighborsUpToYear)
          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
          .map(line => ((line._1, line._4), List(line._3)))

        val pathOf12SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf12
          .filter(line => line._1._1 < line._1._2)
        val pathOf12SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf12
          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
        val pathOf12Subtracted = pathOf12SubtractedFirstSmaller.union(pathOf12SubtractedFirstBigger)
          .reduceByKey(_:::_)
          .subtractByKey(shortestPathsUpTo11)
          .filter(line => !(line._1._1 == line._1._2))

        val shortestPathsUpTo12 = shortestPathsUpTo11.union(pathOf12Subtracted)



//
//
//
//
//
//
//
//        val pathOf12SubtractedWithKeysSwapped = pathOf12Subtracted.map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf12SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf12SubtractedWithKeysSwapped.union(pathOf12Subtracted)
//
//        val validNeighbors1992AsRDDFlatmappedPathsOf13 = pathOf12SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
//          .map(line => (line._1, (line._2, line._3)))
//          .join(validNeighborsUpToYear)
//          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
//          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
//          .map(line => ((line._1, line._4), List(line._3)))
//
//        val pathOf13SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf13
//          .filter(line => line._1._1 < line._1._2)
//        val pathOf13SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf13
//          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf13Subtracted = pathOf13SubtractedFirstSmaller.union(pathOf13SubtractedFirstBigger)
//          .reduceByKey(_:::_)
//          .subtractByKey(shortestPathsUpTo12)
//          .filter(line => !(line._1._1 == line._1._2))
//
//        val shortestPathsUpTo13 = shortestPathsUpTo12.union(pathOf13Subtracted)
//
//
//
//
//
//
//
//
//        val pathOf13SubtractedWithKeysSwapped = pathOf13Subtracted.map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf13SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf13SubtractedWithKeysSwapped.union(pathOf13Subtracted)
//
//        val validNeighbors1992AsRDDFlatmappedPathsOf14 = pathOf13SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
//          .map(line => (line._1, (line._2, line._3)))
//          .join(validNeighborsUpToYear)
//          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
//          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
//          .map(line => ((line._1, line._4), List(line._3)))
//
//        val pathOf14SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf14
//          .filter(line => line._1._1 < line._1._2)
//        val pathOf14SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf14
//          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf14Subtracted = pathOf14SubtractedFirstSmaller.union(pathOf14SubtractedFirstBigger)
//          .reduceByKey(_:::_)
//          .subtractByKey(shortestPathsUpTo13)
//          .filter(line => !(line._1._1 == line._1._2))
//
//        val shortestPathsUpTo14 = shortestPathsUpTo13.union(pathOf14Subtracted)
//
//
//
//
//
//
//
//
//        val pathOf14SubtractedWithKeysSwapped = pathOf14Subtracted.map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf14SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf14SubtractedWithKeysSwapped.union(pathOf14Subtracted)
//
//        val validNeighbors1992AsRDDFlatmappedPathsOf15 = pathOf14SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
//          .map(line => (line._1, (line._2, line._3)))
//          .join(validNeighborsUpToYear)
//          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
//          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
//          .map(line => ((line._1, line._4), List(line._3)))
//
//        val pathOf15SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf15
//          .filter(line => line._1._1 < line._1._2)
//        val pathOf15SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf15
//          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf15Subtracted = pathOf15SubtractedFirstSmaller.union(pathOf15SubtractedFirstBigger)
//          .reduceByKey(_:::_)
//          .subtractByKey(shortestPathsUpTo14)
//          .filter(line => !(line._1._1 == line._1._2))
//
//        val shortestPathsUpTo15 = shortestPathsUpTo14.union(pathOf15Subtracted)
//
//
//
//
//
//
//
//
//
//
//        val pathOf15SubtractedWithKeysSwapped = pathOf15Subtracted.map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf15SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf15SubtractedWithKeysSwapped.union(pathOf15Subtracted)
//
//        val validNeighbors1992AsRDDFlatmappedPathsOf16 = pathOf15SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
//          .map(line => (line._1, (line._2, line._3)))
//          .join(validNeighborsUpToYear)
//          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
//          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
//          .map(line => ((line._1, line._4), List(line._3)))
//
//        val pathOf16SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf16
//          .filter(line => line._1._1 < line._1._2)
//        val pathOf16SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf16
//          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf16Subtracted = pathOf16SubtractedFirstSmaller.union(pathOf16SubtractedFirstBigger)
//          .reduceByKey(_:::_)
//          .subtractByKey(shortestPathsUpTo15)
//          .filter(line => !(line._1._1 == line._1._2))
//
//        val shortestPathsUpTo16 = shortestPathsUpTo15.union(pathOf16Subtracted)
//
//
//
//
//
//
//
//
//
//
//        val pathOf16SubtractedWithKeysSwapped = pathOf16Subtracted.map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf16SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf16SubtractedWithKeysSwapped.union(pathOf16Subtracted)
//
//        val validNeighbors1992AsRDDFlatmappedPathsOf17 = pathOf16SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
//          .map(line => (line._1, (line._2, line._3)))
//          .join(validNeighborsUpToYear)
//          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
//          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
//          .map(line => ((line._1, line._4), List(line._3)))
//
//        val pathOf17SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf17
//          .filter(line => line._1._1 < line._1._2)
//        val pathOf17SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf17
//          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf17Subtracted = pathOf17SubtractedFirstSmaller.union(pathOf17SubtractedFirstBigger)
//          .reduceByKey(_:::_)
//          .subtractByKey(shortestPathsUpTo16)
//          .filter(line => !(line._1._1 == line._1._2))
//        val shortestPathsUpTo17 = shortestPathsUpTo16.union(pathOf17Subtracted)
//
//
//
//
//
//
//
//
//
//        val pathOf17SubtractedWithKeysSwapped = pathOf17Subtracted.map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf17SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf17SubtractedWithKeysSwapped.union(pathOf17Subtracted)
//
//        val validNeighbors1992AsRDDFlatmappedPathsOf18 = pathOf17SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
//          .map(line => (line._1, (line._2, line._3)))
//          .join(validNeighborsUpToYear)
//          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
//          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
//          .map(line => ((line._1, line._4), List(line._3)))
//
//        val pathOf18SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf18
//          .filter(line => line._1._1 < line._1._2)
//        val pathOf18SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf18
//          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf18Subtracted = pathOf18SubtractedFirstSmaller.union(pathOf18SubtractedFirstBigger)
//          .reduceByKey(_:::_)
//          .subtractByKey(shortestPathsUpTo17)
//          .filter(line => !(line._1._1 == line._1._2))
//        val shortestPathsUpTo18 = shortestPathsUpTo17.union(pathOf18Subtracted)
//
//
//
//
//
//
//
//
//
//
//        val pathOf18SubtractedWithKeysSwapped = pathOf18Subtracted.map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf18SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf18SubtractedWithKeysSwapped.union(pathOf18Subtracted)
//
//        val validNeighbors1992AsRDDFlatmappedPathsOf19 = pathOf18SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
//          .map(line => (line._1, (line._2, line._3)))
//          .join(validNeighborsUpToYear)
//          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
//          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
//          .map(line => ((line._1, line._4), List(line._3)))
//
//        val pathOf19SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf19
//          .filter(line => line._1._1 < line._1._2)
//        val pathOf19SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf19
//          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf19Subtracted = pathOf19SubtractedFirstSmaller.union(pathOf19SubtractedFirstBigger)
//          .reduceByKey(_:::_)
//          .subtractByKey(shortestPathsUpTo18)
//          .filter(line => !(line._1._1 == line._1._2))
//        val shortestPathsUpTo19 = shortestPathsUpTo18.union(pathOf19Subtracted)
//
//
//
//
//
//
//
//
//        val pathOf19SubtractedWithKeysSwapped = pathOf19Subtracted.map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf19SubtractedWithKeysSwappedUnionedWithNonSwapped = pathOf19SubtractedWithKeysSwapped.union(pathOf19Subtracted)
//
//        val validNeighbors1992AsRDDFlatmappedPathsOf20 = pathOf19SubtractedWithKeysSwappedUnionedWithNonSwapped.map(line => (line._1._1, line._2, line._1._2))
//          .map(line => (line._1, (line._2, line._3)))
//          .join(validNeighborsUpToYear)
//          .map(line => (line._2._1._2, line._2._1._1, line._1, line._2._2))
//          .flatMap(line => line._4.map((line._1, line._2, line._3, _)))
//          .map(line => ((line._1, line._4), List(line._3)))
//
//        val pathOf20SubtractedFirstSmaller = validNeighbors1992AsRDDFlatmappedPathsOf20
//          .filter(line => line._1._1 < line._1._2)
//        val pathOf20SubtractedFirstBigger = validNeighbors1992AsRDDFlatmappedPathsOf20
//          .filter(line => line._1._1 > line._1._2).map(line => ((line._1._2, line._1._1), line._2))
//        val pathOf20Subtracted = pathOf20SubtractedFirstSmaller.union(pathOf20SubtractedFirstBigger)
//          .reduceByKey(_:::_)
//          .subtractByKey(shortestPathsUpTo19)
//          .filter(line => !(line._1._1 == line._1._2))
//        val shortestPathsUpTo20 = shortestPathsUpTo19.union(pathOf20Subtracted)






        println("Total connected pairs for 1993...")
        println("d=1 " + shortestPathsUpTo1.count())
        println("d=2 " + shortestPathsUpTo2.count())
        println("d=3 " + shortestPathsUpTo3.count())
        println("d=4 " + shortestPathsUpTo4.count())
        println("d=5 " + shortestPathsUpTo5.count())
        println("d=6 " + shortestPathsUpTo6.count())
        println("d=7 " + shortestPathsUpTo7.count())
        println("d=8 " + shortestPathsUpTo8.count())
        println("d=9 " + shortestPathsUpTo9.count())
        println("d=10 " + shortestPathsUpTo10.count())
        println("d=11 " + shortestPathsUpTo11.count())
        println("d=12 " + shortestPathsUpTo12.count())
//        println("d=13 " + shortestPathsUpTo13.count())
//        println("d=14 " + shortestPathsUpTo14.count())
//        println("d=15 " + shortestPathsUpTo15.count())
//        println("d=16 " + shortestPathsUpTo16.count())
//        println("d=17 " + shortestPathsUpTo17.count())
//        println("d=18 " + shortestPathsUpTo18.count())
//        println("d=19 " + shortestPathsUpTo19.count())
//        println("d=20 " + shortestPathsUpTo20.count())

        sc.stop()

//    counts.saveAsTextFile(args(1))
    }
}
