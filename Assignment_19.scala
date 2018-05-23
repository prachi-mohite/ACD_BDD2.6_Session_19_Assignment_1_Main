import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
import scala.util.matching.Regex

object Assignment_19 {

  def main(args: Array[String]): Unit = {

    //Create spark object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Basic Example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //create RDD by using tsextFile method of the sparkcontext object
    val newdataset = spark.sparkContext.textFile("E:\\Prachi IMP\\Hadoop\\Day 18 Spark\\Day 19 - Spark RDD\\19_Dataset.txt")

    //Task 1.1 Number of lines / Rows in the file
    //Printing each line of data from document
    newdataset.foreach(w=>println(w))
    //Printing number of Rows in Dataset
    val noOfLines = newdataset.count()
    //printing total count of rows in document
    println("no. Of lines present in the file: " + noOfLines)

    //task 1.2 Write a program to read a text file and print the number of words in the document.
    //Craete new RDD using flatmap method and split on ','
    val splittedRDDs = newdataset.flatMap(f=>f.split(',')) //Here it will return RDD which has combination of words (String + Aplhabates and Only numbers will be excluded)

    //function to verify if the passed string is numeric only or word
    def isWord(a:String):Boolean={
      val numberPattern: Regex = "^[0-9]*$".r
      numberPattern.findFirstMatchIn(a) match {
        case  Some(_) =>
          return false
        case None =>
          return true
      }
    }

    //Looking at the dataset we must get first three elements which satisfies the definition of word here
    val wordOnly = splittedRDDs.filter(f=>isWord(f))
    wordOnly.foreach(x=>println(x))

    //Now we can use the count function to get the number of words in the document
    val countOfWords = wordOnly.count()
    println("no. Of words present in the document: " + countOfWords)

    //Task 1.3 We have a document where the word separator is -, instead of space. Write a spark
    //code, to obtain the count of the total number of words present in the document.
    val splittedRDDs2 = newdataset.flatMap(f=>f.split('-'))
    val AfterSplitting = splittedRDDs2.map(x=>(x,1)).reduceByKey(_+_)
    AfterSplitting.foreach(x=>println(x))
    val countAfterSplitting = AfterSplitting.count()
    println("no. Of words present in the document after separating with -: " + countAfterSplitting)

    //Task 2
    //Problem Statement 2.1.1 : Read the text file, and create a tupled rdd.
    val tupledRDD = newdataset.map(x=>x.split(","))
    //Printing tupled RDD
    tupledRDD.collect().foreach(row => println(row.mkString(",")))

    //Problem Statement 2.1.2 : Find the count of total number of rows present.
    val countResult = tupledRDD.count()
    println("no. Of rows present in tupled RDD -: " + countResult)

    //Problem Statement 2.1.3 :What is the distinct number of subjects present in the entire school
    //We have RDD created with Only words we can take
    val subjectOnly = tupledRDD.map(item=>item(1))
    //get distinct subjects
    val distinctSubjects = subjectOnly.distinct()
    distinctSubjects.foreach(x=>println(x))

    //Problem Statement 2.1.4 : What is the count of the number of students in the school, whose name is Mathew and
    //marks is 55
    def getStudent(name:String,marks:Int) : Boolean ={
      if (name.equalsIgnoreCase("Mathew") && marks==55)
      return true
      else
        return false
    }

    val students = tupledRDD.filter(x=>getStudent(x(0),x(3).toInt))
    students.collect().foreach(row => println(row.mkString(",")))
    val countStudents = students.count()
    println("Number of students in the school, whose name is Mathew and marks is 55 -: " + countStudents)

    //Problem Statement 2.2.1:What is the count of students per grade in the school?
    //first separate the grades and map it to count 1 this is considering every student is separate
    val stdsWithGrades = tupledRDD.map(x=>(x(2),1))
    val group = stdsWithGrades.countByKey()
    group.foreach(x=>println("Students in Grade:"+x))

    //Variation : Getting the distinct student counts per grade depending upon his name
    val distinctStds = tupledRDD.map(x=>(x(2),x(0)))
    val group1 = distinctStds.distinct.countByKey()
    group1.foreach(x=>println(x))

    //Problem Statement 2.2.2 :  Find the average of each student (Note - Mathew is grade-1, is different from Mathew in
    //some other grade!
    //We have to create a RDD which is combination of 1. Grade 2. Student Name 3. Marks
    val students1 = tupledRDD.map(x=>((x(2),x(0)),x(3).toInt))
    //Get Distinct of the above RDD based on key (Grade + Student Name)
    //Group these students based on key (Grade + Student Name)
    //Use Map Values to sum the values based on Key
    val sum1 = students1.distinct.groupByKey().mapValues(x=>x.sum)
    sum1.foreach(x=>println(x))

    //Now create second RDD to calculate average of each student i.e. we must have count of subjects of every students
    //We have to create a RDD which is combination of 1. Grade 2. Student Name 3. Counter
    val students2 = tupledRDD.map(x=>((x(2),x(0)),1))
    //Sum up the counter to get the subjects count
    val count1 = students2.groupByKey().mapValues(x=>x.sum)
    count1.foreach(x=>println(x))

    //we have to join these two RDDs to get the Sum and count in one RDD so that we can get the Average of each student
    val joinedRDD = sum1.join(count1)
    joinedRDD.foreach(x=>println(x))

    //calculate average
    val averageOfEachStd = joinedRDD.map(x=>( (x._1),(x._2._1/x._2._2)))
    averageOfEachStd.foreach(x=>println("Average Of Student: " + x))

    //Problem statement 2.2.3 : What is the average score of students in each subject across all grades?
    //Calculate the total of each subject for all grades
    //Calculate number of students , those got marks for a subject
    val subjectsMarks = tupledRDD.map(x=>(x(1),x(3).toInt))
    val sum3 = subjectsMarks.groupByKey().mapValues(x=>x.sum)
    sum3.foreach(x=>println(x))
    val noOfStudents = tupledRDD.map(x=>(x(1),1)).groupByKey().mapValues(x=>x.sum)
    noOfStudents.foreach(x=>println(x))
    //join these RDD and calculate the average
    val avg2 = sum3.join(noOfStudents).map(x=>( x._1 ,(x._2._1/x._2._2)))
    avg2.foreach(x=>println("Average for Subject:"+ x._1+"==>"+x._2))


    //Problem statement 2.2.4 : What is the average score of students in each subject per grade?
    //Calculate the total of each subject for per grades
    //Calculate number of students , those got marks for a subject per grade
    //Create a RDD with Key Grade + Subject And Value as Marks
    val subMarksPerGrade =  tupledRDD.map(x=>((x(1),x(2)),x(3).toInt))
    val sum4 = subMarksPerGrade.groupByKey().mapValues(x=>x.sum)
    sum4.foreach(x=>println(x))

    //Calculate count of students
    val noOfStudents1 = tupledRDD.map(x=>((x(1),x(2)),1)).groupByKey().mapValues(x=>x.sum)
    noOfStudents1.foreach(x=>println(x))

    //join these RDD and calculate the average
    val avg3 = sum4.join(noOfStudents1).map(x=>(x._1 ,(x._2._1/x._2._2)))
    avg3.foreach(x=>println("Average for :"+ x._1+"==>"+x._2))

    //Problem statement 2.2.5 :For all students in grade-2, how many have average score greater than 50?
    //We have already calculated the average of every student of every grade in Problem statement 2.2.2
    //Need to filter the RDD to get students of grade 2 and average is above 50
   val grade2Students = averageOfEachStd.filter(x=>x._1._1.equalsIgnoreCase("grade-2") && x._2>=50)
    grade2Students.foreach(x=>println(x))

    //Problem Statement 2.3.1: Are there any students in the college that satisfy the below criteria
    // Average score per student_name across all grades is same as average score per student_name per grade
    //We have already calculated average per student_name per grade in problem statement 2.2.2
    //Calculate average per student_name
    //Create above tow RDD with key Student_Name & Value as average
    //Intersect these two RDDs and get the student_name having same average in both the above mentioned cases

    val sumPerStdName = tupledRDD.map(x=>(x(0),x(3).toInt)).groupByKey().mapValues(y=>y.sum)
    sumPerStdName.foreach(x=>println(x))
    val countOfStdName = tupledRDD.map(x=>(x(0),1)).groupByKey().mapValues(y=>y.sum)
    countOfStdName.foreach(x=>println(x))
    //join these two RDDs to get average per student_name
     val avgStdName = sumPerStdName.join(countOfStdName).map(x=>(x._1,x._2._1/x._2._2))
    //The RDD containing average for student_name per grade is averageOfEachStd now intersect this RDD with avgStdName
    val avgPerGrade  = averageOfEachStd.map(x=>(x._1._2,x._2))

    avgStdName.foreach( x=>println("Average per student_Name"+x))
    avgPerGrade.foreach( x=>println("Average per student_Name & Grade"+x))

    val commonStds = avgStdName.intersection(avgPerGrade)
    commonStds.foreach(x=>println("Students having same average per name and per grade"+x))
  }


}
