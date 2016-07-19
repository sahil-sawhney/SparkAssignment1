import com.knoldus.Assignment
import org.scalatest.{FlatSpec, Matchers}


/**
  * the class has test cases to test the questions
  */
class AssignmentSpec extends FlatSpec with Matchers {

  val assignment: Assignment = new Assignment("./src/test/resources/inputFile")

  "Assignment" should "have correct question 2" in {

    val result: Boolean = assignment.question2("./src/test/resources/outputFile")
    result should ===(true)
  }

  "Assignment" should "have correct question 3" in {

    val result: Long = assignment.question3()
    result should ===(7598006)
  }

  "Assignment" should "have correct question 5" in {

    val result: Long = assignment.question5()
    result should ===(704)
  }

  "Assignment" should "have correct question 6" in {

    val result: Long = assignment.question6(200000)
    result should ===(10)
  }

}
