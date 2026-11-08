
import com.mongodb.client.MongoClients
import com.mongodb.client.model.{Filters, Sorts}
import org.bson.Document

import scala.io.StdIn

object QueryByGPA {

  def main(args: Array[String]): Unit = {

    val client = MongoClients.create("mongodb://localhost:27017/")

    try {
      val db = client.getDatabase("university")
      val collection = db.getCollection("students")

      print("Enter minimum GPA: ")
      val input = StdIn.readLine()

      val minGpa = try {
        input.trim.toDouble
      } catch {
        case _: NumberFormatException =>
          println("Invalid GPA! Please enter a number.")
          return
      }

      if (minGpa.isNaN || minGpa < 0.0 || minGpa > 4.0) {
        println("Invalid GPA! GPA must be between 0.0 and 4.0.")
        return
      }

      val filter = Filters.gt("gpa", minGpa)

      val students = collection.find(filter)
        .sort(Sorts.descending("gpa"))
        .iterator()

      try {
        while (students.hasNext) {
          val student = students.next()

          println(
            student.getString("firstName") + " " +
              student.getString("lastName") + " " +
              student.getDouble("gpa")
          )
        }
      } finally {
        students.close()
      }

      val total = collection.countDocuments(filter)

      if (total == 0) {
        println("No students found with GPA greater than " + minGpa)
      }

      println("Total students: " + total)

      val explanation = collection.find(filter).explain()

      val queryPlanner = explanation.get("queryPlanner", classOf[Document])
      val winningPlan = queryPlanner.get("winningPlan")

      println("Explain Plan: " + winningPlan)

    } finally {
      client.close()
    }
  }
}
