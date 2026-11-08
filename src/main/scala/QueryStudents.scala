
import com.mongodb.client.MongoClients
import com.mongodb.client.model.{Filters, Sorts}
import org.bson.Document

import scala.io.StdIn
import scala.jdk.CollectionConverters._

object QueryStudents {

  def main(args: Array[String]): Unit = {

    val client = MongoClients.create("mongodb://localhost:27017/")

    try {
      val db = client.getDatabase("university")
      val collection = db.getCollection("students")

      print("Enter Course ID: ")
      val input = StdIn.readLine()

      val courseId = try {
        input.trim.toInt
      } catch {
        case _: NumberFormatException =>
          println("Invalid Course ID! Please enter an integer.")
          return
      }

      val filter = Filters.eq("courseIds", courseId)

      val students = collection.find(filter)
        .sort(Sorts.ascending("lastName"))
        .iterator()

      try {
        while (students.hasNext) {
          val student = students.next()

          println(
            "ID: " + student.getInteger("_id") +
              " | Name: " + student.getString("firstName") +
              " " + student.getString("lastName") +
              " | GPA: " + student.getDouble("gpa")
          )
        }
      } finally {
        students.close()
      }

      val total = collection.countDocuments(filter)

      if (total == 0) {
        println("No students found for this Course ID.")
      }

      println("Total students: " + total)

      val explanation = collection
        .find(filter)
        .explain()

      val queryPlanner = explanation.get("queryPlanner", classOf[Document])
      val winningPlan = queryPlanner.get("winningPlan")

      println("Explain Plan: " + winningPlan)

    } finally {
      client.close()
    }
  }
}
