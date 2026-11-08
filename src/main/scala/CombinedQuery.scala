
import com.mongodb.client.MongoClients
import com.mongodb.client.model.{Filters, Sorts}
import scala.io.StdIn

object CombinedQuery {

  def main(args: Array[String]): Unit = {

    val client = MongoClients.create("mongodb://localhost:27017/")

    try {
      val db = client.getDatabase("university")
      val collection = db.getCollection("students")

      print("Enter Course ID (press Enter to skip): ")
      val courseInput = StdIn.readLine()

      val courseId = try {
        if (courseInput.trim.isEmpty) {
          None
        } else {
          Some(courseInput.trim.toInt)
        }
      } catch {
        case _: NumberFormatException =>
          println("Invalid input! Enter a valid Course ID and GPA.")
          return
      }

      print("Enter minimum GPA: ")
      val gpaInput = StdIn.readLine()

      val minGpa = try {
        gpaInput.trim.toDouble
      } catch {
        case _: NumberFormatException =>
          println("Invalid input! Enter a valid Course ID and GPA.")
          return
      }

      if (minGpa.isNaN || minGpa < 0.0 || minGpa > 4.0) {
        println("Invalid GPA! GPA must be between 0.0 and 4.0.")
        return
      }

      val query = courseId match {
        case Some(id) =>
          Filters.and(
            Filters.gt("gpa", minGpa),
            Filters.eq("courseIds", id)
          )

        case None =>
          Filters.gt("gpa", minGpa)
      }

      val students = collection.find(query)
        .sort(Sorts.descending("gpa"))
        .iterator()

      var total = 0

      try {
        while (students.hasNext) {
          val student = students.next()

          println(
            student.getInteger("_id") + " " +
              student.getString("firstName") + " " +
              student.getString("lastName") + " " +
              student.getDouble("gpa")
          )

          total += 1
        }
      } finally {
        students.close()
      }

      if (total == 0) {
        println("No matching students found.")
      }

      println("Total students: " + total)

    } finally {
      client.close()
    }
  }
}
