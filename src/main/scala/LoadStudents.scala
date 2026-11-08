
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Indexes
import org.bson.Document
import org.bson.json.{JsonMode, JsonWriterSettings}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.{LocalDate, ZoneOffset}
import java.util.Date
import scala.io.Source
import scala.jdk.CollectionConverters._

object LoadStudents {

  def main(args: Array[String]): Unit = {

    val source = Source.fromFile("hw2-data.csv", "UTF-8")

    val students = try {
      source.getLines().drop(1).filter(_.trim.nonEmpty).map { line =>

        val parts = line.trim.split(",", 5)
        val lastComma = parts(4).lastIndexOf(",")

        val courseText = parts(4).substring(0, lastComma)
        val gpaText = parts(4).substring(lastComma + 1)

        val studentId = parts(0).toInt
        val studentDob = LocalDate.parse(parts(3))

        val courseList = courseText
          .stripPrefix("[")
          .stripSuffix("]")
          .split(",")
          .map(_.trim.toInt)
          .toList

        val studentGpa = gpaText.toDouble

        new Document("_id", studentId)
          .append("firstName", parts(1))
          .append("lastName", parts(2))
          .append(
            "dateOfBirth",
            Date.from(studentDob.atStartOfDay(
              ZoneOffset.UTC
            ).toInstant)
          )
          .append("courseIds", courseList.map(Int.box).asJava)
          .append("gpa", studentGpa)

      }.toList
    } finally {
      source.close()
    }

    println("Total students: " + students.size)
    println("First student: " + students.head)
    println("Last student: " + students.last)

    val client = MongoClients.create("mongodb://localhost:27017/")

    try {
      val db = client.getDatabase("university")
      val collection = db.getCollection("students")

      // Replace the existing collection, as in the Python version.
      collection.drop()

      val result = collection.insertMany(students.asJava)

      println("Inserted students: " +
        result.getInsertedIds.size())

      collection.createIndex(Indexes.ascending("gpa"))
      collection.createIndex(Indexes.ascending("courseIds"))

      println("Indexes:")

      val indexes = collection.listIndexes().iterator()

      try {
        while (indexes.hasNext) {
          println(indexes.next().getString("name"))
        }
      } finally {
        indexes.close()
      }

      // Export one JSON object per line for Spark.
      val jsonSettings = JsonWriterSettings.builder()
        .outputMode(JsonMode.RELAXED)
        .build()

      val jsonLines = students.map { student =>
        val jsonStudent = new Document(student)

        val dob = student.getDate("dateOfBirth")
          .toInstant
          .atZone(ZoneOffset.UTC)
          .toLocalDate
          .toString

        jsonStudent.put("dateOfBirth", dob)
        jsonStudent.toJson(jsonSettings)
      }

      Files.write(
        Paths.get("students.json"),
        jsonLines.asJava,
        StandardCharsets.UTF_8
      )

      println("Saved students to students.json")

    } finally {
      client.close()
    }
  }
}
