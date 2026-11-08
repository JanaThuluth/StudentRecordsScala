# HW2 — Student Records with MongoDB and Spark SQL

## Running the Project

This assignment is implemented in Scala using MongoDB and Apache Spark SQL.

Start MongoDB locally at `mongodb://localhost:27017/`, then run these Scala files in order:

1. `LoadStudents.scala` — Load the 100 records from `hw2-data.csv`, create indexes, and export `students.json`.
2. `QueryStudents.scala` — Search by Course ID.
3. `QueryByGPA.scala` — Search by minimum GPA.
4. `CombinedQuery.scala` — Search by optional Course ID and minimum GPA.
5. `SparkAnalysis.scala` — Run the SQL analyses and save/read Parquet results.

## MongoDB Query Results

| Query | Result |
|---|---:|
| Total imported students | 100 |
| Students enrolled in Course 103 | 36 |
| Students with GPA > 3.5 | 49 |
| Course 103 and GPA > 3.5 | 9 |

The Course ID query uses the multikey index `courseIds_1`. The GPA query uses the single-field index `gpa_1`. Their execution plans show `IXSCAN`.

## Spark SQL Queries and Results

The following queries are executed with `spark.sql(...)` in `SparkAnalysis.scala`.

### 4.1 — Reproducing MongoDB Queries

**Students enrolled in Course 103**

```sql
SELECT _id, firstName, lastName, gpa
FROM students
WHERE array_contains(courseIds, 103)
```

**Result:** 36 students.

**Students with GPA greater than 3.5**

```sql
SELECT _id, firstName, lastName, gpa
FROM students
WHERE gpa > 3.5
ORDER BY gpa DESC
```

**Result:** 49 students.

### 4.2 — Working with Arrays

**Create the enrollments view**

```sql
SELECT _id, firstName, lastName, gpa,
       explode(courseIds) AS courseId
FROM students
```

The result is registered as the temporary view `enrollments`. Each student appears once per enrolled course.

**Course statistics**

```sql
SELECT courseId, COUNT(*) AS totalStudents,
       ROUND(AVG(gpa), 2) AS averageGPA,
       MIN(gpa) AS minGPA,
       MAX(gpa) AS maxGPA
FROM enrollments
GROUP BY courseId
ORDER BY totalStudents DESC
```

| Course ID | Students | Average GPA | Min GPA | Max GPA |
|---|---:|---:|---:|---:|
| 101 | 48 | 3.56 | 3.1 | 3.9 |
| 105 | 47 | 3.50 | 3.1 | 3.9 |
| 104 | 44 | 3.59 | 3.1 | 3.9 |
| 102 | 43 | 3.50 | 3.1 | 3.9 |
| 103 | 36 | 3.39 | 3.1 | 3.9 |

**Number of courses per student**

```sql
SELECT size(courseIds) AS numberOfCourses,
       COUNT(*) AS totalStudents
FROM students
GROUP BY size(courseIds)
ORDER BY numberOfCourses
```

| Number of Courses | Students |
|---|---:|
| 1 | 8 |
| 2 | 66 |
| 3 | 26 |

### 4.3 — Aggregations and Window Functions

**Student counts and average GPA by birth year**

```sql
SELECT YEAR(dateOfBirth) AS birthYear,
       COUNT(*) AS totalStudents,
       ROUND(AVG(gpa), 2) AS averageGPA
FROM students
GROUP BY YEAR(dateOfBirth)
ORDER BY birthYear
```

The query displays each birth year, the number of students born in that year, and their average GPA.

**Top three GPA ranks per course**

```sql
WITH ranked_students AS (
    SELECT courseId, firstName, lastName, gpa,
           DENSE_RANK() OVER (
               PARTITION BY courseId ORDER BY gpa DESC
           ) AS studentRank
    FROM enrollments
)
SELECT courseId, firstName, lastName, gpa, studentRank
FROM ranked_students
WHERE studentRank <= 3
ORDER BY courseId, studentRank, lastName, firstName
```

**Handling ties:** Students with the same GPA in the same course receive the same rank. The next distinct GPA gets the next consecutive rank, without skipping a number. All students ranked 1, 2, or 3 are returned, so ties can result in more than three students for a course.

**Students with duplicate first and last names**

```sql
SELECT s1.firstName, s1.lastName,
       s1._id AS student1_ID, s1.dateOfBirth AS student1_DOB,
       s2._id AS student2_ID, s2.dateOfBirth AS student2_DOB
FROM students s1
JOIN students s2
  ON s1.firstName = s2.firstName
 AND s1.lastName = s2.lastName
 AND s1._id < s2._id
ORDER BY s1.lastName, s1.firstName
```

**Result:** 7 matching student pairs, showing both student IDs and dates of birth.

**Students above the average GPA of every enrolled course**

```sql
WITH course_averages AS (
    SELECT courseId, AVG(gpa) AS averageGPA
    FROM enrollments
    GROUP BY courseId
), student_course_comparison AS (
    SELECT s._id, s.firstName, s.lastName, s.gpa,
           s.courseId, a.averageGPA
    FROM enrollments s
    JOIN course_averages a ON s.courseId = a.courseId
)
SELECT _id, firstName, lastName, gpa,
       MAX(averageGPA) AS highestCourseAverage
FROM student_course_comparison
GROUP BY _id, firstName, lastName, gpa
HAVING gpa > MAX(averageGPA)
ORDER BY gpa DESC, lastName, firstName
```

**Result:** 50 students. Comparing each GPA with the maximum average among that student's courses ensures it is above every enrolled course average.

### 4.4 — Writing Results Back

The course statistics from Section 4.2 are saved in Parquet format and read back successfully:

```scala
courseStats.write.mode("overwrite").parquet("course_statistics.parquet")
val savedStats = spark.read.parquet("course_statistics.parquet")
savedStats.show()
```

**Result:** The saved Parquet data contains the five per-course statistics rows shown in Section 4.2.
