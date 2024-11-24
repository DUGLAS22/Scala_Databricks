// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

val archivo: String ="dbfs:/FileStore/tables/StudentPerformanceFactors.csv"

// COMMAND ----------

val students = spark.read.option("header", "true").option("inferSchema", "true").csv(archivo)

// COMMAND ----------

display(students)

// COMMAND ----------

//el promedio de horas de estudio por genero y tipo de escuela 
val question1 = students
.groupBy("Gender","School_Type")
.agg(
  avg("Hours_studied").alias("Avg_Hours_Studied")
  )

// COMMAND ----------

display(
  question1
)

// COMMAND ----------

//Tipo de influencia de compañeros con el mayor promedio de calificaciones
val question2 = students
  .groupBy("Peer_Influence")
  .agg(
    avg("Exam_Score").alias("Avg_Exam_Score"))
  .orderBy(desc("Avg_Exam_Score"))
  

// COMMAND ----------

display(
  question2
)

// COMMAND ----------

//total estudiantes con acceso a internet y actividades extras 
val question3 = students
  .where(
    (col("Internet_Access") === "Yes") && (col("Extracurricular_Activities") === "Yes")
  )
  .groupBy("Internet_Access","Extracurricular_Activities")
  .agg(
    count("*").alias("conteo")
  )
    

// COMMAND ----------

display(
  question3
)

// COMMAND ----------

//total estudiantes con nivel de educacion de los padres(secundaria) y con la distancia de su casa cerca
val question4 = students
.where(
  (col("Parental_Education_Level")==="High School") && (col("Distance_from_Home")==="Near")
)
.groupBy("Parental_Education_Level","Distance_from_Home")
.agg(
    count("*").alias("Total")
)


// COMMAND ----------

display(
  question4
)

// COMMAND ----------

//promedio de las calificaciones por el nivel de calidad del docente
val question5 = students
.groupBy("Teacher_Quality")
.agg(
   avg("Exam_Score").alias("Avg_Exam_Score")
)

// COMMAND ----------

display(
  question5
)

// COMMAND ----------

// los estudiantes con calificaciones mayores a 65 
val question6 = students
.where(
  col("Exam_Score")> 65 
)
.orderBy(desc("Exam_Score"))

// COMMAND ----------

display (
  question6
)

// COMMAND ----------

//promedio de los estudiantes con tutorias y con una motivacion alta
val question7 = students
.where(
  col("Tutoring_Sessions") > 0 && col("Motivation_Level")==="High"
)
.groupBy("Tutoring_Sessions","Motivation_Level")
.agg(
  avg("Exam_Score").alias("Avg_Exam_Score")
)

// COMMAND ----------

display(
  question7
)

// COMMAND ----------

// el promedio de tutorias por tipo de escuela
val question8 = students
.groupBy("School_Type")
.agg(
  avg("Tutoring_Sessions").alias("Avg_Tutoring_Sessions")
)

// COMMAND ----------

display(
  question8
)

// COMMAND ----------

// los estudiantes con actividad fisica y horas de sueño  tienen mejor promedio de calificacion 
val question9 = students
.groupBy("Physical_Activity","Sleep_Hours")
.agg(
  avg("Exam_Score").alias("Avg_Exam_Score")
)
.orderBy(desc("Avg_Exam_Score"))

// COMMAND ----------

display(
  question9
)

// COMMAND ----------

//el tipo de escuela con mayor promedio de calificacion con ingreso bajos
val question10 = students
.where(
  col("Family_Income")==="Low"
)
.groupBy("School_Type")
.agg(
  avg("Exam_Score").alias("Avg_Exam_Score")
)
.orderBy(desc("Avg_Exam_Score"))

// COMMAND ----------

display(
  question10
)

// COMMAND ----------

//el promedio de calificacion por tipo de escuela y la diferencia entre la calificacion y el promedio
val windowSpec = Window.partitionBy("School_Type")
val functionWindows = students
  .withColumn("Avg_Score_By_School", avg("Exam_Score").over(windowSpec))
  .withColumn("Difference_From_Avg", col("Exam_Score") - col("Avg_Score_By_School"))
  .select("Gender", "School_Type", "Exam_Score", "Avg_Score_By_School", "Difference_From_Avg")
  .orderBy("School_Type", "Gender")

// COMMAND ----------

display(
  functionWindows
)
