package examen_estructura

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Examen {

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * estudiantes (nombre, edad, calificación).
   * Realiza las siguientes operaciones:
   *
   * Muestra el esquema del DataFrame.
   * Filtra los estudiantes con una calificación mayor a 8.
   * Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    print(estudiantes.printSchema())
    println("Estudiantes con calificación mayor que 8")
    estudiantes.filter(column("calificacion") > 8).show() // No se indica si se debe mantener como resultado para próximo paso
    val resultado = estudiantes.select(column("nombre")).orderBy(desc("calificacion")).toDF() // Se consideran TODOS los estudiantes SIN FILTRO como en el test
    println("Nombres de los estudiantes ordenados por calificación")
    resultado.show()
    resultado
  }

  /** Ejercicio 2: UDF (User Defined Function)
   * Pregunta: Define una función que determine si un número es par o impar.
   * Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */

  // Se crea función base
  def esPar(valor: Int): String = {
    if (valor % 2 == 0) "Par" else "Impar"
  }

  // Se crea
  val esParUDF: UserDefinedFunction = udf((valor: Int) => esPar(valor))

  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
    numeros.withColumn("numero", esParUDF(column("numero")))
  }

  /** Ejercicio 3: Joins y agregaciones
   * Pregunta: Dados dos DataFrames,
   * uno con información de estudiantes (id, nombre)
   * y otro con calificaciones (id_estudiante, asignatura, calificacion),
   * realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    val estudiantesJoint = estudiantes.select("id", "nombre").join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
    estudiantesJoint.groupBy("id","nombre").agg(avg("calificacion").alias("promedio"))
  }


  /** Ejercicio 4: Uso de RDDs
   * Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
   */

  def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
    val rdd = spark.sparkContext.parallelize(palabras)
    rdd.map(x => (x, 1)).reduceByKey(_ + _)
  }

  /**
   * Ejercicio 5: Procesamiento de archivos
   * Pregunta: Carga un archivo CSV que contenga información sobre
   * ventas (id_venta, id_producto, cantidad, precio_unitario)
   * y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */

  def ejercicio5(ventas: DataFrame)(implicit spark: SparkSession): DataFrame = {
    var resultado = ventas.withColumn("ingreso_total", column("cantidad") * column("precio_unitario"))
    resultado = resultado.groupBy("id_producto").agg(sum("ingreso_total"))
    resultado.sort(column("id_producto").asc)
  }
}
