import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

val rootLogger = Logger.getRootLogger()
rootLogger.setLevel(Level.ERROR)


def saveToHive(df: DataFrame, databaseName: String, tableName: String) = {
    val tempTable = s"${tableName}_tmp_${System.currentTimeMillis / 1000}"
    df.registerTempTable(tempTable)
    sqlContext.sql(s"create table ${databaseName}.${tableName} stored as ORC as select * from ${tempTable}")
    sqlContext.dropTempTable(tempTable)

}

val employees = sqlContext.sql("select * from employees.employees")
val departments = sqlContext.sql("select * from employees.departments")
val dept_emp = sqlContext.sql("select * from employees.dept_emp")

val flat = employees.withColumn("full_name", concat(employees("last_name"), lit(", "), employees("first_name"))).
                     select("full_name", "emp_no").
                     join(dept_emp,"emp_no").
                     join(departments, "dept_no")
flat.show()


saveToHive(flat, "default", "emp_dept_flat")
