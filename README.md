## Scala sample sbt project for spark 1.6.1

### Description: 
Also inclueds resusable code for below functions

1. SparkApp - Similar to Scala App. When extended by a Spark Job creates spark context, sql context, validates arguments ects.
2. Scala App to query Redshift and postgres dbs.
3. Convert CSV to Parquet
4. Convert parquet to CSV
5. Sample wordcount expample showing SaprkApp in action.

### Dependencies:

All maven dependencies are coded in build.sbt. As a special case, download Redhsift JDBC jar from aws and copy under lib folder before
building.

### Building Jar:

Fat jar for spark cluster execution can be built using "sbt assembly" as the plugin is included.


