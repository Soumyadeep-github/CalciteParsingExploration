package inprogress.calciteToBeam

import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteTable
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode
import org.apache.calcite.adapter.java.AbstractQueryableTable
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools.Frameworks
import succeeded.ParseSQLQueries.{VisitorChecksEverything, VisitorSF}

import java.util.Collections
import java.util.stream.Collectors
import scala.util.chaining.scalaUtilChainingOps

object BeamRelNodeTest extends App {


  val schema = Frameworks.createRootSchema(true)

  schema.add("arbitrary_tbl", new AbstractTable() {
    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType =
      typeFactory.builder
        .add("id", SqlTypeName.INTEGER)
        .add("value", SqlTypeName.VARCHAR)
        .build()
  })

  val plannerConfig = Frameworks.newConfigBuilder()
    .parserConfig(SqlParser.config().withCaseSensitive(false))
    .defaultSchema(schema)
    .build()

  val nPlanner = Frameworks.getPlanner(plannerConfig)
  //  val parse: SqlNode = nPlanner.parse("SELECT id FROM arbitrary_tbl WHERE id > 6")
  val parse: SqlNode = nPlanner.parse(
    """
      |SELECT
      |  artists.first_name,
      |  artists.last_name,
      |  artist_sales.sales
      |FROM artists
      |JOIN (
      |    SELECT artist_id, SUM(sales_price) AS sales
      |    FROM sales
      |    GROUP BY artist_id
      |  ) AS artist_sales
      |  ON artists.id = artist_sales.artist_id
      |""".stripMargin)
//    """
//      |SELECT LEFT(sub.date, 2) AS cleaned_month,
//      |       sub.day_of_week,
//      |       AVG(sub.incidents) AS average_incidents
//      |  FROM (
//      |        SELECT day_of_week,
//      |               date,
//      |               COUNT(incidnt_num) AS incidents
//      |          FROM tutorial.sf_crime_incidents_2014_01
//      |         GROUP BY 1,2
//      |       ) sub
//      | GROUP BY 1,2
//      | ORDER BY 1,2
//      |""".stripMargin)
  val obj = new VisitorSF()
  obj(parse)
//  println(parse)
  //  val parse = nPlanner.parse("SELECT id[0] FROM (SELECT ARRAY['1', '2'] AS id)")
  /*
  val validate: SqlNode = nPlanner.validate(parse)
  val convert: RelNode = nPlanner.rel(validate).rel
  println(convert)
  println(schema.getTable("arbitrary_tbl"))


//  Table
  println(schema.getTable("arbitrary_tbl").isInstanceOf[BeamCalciteTable])



  // DOES NOT WORK
//  convert.getInputs.stream().map(x => x.asInstanceOf[BeamRelNode]).toArray
//  println(convert.asInstanceOf[BeamRelNode].buildPTransform().toString)
  val beamNode : BeamRelNode = null
  println(convert.explain())

   */
}
