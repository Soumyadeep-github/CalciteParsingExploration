package inprogress.calciteToBeam

import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteTable
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode
import org.apache.calcite.adapter.java.AbstractQueryableTable
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools.Frameworks

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
  val parse = nPlanner.parse("SELECT id FROM arbitrary_tbl WHERE id > 6")
//  val parse = nPlanner.parse("SELECT id[0] FROM (SELECT ARRAY['1', '2'] AS id)")
  val validate = nPlanner.validate(parse)
  val convert: RelNode = nPlanner.rel(validate).rel
  println(convert)
  println(schema.getTable("arbitrary_tbl"))


//  Table
  println(schema.getTable("arbitrary_tbl").isInstanceOf[BeamCalciteTable])



  // DO NOT WORK
//  convert.getInputs.stream().map(x => x.asInstanceOf[BeamRelNode]).toArray
//  println(convert.asInstanceOf[BeamRelNode].buildPTransform().toString)
  val beamNode : BeamRelNode = null
  println(convert.getConvention)
}
