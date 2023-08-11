package succeeded

import org.apache.calcite.DataContext
import org.apache.calcite.config.{CalciteConnectionConfigImpl, CalciteConnectionProperty}
import org.apache.calcite.jdbc.{CalciteSchema, JavaTypeFactoryImpl}
import org.apache.calcite.linq4j.{Enumerable, Linq4j}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.plan.{ConventionTraitDef, RelOptCluster, RelOptTable}
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.{SqlNode, SqlSelect}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.{SqlValidator, SqlValidatorUtil}
import org.apache.calcite.sql2rel.{SqlToRelConverter, StandardConvertletTable}

import java.util
import java.util.{Collections, Properties}

object Commented extends App{

// 12th April 2023

  val rootSchema = CalciteSchema.createRootSchema(true)
//  val planner4 = Frameworks.getPlanner(frameworkConfig)
//  val parsed = planner4.parse("SELECT a1_c FROM arbitrary_tbl WHERE a1_c > 6")

  val TBL_DATA = util.Arrays.asList(
    Array(1, "row_1").asInstanceOf[Array[Object]],
    Array(2, "row_2").asInstanceOf[Array[Object]],
    Array(3, "row_3").asInstanceOf[Array[Object]],
    Array(4, "row_4").asInstanceOf[Array[Object]],
    Array(5, "row_5").asInstanceOf[Array[Object]],
    Array(6, "row_6").asInstanceOf[Array[Object]],
    Array(7, "row_7").asInstanceOf[Array[Object]],
    Array(8, "row_8").asInstanceOf[Array[Object]],
    Array(9, "row_9").asInstanceOf[Array[Object]],
    Array(10, "row_10").asInstanceOf[Array[Object]],
    Array(11, "row_11").asInstanceOf[Array[Object]],
    Array(12, "row_12").asInstanceOf[Array[Object]],
    Array(13, "row_13").asInstanceOf[Array[Object]],
    Array(14, "row_14").asInstanceOf[Array[Object]],
    Array(15, "row_15").asInstanceOf[Array[Object]]
  )
  val typeFactory = new JavaTypeFactoryImpl;
  val someTblType = new RelDataTypeFactory.Builder(typeFactory)
    .add("id", SqlTypeName.INTEGER)
    .add("value", SqlTypeName.VARCHAR)
    .build()


  class ListTable (private val rowType: RelDataType, private val data: util.List[Array[Object]])
    extends AbstractTable with ScannableTable {
    override def scan(root: DataContext): Enumerable[Array[Object]] = Linq4j.asEnumerable(data)

    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = rowType
  }

  val arbitraryTbl = new ListTable(someTblType, TBL_DATA)

  private def newCluster(factory: RelDataTypeFactory) = {
    val planner = new VolcanoPlanner
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
    RelOptCluster.create(planner, new RexBuilder(factory))
  }

  rootSchema.add("arbitrary_tbl", arbitraryTbl)

  val sqlNodeNotValidated = SqlParser.create("SELECT id FROM arbitrary_tbl WHERE id > 6").parseQuery()

  val props = new Properties
  props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName, "false")

  val config = new CalciteConnectionConfigImpl(props)
  val catalogReader = new CalciteCatalogReader(rootSchema, Collections.singletonList(""), typeFactory, config)

  val validator = SqlValidatorUtil.newValidator(
    SqlStdOperatorTable.instance, catalogReader, typeFactory, SqlValidator.Config.DEFAULT)

  val NOOP_EXPANDER : RelOptTable.ViewExpander = {
    (rowType: RelDataType, queryString: String, schemaPath: util.List[String], viewPath: util.List[String]) => null
  }

  val cluster = newCluster(typeFactory)

  val relConverter = new SqlToRelConverter(NOOP_EXPANDER, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.config)

  val validNode = validator.validate(sqlNodeNotValidated)

  val logPlan = relConverter.convertQuery(validNode, false, true).rel

  println(relConverter.convertQuery(validNode, false, true).toString)
//  println(relConverter.convertQuery(validNode, false, true))
//  relConverter.convertSelect(SqlParser.create("SELECT id FROM arbitrary_tbl").parseQuery().asInstanceOf[SqlSelect], true)
  println(logPlan)

}
