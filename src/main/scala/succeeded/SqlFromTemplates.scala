package succeeded

import org.apache.calcite.avatica.util.{Casing, Quoting}
import org.apache.calcite.config.Lex
import org.apache.calcite.interpreter.Bindables
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.{RelNode, RelShuttleImpl}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.dialect.{AnsiSqlDialect, BigQuerySqlDialect, SnowflakeSqlDialect}
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.impl.SqlParserImpl
import org.apache.calcite.sql.pretty.SqlPrettyWriter
import org.apache.calcite.sql.validate.SqlConformanceEnum
import org.apache.calcite.sql.{SqlExplainLevel, SqlNode, SqlSelect}
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, Planner}

import java.io.PrintWriter


object SqlFromTemplates extends App {

  // @col1, @tbl1
  // COLUMN_VAR_col1, TABLE_VAR_tbl1
  //
  /*
  * {
       "name": "trans",
       "description": "transaction dataset macro",
       "type": "ads",
       "columnMacros": [
         {
           "name": "rev",
           "description": "revenue",
           "dataType": "double",
           "columnUsage": "select"
         },
         {
           "name": "luID",
           "description": "liveramp id",
           "dataType": "int",
           "columnUsage": "select"
         }
       ]
     }
     * ['rev', 'luID', 'trans']
     * str -> regex-repl -> all ocure - regexp list ->
     * ['rev', 'rev', '@rev']
     * [\s|\.]\@[a-zA-Z0-9|\.]
     * SELECT @c FROM @t WHERE @c = 'abc@c' AND @c2 = '@c1'
     * SELECT COLUMN_IDENTIFIER_c FROM TABLE_IDENTIFIER_t
     * WHERE
     * */
  private val sqlQuery: String =
  """
    |SELECT sum("@trans.@rev") as total_revenue from "@trans" WHERE "@trans.@rev" = 200""".stripMargin
//  val newQ = sqlQuery.replace("@trans", "transactions")
//    .replaceAll("@rev", "revenue")
//  val q1 = "select @gm.@id from @gm where @gm.@email = 'abc.@gm'"
//  // abc.
//  println(q1)
//  println(q1.replace("@gm", "global_merchandise")
//    .replace("@id", "id")
//    .replace("@email", "emailId"))
//  val schema = Frameworks.createRootSchema(true)
//  schema.add("transactions", new AbstractTable() {
//    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType =
//      typeFactory.builder
//        .add("revenue", SqlTypeName.INTEGER)
//        .add("value", SqlTypeName.VARCHAR)
//        .build()
//  })
//  Lex;
  val sqlParserConfig = SqlParser.config()
  //                          .withLex(Lex.ORACLE) // working
  //                          .withLex(Lex.MYSQL_ANSI) // working
  private val frameworkConfig: FrameworkConfig = Frameworks.newConfigBuilder.parserConfig(sqlParserConfig).build()
  val planner: Planner = Frameworks.getPlanner(frameworkConfig)
//  val sqlNode: SqlNode = planner.parse(newQ)
//  val plannerConfig = Frameworks.newConfigBuilder()
//    .parserConfig(SqlParser.config().withCaseSensitive(false))
//    //    .sqlValidatorConfig(SqlValidator.Config.DEFAULT)
//    .defaultSchema(schema)
//    .build()
  //
//  val nPlanner = Frameworks.getPlanner(plannerConfig)
  val parsed = planner.parse(sqlQuery)
  println(parsed)

  val sqlSelectStmt: SqlSelect = parsed.asInstanceOf[SqlSelect]
  sqlSelectStmt


//  val validate = nPlanner.validate(parse)
//  val convert: RelNode = nPlanner.rel(validate).rel
//  println(convert.getInputs)
//  println(sqlNode.toSqlString(SnowflakeSqlDialect.DEFAULT))


//  "SELECT identifier('@col1') FROM table_name WHERE predicate_column = 'predicate_value'"
/*
  private val frameworkConfig: FrameworkConfig = Frameworks.newConfigBuilder.build()
  private val planner: Planner = Frameworks.getPlanner(frameworkConfig)
  private val planner2: Planner = Frameworks.getPlanner(frameworkConfig)
  private val planner3: Planner = Frameworks.getPlanner(frameworkConfig)
  private val sqlNode: SqlNode = planner.parse(sqlQuery)
  println("-"*100)
  println("TEMPLATE QUERY : "+sqlQuery)
  println("-"*100)
//  println(sqlNode.getKind)
  private val sqlSelectStmt: SqlSelect = sqlNode.asInstanceOf[SqlSelect]

  private val setSelectColumnsQuery = "SELECT age, department" // SET list_of_columns = age
  private val selectList = planner2.parse(setSelectColumnsQuery).asInstanceOf[SqlSelect].getSelectList
  private val setFromTableQuery = "SELECT employee" // SET table_name = employee
  private val fromTable = planner3.parse(setFromTableQuery).asInstanceOf[SqlSelect].getSelectList

  sqlSelectStmt.setSelectList(selectList)
  sqlSelectStmt.setFrom(fromTable)
  private val finalQuery = sqlSelectStmt.asInstanceOf[SqlNode]
  println("-" * 100)
  println("SNOWFLAKE SQL DIALECT")
  println(finalQuery.toSqlString(SnowflakeSqlDialect.DEFAULT))
//  println(finalQuery.toSqlString(BigQuerySqlDialect.DEFAULT))
//  println(.toSqlString(SqlDialect))

//  SqlParser.configBuilder()
//    .setParserFactory(SqlParserImpl.FACTORY)
  val sqlParser = SqlParser.configBuilder
    .setParserFactory(SqlParserImpl.FACTORY)
    .setQuoting(Quoting.DOUBLE_QUOTE)
    .setQuotedCasing(Casing.UNCHANGED)
    .setConformance(SqlConformanceEnum.DEFAULT)
    .build

  println(finalQuery.toSqlString(BigQuerySqlDialect.DEFAULT))
  println("-"*100)
  println("ANSI SQL DIALECT")
  println(SqlParser.create(sqlQuery).parseQuery().toSqlString(AnsiSqlDialect.DEFAULT))
//  println(SqlParser.create(sqlQuery).parseQuery("SELECT foo").toString)

  val basicParser = SqlParser.create("SELECT id FROM arbitrary_tbl WHERE id > 6")
  println(basicParser.parseQuery().toSqlString(SnowflakeSqlDialect.DEFAULT))

  println("____________________________________")

  // 12th April 2023
/*
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
  val typeFactory = new JavaTypeFactoryImpl
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

  println(logPlan)

  /*
  * TODO: Visit each node with RelShuttle*/


*/

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
    //    .sqlValidatorConfig(SqlValidator.Config.DEFAULT)
    .defaultSchema(schema)
    .build()
  //
  val nPlanner = Frameworks.getPlanner(plannerConfig)
  val parse = nPlanner.parse("SELECT id FROM arbitrary_tbl WHERE id > 6")
  val validate = nPlanner.validate(parse)
  val convert: RelNode = nPlanner.rel(validate).rel
  println(convert.getInputs)



  println(convert.toString)

  val writer = new SqlPrettyWriter()
  validate.unparseWithParentheses(writer, 0, 0, true)

//  println(ImmutableList.of(writer.toSqlString.getSql))

//  println(writer.toSqlString.getSql)

  val relWriter = new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.ALL_ATTRIBUTES, false)
  convert.explain(relWriter)

  val shuttle = new RelShuttleImpl()
//  convert.accept(shuttle)
  println(shuttle.visit(convert).toString)

  val newShuttle = new RelShuttleImpl () {

//    def apply(rootRel: RelNode): RelNode = visit(rootRel)

    override def visit(scan: TableScan): RelNode = {
      val table = scan.getTable
      if (scan.isInstanceOf[LogicalTableScan] && Bindables.BindableTableScan.canHandle(table)) {
        return Bindables.BindableTableScan.create(scan.getCluster, table)
      }
      super.visit(scan)
    }
  }

  println(convert.accept(newShuttle).getInputs)
  println("-"*100)
//  val newVis = new SqlBasicVisitor[]

  println(convert.explain())
//  convert.accept(newShuttle).accept(newShuttle).accept(newShuttle).accept(newShuttle)
//  println(convert.getDigest)
//  println(convert.getCorrelVariable)
  println(convert)
//  println(convert.accept(newShuttle).getDigest)
//  println(newShuttle(convert))

  println(shuttle.visit(convert))


//  Frameworks.getPlanner(plannerConfig).parse("SELECT FROM tbl_1")

//  val validate = plannerN.validate(parsed)
//  planner4.rel().project()
*/
}
