package succeeded

import org.apache.calcite.sql.parser.{SqlParser, SqlParserImplFactory}
import org.apache.calcite.sql.util.SqlBasicVisitor
import org.apache.calcite.sql.validate.SqlConformanceEnum
import org.apache.calcite.sql._
import org.apache.calcite.config.Lex
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl


object ParseSQLQueries extends App {

//  println(parsed)
  class VisitorChecksEverythingN extends SqlBasicVisitor[SqlNode] {
    def apply(sqlRoot: SqlNode): SqlNode = visit(
      SqlNodeList.of(sqlRoot)
    )

    override def visit(call: SqlCall): SqlNode = {
      println("------------ INSIDE SqlCall visit ---- ")
      println(call.getOperator.getName.equalsIgnoreCase("unnest"))
      if (call.getOperator.getName.equalsIgnoreCase("unnest")) {
        println("returning call")
        println(call.asInstanceOf[SqlNode])
        call
      } else {
        println("ho")
        call.getOperator.acceptCall(this, call)
      }

      //    call match {
  //      case select: SqlSelect => return select
  //      case join: SqlJoin => return join
  //      case _ => return (_: SqlNode)
  //    }
    }
  }

  class VisitorChecksEverything extends SqlBasicVisitor[Boolean] {
    def apply(sqlRoot: SqlNode): Boolean = visit(
      SqlNodeList.of(sqlRoot)
    )

    override def visit(call: SqlCall): Boolean = {
      println("------------ INSIDE SqlCall visit ---- ")
      call match {
        case select: SqlSelect =>
          println("****** SQL SELECT ******")
          println("GET GROUP " + select.getGroup)
          println("GET HAVING " + select.getHaving)
          println(select.getOperandList.get(1))
        case join: SqlJoin =>
          println("****** SQL JOIN *******")
          println("GET LEFT " + join.getLeft)
          println("GET RIGHT " + join.getRight)
          println("GET JOIN TYPE " + join.getJoinType)
        case _ =>
          if (call.getOperator.getName.equalsIgnoreCase("unnest")) println("unnest node") else println("null")
          println(call.getClass)
      }
      call.getOperator.acceptCall(this, call)
    }
  }

//  private val query1 = "select misc.abc.empid from misc.abc "
//  private val query = "select depts.name, count(emps.empid) " +
//    "from emps " +
//    "inner join depts " +
//    "on emps.deptno = depts.deptno" +
//    " group by depts.deptno, depts.name" +
//    " order by depts.name"
//  private val query = "SELECT EXPLODE(ARRAY['1','2','3','4','5','6'])"
  val query = "SELECT EXPLODE(ARRAY['1', '2'])"
  //  val query = "SELECT ['1', '2']"
  private val sqlParserConfig = SqlParser
    .configBuilder()
//    .setParserFactory(SqlBabelParserImpl.FACTORY)
    .setConformance(SqlConformanceEnum.BABEL)
    .build()
//  val obj = new VisitorChecksEverything()
  private val pQ: SqlNode = SqlParser.create(query, sqlParserConfig).parseQuery()
//  println("\n"+"\n")
  println("Printing select columns here : --")
//  obj(pQ)
  val obj1 = new PostgresOpSeekAndReplace()
  val node = obj1.call(pQ)
  println("Scala entry point : "+node)

  val customConf = SqlParser.config()
    .withLex(Lex.ORACLE)
//    .withConformance(SqlConformanceEnum.MYSQL_5)
//  println(SqlParser.create("SELECT EXPLODE(ARRAY['1', '2'])", customConf).parseQuery())
  println(SqlParser.create("SELECT IDENTIFIER('@col1'), COUNT(IDENTIFIER('@col2')) " +
    "FROM tbl2 " +
    "JOIN tbl3 " +
    "ON IDENTIFIER('@tbl2.@col1') = IDENTIFIER('@tbl3.@col1_') " +
    "GROUP BY IDENTIFIER('@col1') ORDER BY IDENTIFIER('@col1')", customConf).parseQuery())


//  new

//  println(pQ.toSqlString(SnowflakeSqlDialect.DEFAULT))
//  println(pQ.toSqlString(BigQuerySqlDialect.DEFAULT))
//  println(pQ.toSqlString(SparkSqlDialect.DEFAULT))
//  println(pQ.toSqlString(PostgresqlSqlDialect.DEFAULT))
//  println(pQ.toSqlString(AnsiSqlDialect.DEFAULT))


}

