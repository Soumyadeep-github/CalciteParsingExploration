package succeeded

import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.SqlBasicVisitor
import org.apache.calcite.sql._
import org.apache.calcite.tools.Frameworks


object HavingFilter extends App{

  class VisitorChecksAndSetsHaving extends SqlBasicVisitor[Boolean] {
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
          if (select.getGroup != null && select.getHaving == null) {
            val havingSqlNode = Frameworks.getPlanner(Frameworks.newConfigBuilder().parserConfig(SqlParser.config()
                                  .withUnquotedCasing(Casing.UNCHANGED))
                      .build()).parse("SELECT * FROM some_table GROUP BY some_col HAVING COUNT(*) > 120")
                      .asInstanceOf[SqlSelect].getHaving
            select.setHaving(havingSqlNode)
          }
          println("GET HAVING AFTER APPLICATION : " + select.getHaving)
        case join: SqlJoin =>
            println("****** SQL JOIN *******")
            println("GET LEFT " + join.getLeft)
            println("GET RIGHT " + join.getRight)
            println("GET JOIN TYPE " + join.getJoinType)
        case _ =>
            println(call.getClass)
      }
      call.getOperator.acceptCall(this, call)
    }
  }

  val parserConfig = SqlParser.config()
    .withUnquotedCasing(Casing.UNCHANGED)

  val frameworkConfig = Frameworks.newConfigBuilder()
    .parserConfig(parserConfig)
    .build()

  val planner = Frameworks.getPlanner(frameworkConfig)
  val sqlTree = planner.parse("SELECT * FROM movie JOIN hubie ON movie.id = hubie.movie_id GROUP BY movie_id.category_id")

  val filterer = new VisitorChecksAndSetsHaving()
  println(sqlTree.toSqlString(SnowflakeSqlDialect.DEFAULT))
  println("----------------------------------------")
  println(filterer(sqlTree))
  println("----------------------------------------")
  println(sqlTree.toSqlString(SnowflakeSqlDialect.DEFAULT))

}
