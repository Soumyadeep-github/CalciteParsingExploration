package succeeded

import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.core.{TableFunctionScan, TableScan}
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.{RelNode, RelShuttleImpl}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.Frameworks

import scala.jdk.CollectionConverters.CollectionHasAsScala

object RelShuttleBruteForce extends App {

  // Refer StatelessRelShuttleImpl from
  // dremio-oss/sabot/kernel/src/main/java/com/dremio/exec/planner/StatelessRelShuttleImpl.java

  // then refer RoutingShuttle from
  // dremio-oss/sabot/kernel/src/main/java/com/dremio/exec/planner/RoutingShuttle.java

  // then refer queryRel.accept(new RoutingShuttle() {
  //      @Override
  //      public RelNode visit(RelNode other) {
  //        if (other instanceof LogicalJoin) {
  //          hasJoin = true;
  //          if (hasAgg) {
  //            return other;
  //          }
  //        } else if ((other instanceof LogicalAggregate) || (other instanceof LogicalFilter)) {
  //          hasAgg = true;
  //          if (hasJoin) {
  //            return other;
  //          }
  //        }
  //        return super.visit(other);
  //      }
  //    }
  //    );
  //    }
  // dremio-oss/sabot/kernel/src/main/java/com/dremio/exec/planner/accelaration/DremioMaterialization.java

    abstract class RelShuttleCustom extends RelShuttleImpl {
      override def visit(scan: TableScan): RelNode =  visit(scan.asInstanceOf[RelNode])
      override def visit(scan: TableFunctionScan): RelNode = visit(scan.asInstanceOf[RelNode])
      override def visit(values: LogicalValues): RelNode = visit(values.asInstanceOf[RelNode])
      override def visit(filter: LogicalFilter): RelNode = visit(filter.asInstanceOf[RelNode])
      override def visit(calc: LogicalCalc): RelNode = visit(calc.asInstanceOf[RelNode])
      override def visit(project: LogicalProject): RelNode = visit(project.asInstanceOf[RelNode])
      override def visit(join: LogicalJoin): RelNode = visit(join.asInstanceOf[RelNode])
      override def visit(correlate: LogicalCorrelate): RelNode = visit(correlate.asInstanceOf[RelNode])
      override def visit(union: LogicalUnion): RelNode = visit(union.asInstanceOf[RelNode])
      override def visit(intersect: LogicalIntersect): RelNode = visit(intersect.asInstanceOf[RelNode])
      override def visit(minus: LogicalMinus): RelNode = visit(minus.asInstanceOf[RelNode])
      override def visit(aggregate: LogicalAggregate): RelNode = visit(aggregate.asInstanceOf[RelNode])
      override def visit(`match`: LogicalMatch): RelNode = visit(`match`.asInstanceOf[RelNode])
      override def visit(sort: LogicalSort): RelNode = visit(sort.asInstanceOf[RelNode])
      override def visit(exchange: LogicalExchange): RelNode = visit(exchange.asInstanceOf[RelNode])
      override def visit(modify: LogicalTableModify): RelNode = visit(modify.asInstanceOf[RelNode])
      override def visit(other: RelNode): RelNode = super.visit(other)
    }

    class RelShuttleCustomImpl extends RelShuttleCustom {
      override def visit(other: RelNode): RelNode = {
        if (other.isInstanceOf[LogicalJoin]) {
          println("Join found")
          println(other.asInstanceOf[LogicalJoin].getLeft.getRowType.getFieldNames)
          println(other.asInstanceOf[LogicalJoin].getRight.getRowType.getFieldNames)
          println(other.asInstanceOf[LogicalJoin].getJoinType.toString)
          // Assert JoinType as INNER
//          assert(!other.asInstanceOf[LogicalJoin].getJoinType.isOuterJoin)
          // Assert left join key
//          assert(other.asInstanceOf[LogicalJoin].getLeft.getRowType.getFieldNames.contains("departmentId"))
          // Assert right join key
//          assert(other.asInstanceOf[LogicalJoin].getRight.getRowType.getFieldNames.contains("Id"))
        } else if (other.isInstanceOf[LogicalTableScan]) {
          println("Table found")
          println(other.asInstanceOf[LogicalTableScan].getTable)
          println(other.asInstanceOf[LogicalTableScan].getTable.getRowType.getFieldNames)
        } else if (other.isInstanceOf[LogicalProject]) {
          println("Project found")
          println(other.asInstanceOf[LogicalProject].getRowType.getFieldNames)
          // Assert if selected column names are allowed
//          assert(other.asInstanceOf[LogicalProject].getRowType.getFieldNames.asScala.toList ==
//                 List("EmpId", "address", "doj", "departmentId", "Id", "Name"))
        }
        super.visit(other) // this is like a base case, when all others fail; such that a RelNode is still returned
      }
    }

  val schema = Frameworks.createRootSchema(true)
  schema.add("employee", new AbstractTable() {
    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType =
      typeFactory.builder
        .add("EmpId", SqlTypeName.VARCHAR)
        .add("address", SqlTypeName.VARCHAR)
        .add("doj", SqlTypeName.DATE)
        .add("departmentId", SqlTypeName.VARCHAR)
        .build()
  })
  schema.add("department", new AbstractTable() {
    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType =
      typeFactory.builder
        .add("Id", SqlTypeName.VARCHAR)
        .add("Name", SqlTypeName.VARCHAR)
        .build()
  })

  val parserConfig = SqlParser.config.withUnquotedCasing(Casing.UNCHANGED)
  val frameworkConfig = Frameworks.newConfigBuilder.parserConfig(parserConfig)
    .defaultSchema(schema)
    .build
  val planner = Frameworks.getPlanner(frameworkConfig)
  val sqlTree: SqlNode = planner.parse(
    "SELECT departmentId, COUNT(*) AS cnt " +
      "FROM employee JOIN department " +
      "ON employee.departmentId = department.Id " +
      "GROUP BY departmentId ORDER BY 1")
  val validSqlTree: SqlNode = planner.validate(sqlTree)
  val tree2Rel: RelNode = planner.rel(validSqlTree).rel

  val newCustShuttle = new RelShuttleCustomImpl()
//  if (tree2Rel.isInstanceOf[RelRoot]) {
//    println("REL ROOT FOUND")
//  }
  tree2Rel.accept(newCustShuttle)


  /**
   *
   * TODO : DO THIS FIRST
   *    Figure out a way to include this logic with a ConverterRule
   *
   * TODO : Implement the above in SqlShuttle again before improving this.
   *    So that we can show that such queries can be parsed without schema.
   *
   *
   */
}
