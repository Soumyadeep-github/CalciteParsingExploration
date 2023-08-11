package succeeded.ApacheCalcite.parserExplorartion.testingParsers;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;
import succeeded.ApacheCalcite.parserExplorartion.calcite.SqlVisitorImplementation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CalciteQueryParsing {

//    private SqlValidator createSqlValidator(Context context,
//                                            CalciteCatalogReader catalogReader) {
//        final SqlOperatorTable opTab0 =
//                context.config().fun(SqlOperatorTable.class,
//                        SqlStdOperatorTable.instance());
//        final SqlOperatorTable opTab =
//                ChainedSqlOperatorTable.of(opTab0, catalogReader);
//        final JavaTypeFactory typeFactory = context.getTypeFactory();
//        final CalciteConnectionConfig connectionConfig = context.config();
//        final SqlValidator.Config config = SqlValidator.Config.DEFAULT
//                .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
//                .withSqlConformance(connectionConfig.conformance())
//                .withDefaultNullCollation(connectionConfig.defaultNullCollation())
//                .withIdentifierExpansion(true);
//        return new CalciteSqlValidator(SqlStdOperatorTable.instance(), catalogReader, typeFactory,
//                config);
//    }
//    SqlValidatorImpl;

    public static void main(String[] args) throws SqlParseException, ValidationException {
        SqlParser.Config parserConfig = SqlParser.config()
//                .withConformance(SqlConformance.PRAGMATIC_2003)
                .withUnquotedCasing(Casing.UNCHANGED);
        SchemaPlus schema = Frameworks.createRootSchema(true);
        SqlValidator.Config valConf = SqlValidator.Config.DEFAULT;
        FrameworkConfig frameworksConfig = Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
//                .defaultSchema(schema)
                .build();

//        schema.add("tbl1", new AbstractTable() {
//            @Override
//            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
//                return typeFactory.builder()
//                        .add("EmpId", SqlTypeName.VARCHAR)
//                        .add("address", SqlTypeName.VARCHAR)
//                        .add("doj", SqlTypeName.DATE)
//                        .add("departmentId", SqlTypeName.VARCHAR)
//                        .build();
//            }
//        });

        schema.add("employee", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                        .add("EmpId", SqlTypeName.VARCHAR)
                        .add("address", SqlTypeName.VARCHAR)
                        .add("doj", SqlTypeName.DATE)
                        .add("departmentId", SqlTypeName.VARCHAR)
                        .build();
            }
        });

        schema.add("department", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                        .add("Id", SqlTypeName.VARCHAR)
                        .add("Name", SqlTypeName.VARCHAR)
                        .build();
            }
        });

        String query1 = "SELECT LEFT(sub.date_column, 2) AS cleaned_month,\n" +
                "       sub.day_of_week,\n" +
                "       AVG(sub.incidents) AS average_incidents\n" +
                "  FROM (\n" +
                "        SELECT day_of_week, \n" +
                "               date_column,\n" +
                "               COUNT(incident_num) AS incidents\n" +
                "          FROM tutorial.sf_crime_incidents_2014_01\n" +
                "         GROUP BY 1,2\n" +
                "       ) sub\n" +
                " GROUP BY 1,2\n" +
                " ORDER BY 1,2";
        String query2 = "SELECT\n" +
                "  artists.first_name,\n" +
                "  artists.last_name,\n" +
                "  artist_sales.sales\n" +
                "FROM artists\n" +
                "JOIN (\n" +
                "    SELECT artist_id, SUM(sales_price) AS sales\n" +
                "    FROM sales\n" +
                "    GROUP BY artist_id\n" +
                "  ) AS artist_sales\n" +
                "  ON artists.id = artist_sales.artist_id";
        String query3 = "WITH levels AS (\n" +
                "  SELECT\n" +
                "    id,\n" +
                "    first_name,\n" +
                "    last_name,\n" +
                "    superior_id,\n" +
                "    1 AS level\n" +
                "  FROM employees\n" +
                "  WHERE superior_id IS NULL\n" +
                "  UNION ALL\n" +
                "  SELECT\n" +
                "    employees.id,\n" +
                "    employees.first_name,\n" +
                "    employees.last_name,\n" +
                "    employees.superior_id,\n" +
                "    levels.level + 1\n" +
                "  FROM employees, levels\n" +
                "  WHERE employees.superior_id = levels.id\n" +
                ")\n" +
                " \n" +
                "SELECT *\n" +
                "FROM levels";
        String query4 = "WITH avg_bonus_department AS\n" +
                "    (SELECT department, AVG(bonus) AS average_bonus\n" +
                "    FROM employees\n" +
                "    GROUP BY department),\n" +
                "    above_average AS\n" +
                "    (SELECT e.department, count(*) AS employees_above_average\n" +
                "     FROM employees e\n" +
                "     JOIN avg_bonus_department avg_a\n" +
                "     ON e.department = avg_a.department\n" +
                "     WHERE bonus > average_bonus\n" +
                "     GROUP BY e.department),\n" +
                "     below_average AS\n" +
                "     (SELECT e.department, count(*) AS employees_below_average\n" +
                "     FROM employees e\n" +
                "     JOIN avg_bonus_department avg_b\n" +
                "     ON e.department = avg_b.department\n" +
                "     WHERE bonus < average_bonus\n" +
                "     GROUP BY e.department)\n" +
                "SELECT ag.department, ag.average_bonus, aa.employees_above_average, ba.employees_below_average\n" +
                "FROM avg_bonus_department ag\n" +
                "LEFT JOIN above_average aa\n" +
                "ON ag.department = aa.department\n" +
                "LEFT JOIN below_average ba\n" +
                "ON ag.department = ba.department";
        String query5 = "SELECT x, count(1) FROM foo JOIN bar ON foo.a = bar.y WHERE z IS NOT NULL GROUP BY 1 ORDER BY 2 DESC, b";
        String query6 = "SELECT * FROM (SELECT a, e, f FROM tbl1) JOIN (SELECT b, c, d FROM tbl2) ON tbl1.a = tbl2.b";
        String query7 = "SELECT country, state, city, locality, taluk, COUNT(*) as population " +
                "FROM census_table " +
                "GROUP BY 1, 2, 3, 4, 5";
        String query8 = "SELECT country, COUNT(*) AS pop FROM census GROUP BY country";
        String query9 = "SELECT country, state, COUNT(*) AS pop FROM census GROUP BY 1, state HAVING COUNT(*) > 120 ORDER BY country, state";
        String query10 = "SELECT DISTINCT(imp.BID_ID) from imp join tra where imp.id = tra.user_id";
        String query11 = "SELECT sha256(SUBSTRING('ABCDEFGHIJKLMNOP', 1, 5)) AS s";
        String query12 = "SELECT a,b,c,d FROM table1 WHERE a=1 AND b>20 AND C IN (1,2,3,4,5)";
        String query13 = "select \"t.@@id\", \"@@id\" from \"@@imp\" as i join \"@@tx\" as t where \"t.@@bid\" = \"i.@@bid\" and \"i.@@email\" = 'abc@gmail.com'";
        SqlNode sqlTree = SqlParser.create(query13, parserConfig).parseQuery();
//        SqlNode sqlTree = planner.parse(
//                "SELECT * FROM movie JOIN hubie ON movie.id = hubie.movie_id GROUP BY movie_id.category_id");
//        SqlSelect sqlTree1 = (SqlSelect) sqlTree;
        SqlVisitorImplementation custVisitor = new SqlVisitorImplementation();
        custVisitor.visit(SqlNodeList.of(sqlTree));
        ParsedQueryElements parsedQueryElements = custVisitor.getParsedQueryElements();
        System.out.println("************************************************************************");
        /* TODO :
            - Fetch operations done on each column.
            - Explore AbstractTable for both validated SqlNode and RelNode (rel2sql)
        */
        System.out.println("SELECT : "+parsedQueryElements.SELECT);
        System.out.println("GROUP BY : "+parsedQueryElements.GROUP_BY);
        System.out.println("FROM : "+parsedQueryElements.FROM);
        System.out.println("ORDER BY : "+parsedQueryElements.ORDER_BY);
        System.out.println("JOIN : "+parsedQueryElements.JOIN);
        System.out.println("COLUMN OPERATIONS : "+parsedQueryElements.COLUMN_OPERATIONS);
        System.out.println("WHERE : "+parsedQueryElements.WHERE);
        System.out.println("HAVING : "+parsedQueryElements.HAVING);
//        System.out.println("************************************************************************");
//        SqlNode sqlTree1 = SqlParser.create(query5, parserConfig).parseQuery();
//        SqlVisitorImplementation visitor2 = new SqlVisitorImplementation();
//        visitor2.visit(SqlNodeList.of(sqlTree1));
//        Map<String, ArrayList<?>> map = new HashMap<>();
//        Planner planner1 = Frameworks.getPlanner(frameworksConfig);
//        SqlNode sqlTreeNew = planner1.parse(
//                "SELECT departmentId, COUNT(*) AS cnt " +
//                        "FROM employee JOIN department " +
//                        "ON employee.departmentId = department.Id " +
//                        "GROUP BY 1 " +
//                        "ORDER BY 1");
//        SqlVisitorImplementation visitor3 = new SqlVisitorImplementation();
//        visitor3.visit(SqlNodeList.of(sqlTreeNew));
//        ParsedQueryElements parsedQueryElements3 = visitor3.getParsedQueryElements();
//        System.out.println("SELECT : "+parsedQueryElements3.SELECT);
//        System.out.println("GROUP BY : "+parsedQueryElements3.GROUP_BY);
//        System.out.println("FROM : "+parsedQueryElements3.FROM);
//        System.out.println("ORDER BY : "+parsedQueryElements3.ORDER_BY);
//        System.out.println("JOIN : "+parsedQueryElements3.JOIN);
//        System.out.println("COLUMN OPERATIONS : "+parsedQueryElements3.COLUMN_OPERATIONS);
//        System.out.println("WHERE : "+parsedQueryElements3.WHERE);
//        System.out.println("HAVING : "+parsedQueryElements3.HAVING);
//        System.out.println("SELECT : "+custVisitor.getSelect());

//        System.out.println(((SqlOrderBy) (SqlCall) sqlTreeNew).orderList);
//        System.out.println(((SqlOrderBy) sqlTreeNew).orderList);
//        System.out.println();
//        planner1.validate(sqlTreeNew);
/*
        // CUSTOM VALIDATOR
        ArrayList<Object[]> a = new ArrayList<>();

        List<Object[]> b = List.of(new List[][]{new List[]{List.of(1, "some")}});


        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        RelDataType someTblType = new RelDataTypeFactory.Builder(typeFactory)
                .add("id", SqlTypeName.INTEGER)
                .add("value", SqlTypeName.VARCHAR)
                .build();
        Commented.ListTable arbitraryTbl = new Commented.ListTable(someTblType, b);
        rootSchema.add("arbitrary_tbl", arbitraryTbl);

        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(rootSchema, Collections.singletonList(""), 
                typeFactory, config);
        SqlValidatorWithHints validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(), catalogReader, typeFactory, SqlValidator.Config.DEFAULT);
        SqlNode sqlNodeNotValidated = SqlParser.create("SELECT id FROM arbitrary_tbl WHERE id > 6 GROUP BY () ORDER BY 1").parseQuery();
        System.out.println(validator.validate(sqlNodeNotValidated));
//        System.out.println(validator.validate(sqlNodeNotValidated).toSqlString(MysqlSqlDialect.DEFAULT));
        */
    }
}
