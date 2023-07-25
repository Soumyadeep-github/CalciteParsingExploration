package succeeded.ApacheCalcite.macroMapping;

import succeeded.ApacheCalcite.macroMapping.supportingClasses.CustomSqlVisitor;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.*;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.util.*;

public class ReplaceMacros {

    public static void main(String[] args) throws SqlParseException {
        SqlParser.Config parserConfig = SqlParser.config()
                .withUnquotedCasing(Casing.UNCHANGED);
        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .build();

        Planner planner = Frameworks.getPlanner(frameworkConfig);
        SqlNode sqlTree = planner.parse(
                "SELECT \"@tbl\".\"@col\" FROM \"@tbl\" "
        );
//        SqlNode sqlTree = planner.parse(
//                "SELECT * FROM movie JOIN hubie ON movie.id = hubie.movie_id GROUP BY movie_id.category_id");
        System.out.println(sqlTree.toSqlString(SnowflakeSqlDialect.DEFAULT));
        List<String> b = Arrays.asList("a", "b", "a", "c", "d");
        Set<String> a = new HashSet<>(List.of("a", "b", "c"));
        System.out.println(b.containsAll(a));
        Map<String, String> values = new HashMap<>();
        values.put("@tbl.@col", "id");
        values.put("@tbl", "employee");
        CustomSqlVisitor custVisitor = new CustomSqlVisitor(Collections.singletonList("@tbl.@col"), values);
        List<SqlNode> nodesJavaList = Arrays.asList(sqlTree);
        SqlNodeList nodes = new SqlNodeList(nodesJavaList, SqlParserPos.ZERO);
        SqlNode s = custVisitor.visit(nodes);
        System.out.println(s);
    }
}
