package parserExplorartion.testingParsers;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import parserExplorartion.calcite.SqlVisitorImplementation;

public class CalciteQueryParsing {
    public static void main(String[] args) throws SqlParseException {
        SqlParser.Config parserConfig = SqlParser.config()
                .withUnquotedCasing(Casing.UNCHANGED);
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
        SqlNode sqlTree = SqlParser.create(query4, parserConfig).parseQuery();
//        SqlNode sqlTree = planner.parse(
//                "SELECT * FROM movie JOIN hubie ON movie.id = hubie.movie_id GROUP BY movie_id.category_id");

        SqlVisitorImplementation custVisitor = new SqlVisitorImplementation();
        custVisitor.visit(SqlNodeList.of(sqlTree));
    }
}
