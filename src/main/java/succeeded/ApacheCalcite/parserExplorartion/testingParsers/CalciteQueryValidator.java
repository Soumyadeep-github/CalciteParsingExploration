//package parserExplorartion.testingParsers;
//
//
//import org.apache.calcite.prepare.CalcitePrepareImpl;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.sql.parser.SqlParseException;
//import org.apache.calcite.sql.parser.SqlParser;
//import org.apache.calcite.tools.FrameworkConfig;
//import org.apache.calcite.tools.Frameworks;
//import org.junit.Test;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.Properties;
//
//public class CalciteQueryValidator {
////    public static void main(String[] args) {
//
//    @Test
//    public void callClass() {
//        String sqlQuery = "select country, count(*) as count_country from table12 group by 1";
//
//        // Create a connection to the in-memory Apache Calcite instance
//        Properties info = new Properties();
//        info.setProperty("lex", "MYSQL");
//        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
//            // Create a framework configuration with default settings
//            FrameworkConfig config = Frameworks.newConfigBuilder().build();
//
//            // Parse the SQL query using Calcite's SQL parser
//            SqlParser sqlParser = SqlParser.create(sqlQuery, SqlParser.Config.DEFAULT);
//
//            // Get the parsed SQL statement
//            org.apache.calcite.sql.SqlNode sqlNode = sqlParser.parseStmt();
//
//            // Prepare the statement (validate and convert it into a relational algebra representation)
////            RelNode relNode = CalcitePrepareImpl.prepareSql(config, connection, sqlNode).rel;
//
//            // If the query is valid, the program will reach this point
//            System.out.println("SQL query is valid and parsed successfully!");
//            System.out.println("Relational Algebra representation:");
////            System.out.println(relNode);
//        } catch (SQLException | SqlParseException e) {
//            // If the query is invalid, an exception will be thrown
//            System.out.println("Error parsing/validating the SQL query:");
//            e.printStackTrace();
//        }
//    }
//}