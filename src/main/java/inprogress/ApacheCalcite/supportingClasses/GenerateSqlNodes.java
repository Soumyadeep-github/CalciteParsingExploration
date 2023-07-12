package inprogress.ApacheCalcite.supportingClasses;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.codehaus.jackson.map.util.Comparators;

import java.util.Comparator;
import java.util.List;

public class GenerateSqlNodes {

    /* TODO: Remove static only from methods after testing*/
    private static SqlParser.Config parserConf = SqlParser.config()
            .withConformance(SqlConformanceEnum.LENIENT)
            .withLex(Lex.JAVA);

    private static SqlNode parseExpression(String sqlStmt, String fieldsOrColumns) throws SqlParseException {
        return SqlParser.create(String.format(sqlStmt, fieldsOrColumns), parserConf).parseStmt();
    }

    public static SqlNode getSelectNode(String columnNames) throws SqlParseException {
        String sqlStmt = "SELECT %s";
        return ((SqlSelect) parseExpression(sqlStmt, columnNames)).getSelectList().get(0);
    }

    public static SqlNode getSelectNode(List<String> columnNames) throws SqlParseException {
        String sqlStmt = "SELECT %s";
        return ((SqlSelect) parseExpression(sqlStmt, String.join(", ", columnNames))).getSelectList();
    }

    public static SqlNode getFromOrJoinNode(String tableName) throws SqlParseException {
        String sqlStmt = "SELECT * FROM %s";
        return ((SqlSelect) parseExpression(sqlStmt, tableName)).getFrom();
    }

//    public static SqlNode getHavingNode(String groupByCols, List<Object> ) throws SqlParseException {
//
//    }

    public static void main(String[] args) throws SqlParseException {
//        System.out.println(getSelectNode("col1"));
//        System.out.println(getSelectNode(List.of("col1", "col2")));
//        System.out.println(getFromOrJoinNode("table_foo"));
        AggregationClausesDTO dto = new AggregationClausesDTO.builder()
                .columnName("col1")
                .aggFunction("SUM")
                .comparison(ComparatorSigns.LESS_THAN_EQUAL_TO)
                .predicate(109.06)
                .filterOnly(false)
                .build();
        System.out.println(dto.filterRepr());
        System.out.println(dto.selectClauseRepr());
    }
}

/* TODO: Build expressions from DTO */
enum ComparatorSigns {
    GREATER_THAN("gt"),
    LESS_THAN("lt"),
    GREATER_THAN_EQUAL_TO("gte"),
    LESS_THAN_EQUAL_TO("lte"),
    EQUAL_TO("e"),
    NOT_EQUAL_TO("ne"),
    IN("in"),
    NOT_IN("nin"),
    LIKE("lk"),
    NOT_LIKE("nlk")
    ;

    String key;

    ComparatorSigns(String key) { this.key = key; }

    String getValue() {
        switch (this.key) {
            case "gt": return GREATER_THAN.toString();
            case "lt": return LESS_THAN.toString();
            case "gte": return GREATER_THAN_EQUAL_TO.toString();
            case "lte": return LESS_THAN_EQUAL_TO.toString();
            case "e": return EQUAL_TO.toString();
            case "ne": return NOT_EQUAL_TO.toString();
            case "in": return IN.toString();
            case "nin": return NOT_IN.toString();
            case "lk": return LIKE.toString();
            case "nlk": return NOT_LIKE.toString();
            default: throw new IllegalArgumentException();
        }
    }

    @Override
    public String toString() {
        switch (this) {
            case GREATER_THAN: return ">";
            case LESS_THAN: return "<";
            case GREATER_THAN_EQUAL_TO: return ">=";
            case LESS_THAN_EQUAL_TO: return "<=";
            case EQUAL_TO: return "=";
            case NOT_EQUAL_TO: return "<>";
            case IN: return "IN";
            case NOT_IN: return "NOT IN";
            case LIKE: return "LIKE";
            case NOT_LIKE: return "NOT LIKE";
            default: throw new IllegalArgumentException();
        }
    }
}

class AggregationClausesDTO {
    private String columnName;

    private String aggFunction;

    private Boolean filterOnly;

    private ComparatorSigns comparison;

    private Object predicate;

    public String getColumnName() {
        return columnName;
    }

    public String getAggFunction() {
        return aggFunction;
    }


    public Object getPredicate() {
        return predicate;
    }

    public Boolean getFilterOnly() {
        return filterOnly;
    }

    public ComparatorSigns getComparison() {
        return comparison;
    }

    private AggregationClausesDTO(builder builderObj) {
        this.columnName = builderObj.columnName;
        this.aggFunction = builderObj.aggFunction;
        this.filterOnly = builderObj.filterOnly;
        this.comparison = builderObj.comparison;
        this.predicate = builderObj.predicate;
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", this.aggFunction, this.columnName);
    }

    public String selectClauseRepr() {
        return String.format("%s(%s) AS %s", this.aggFunction, this.columnName, this.columnName);
    }

    public String filterRepr() {
        if (this.comparison != null) {
            return String.format("%s(%s) %s %s", this.aggFunction, this.columnName,
                    this.comparison, this.predicate);
        }
        return null;
    }

    public static class builder {
        private String columnName;

        private String aggFunction;

        private Boolean filterOnly;

        private ComparatorSigns comparison;

        private Object predicate;

        public builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public builder aggFunction(String aggFunction) {
            this.aggFunction = aggFunction;
            return this;
        }

        public builder filterOnly(Boolean filterOnly) {
            this.filterOnly = filterOnly;
            return this;
        }

        public builder comparison(ComparatorSigns comparison) {
            this.comparison = comparison;
            return this;
        }

        public builder predicate(Object predicate) {
            this.predicate = predicate;
            return this;
        }

        public AggregationClausesDTO build() {
            AggregationClausesDTO dto = new AggregationClausesDTO(this);
            return dto;
        }
    }
}