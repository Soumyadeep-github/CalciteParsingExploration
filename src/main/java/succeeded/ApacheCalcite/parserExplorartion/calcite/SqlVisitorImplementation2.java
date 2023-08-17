package succeeded.ApacheCalcite.parserExplorartion.calcite;

import org.checkerframework.checker.nullness.qual.NonNull;
import succeeded.ApacheCalcite.macroMapping.supportingClasses.CalciteSqlClasses;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import succeeded.ApacheCalcite.parserExplorartion.testingParsers.ParsedQueryElements;
import succeeded.ApacheCalcite.parserExplorartion.testingParsers.SelectKeyWordStructure;

import java.util.*;
import java.util.stream.Collectors;

public class SqlVisitorImplementation2 extends SqlBasicVisitor<SqlNode> {

    //    private List<String> columns;
//
//    private Map<String, String> kv;
//
    private Integer i = 0;
//
//    public SqlVisitorImplementation(List<String> columns, Map<String, String> keyValues) {
//        this.columns = columns;
//        this.kv = keyValues;
//    }

    public SqlVisitorImplementation2() {}

    private final ParsedQueryElements parsedQueryElements = new ParsedQueryElements();


    public ParsedQueryElements getParsedQueryElements() {
        return parsedQueryElements;
    }

    public void parseJoin(SqlCall call) {
        SqlJoin join = (SqlJoin) call;
        Map<String, String> map = new HashMap<>();
        map.put("JOIN_TYPE", join.getJoinType().toString());
        if(join.getCondition() != null) map.put("CONDITION", join.getCondition().toString());
        if (!join.getLeft().getKind().equals(SqlKind.JOIN)) {
            System.out.println("KIND LEFT : "+join.getLeft().getKind());
            if (join.getLeft() instanceof SqlSelect) {
//                System.out.println("REACH");
                SqlSelect leftNode = ((SqlSelect) join.getLeft());
                if (leftNode.getFrom() != null) map.put("LEFT_TBL_NAME", leftNode.getFrom().toString());
            } else if (join.getLeft() instanceof SqlBasicCall) {
                List<String> left = parseBasicCallForTable(join.getLeft(), "JOIN");
                map.put("LEFT_TBL_NAME", left.get(0));
                if (left.size() > 1) map.put("LEFT_TBL_ALIAS", left.get(1));
            }
        }
        if (!join.getRight().getKind().equals(SqlKind.JOIN)) {
            if (join.getRight() instanceof SqlSelect) {
                SqlSelect rightNode = ((SqlSelect) join.getRight());
                if (rightNode.getFrom() != null) map.put("RIGHT_TBL_NAME", rightNode.getFrom().toString());
            } else if (join.getRight() instanceof SqlBasicCall) {
                List<String> right = parseBasicCallForTable(join.getRight(), "JOIN");
                map.put("RIGHT_TBL_NAME", right.get(0));
                if (right.size() > 1) map.put("RIGHT_TBL_ALIAS", right.get(1));
            }
        }

//                if (!join.getLeft().getKind().equals(SqlKind.JOIN)) {
//                    System.out.println("HEY");
//                    if (join.getLeft() instanceof SqlSelect) {
//                    SqlBasicCall leftNode = (SqlBasicCall) join.getLeft();
////                    System.out.println("L N contents: "+leftNode.getOperator());
//                        if (leftNode.getFrom() != null) map.put("LEFT_TBL_NAME", leftNode.getFrom().toString());
//                    }
//                }
//                if (!join.getRight().getKind().equals(SqlKind.JOIN)) {
//                    System.out.println("HEY 1");
////                    if (join.getRight() instanceof SqlSelect) {
//                    SqlSelect rightNode = ((SqlSelect) join.getRight());
//                    if (rightNode.getFrom() != null) map.put("RIGHT_TBL_NAME", rightNode.getFrom().toString());
////                    }
//                }
        if (!join.getLeft().getKind().equals(SqlKind.JOIN) ||
                !join.getRight().getKind().equals(SqlKind.JOIN)) {
            call.getOperator().acceptCall(this, call);
        }
        System.out.println(map);
        parsedQueryElements.JOIN.add(map);
    }

    public void parseSelect(SqlCall call) {
        SqlSelect select = (SqlSelect) call;
        assert select.getFrom() != null;
        if (select.getFrom().getKind().equals(SqlKind.JOIN)) {
//            SqlJoin call1 = (SqlJoin) select.getFrom();
//            System.out.println(" INSTANCE : " + call1);
            parseJoin((SqlCall) select.getFrom());
        } else {
            parseBasicCallForTable(select.getFrom(), "SELECT");
//            if (!parsedQueryElements.FROM.contains(select.getFrom())) parsedQueryElements.FROM.add(select.getFrom());
        }
        if (select.getWhere() != null) {
            parsedQueryElements.WHERE.add(select.getWhere());
        }
        if (select.getHaving() != null) {
            parsedQueryElements.HAVING.add(select.getHaving());
        }

        if (select.getGroup() != null) {
            select.getGroup().getList()
                    .stream()
                    .filter(Objects::nonNull)
                    .forEach(
                            x -> {
                                if (x.getKind() == SqlKind.LITERAL) {
                                    parsedQueryElements.GROUP_BY.add(select.getSelectList()
                                            .getList()
                                            .get(Integer.parseInt(x.toString()) - 1)
                                    );
                                } else parsedQueryElements.GROUP_BY.add(x);
                            }
                    );
        }
        if (select.getOrderList() != null) {
            select.getOrderList().getList().forEach(x -> {if (x != null && !parsedQueryElements.ORDER_BY.contains(x)) parsedQueryElements.ORDER_BY.add(x);});
        }
        if (select.getSelectList() != null) {
            select.getSelectList().getList().forEach(x -> {if (x != null && !parsedQueryElements.SELECT.contains(x)) parsedQueryElements.SELECT.add(x);});
        }
    }

//    public Map<String, String> parseBasicCallForJoin() {
//
//    }

    public List<String> parseBasicCallForTable(SqlNode node, @NonNull String source) {
        SqlBasicCall basicCall = (SqlBasicCall) node;
        switch (source) {
            case "JOIN":
            case "SELECT":
                parsedQueryElements.FROM.add(basicCall.operand(0));
                return basicCall.getOperandList().stream().map(SqlNode::toString).collect(Collectors.toList());
            default:
                return null;
        }
    }


    @Override
    public SqlNode visit(SqlCall call) {
        call.getOperator().acceptCall(this, call);
        CalciteSqlClasses z = CalciteSqlClasses.valueOf(call.getClass().getSimpleName());
        switch (z) {
            case SqlSelect:
                i += 1;
                System.out.println("-------------------- SqlSelect -------------------");
                parseSelect(call);
                return call;
            case SqlJoin:
                i += 1;
                System.out.println("-------------------- SqlJoin -------------------");
                parseJoin(call);
                return call;
            case SqlOrderBy:
                i += 1;
                System.out.println("-------------------- SqlOrderBy -------------------");
                System.out.println("BASIC CALL OR NOT : "+call.getClass().getName());
                SqlOrderBy orderBy = (SqlOrderBy) call;
                System.out.println("I : "+i);
//                System.out.println("ORDER BY OPERATOR : "+orderBy.getOperator());
//                System.out.println("ORDER BY : "+orderBy.getOperandList());
                orderBy.getOperandList().stream().filter(Objects::nonNull).filter(x -> x.getKind() == SqlKind.OTHER)
                        .forEach(x -> parsedQueryElements.ORDER_BY.addAll(((SqlNodeList) x).getList()));

//                System.out.println("OPERAND LIST : "+orderBy.getOperandList());
                return orderBy;
            case SqlWindow:
                i += 1;
                System.out.println("-------------------- SqlWindow -------------------");
                System.out.println("BASIC CALL OR NOT : "+call.getClass().getName());
                SqlWindow window = (SqlWindow) call;
                System.out.println("I : "+i);
//                System.out.println("WINDOW ORDER LIST : "+window.getOrderList());
//                System.out.println("WINDOW DECLARED NAME : "+window.getDeclName());
//                System.out.println("WINDOW PARTITION LIST : "+window.getPartitionList());
//                System.out.println("WINDOW CALL : "+window.getWindowCall());
//                System.out.println("WINDOW LOWER BOUND : "+window.getLowerBound());
//                System.out.println("WINDOW UPPER BOUND : "+window.getUpperBound());
//                System.out.println("WINDOW REF NAME : "+window.getRefName());
                return window;
            case SqlWith:
                i += 1;
                System.out.println("-------------------- SqlWith -------------------");
                System.out.println("BASIC CALL OR NOT : "+call.getClass().getName());
                SqlWith with = (SqlWith) call;
                System.out.println("I : "+i);
                System.out.println("WITH LIST : "+Arrays.toString(with.withList.toArray()));
                System.out.println("WITH BODY KIND : "+with.body.getKind());
                System.out.println("WITH OPERATOR : "+with.getOperator());
//                System.out.println("OPERAND LIST : "+with.getOperandList());
                System.out.println("WITH KIND : " + with.getKind());
                return with;
            case SqlWithItem:
                i += 1;
                System.out.println("-------------------- SqlWithItem -------------------");
                System.out.println("BASIC CALL OR NOT : "+call.getClass().getName());
                SqlWithItem withItem = (SqlWithItem) call;
                System.out.println("I : "+i);
//                System.out.println("WITH ITEM OPERATOR : "+withItem.getOperator());
//                System.out.println("OPERAND LIST : "+withItem.getOperandList());
                if (withItem.columnList != null) System.out.println("WITH ITEM LIST : "+Arrays.toString(withItem.columnList.toArray()));
                System.out.println("WITH ITEM NAMES : "+Arrays.toString(withItem.name.names.toArray()));
                System.out.println("WITH ITEM SIMPLE NAME : "+withItem.name.getSimple());
                System.out.println("WITH ITEM QUERY STRING : "+withItem.query.toString());
                return withItem;
            case SqlMerge:
                i += 1;
                System.out.println("-------------------- SqlMerge -------------------");
                System.out.println("BASIC CALL OR NOT : "+call.getClass().getName());
                SqlMerge merge = (SqlMerge) call;
                System.out.println("I : "+i);
//                merge.getAlias();
//                merge.getCondition();
//                merge.getSourceSelect();
//                merge.getSourceTableRef();
//                merge.getTargetTable();
//                merge.getUpdateCall();
//                merge.getOperandList();
                return merge;
            case SqlAlter:
                i += 1;
                System.out.println("-------------------- SqlAlter -------------------");
                System.out.println("BASIC CALL OR NOT : "+call.getClass().getName());
                SqlAlter alter = (SqlAlter) call;
                System.out.println("I : "+i);
//                alter.getScope();
                return alter;
            case SqlBasicCall:
                i += 1;
                System.out.println("-------------------- SqlBasicCall -------------------");
                SelectKeyWordStructure selectKeyWordStructure = new SelectKeyWordStructure();
                System.out.println("I : "+i);
                SqlBasicCall basicCall = (SqlBasicCall) call;
                if (basicCall.getKind() == SqlKind.AS) {
                    selectKeyWordStructure.alias = basicCall.operand(1).toString();
                    selectKeyWordStructure.isAlias = true;
                } else selectKeyWordStructure.isAlias = false;
                if (!basicCall.getKind().belongsTo(Set.of(SqlKind.AND, SqlKind.OR, SqlKind.NOT))) {
                    if (!Objects.equals(basicCall.operand(0).toString(), "*")) selectKeyWordStructure.columnName = basicCall.operand(0).toString();
                }
                Set<SqlKind> comparators = new HashSet<>(Arrays.asList(
                        SqlKind.GREATER_THAN,
                        SqlKind.GREATER_THAN_OR_EQUAL,
                        SqlKind.LESS_THAN,
                        SqlKind.LESS_THAN_OR_EQUAL,
                        SqlKind.EQUALS,
                        SqlKind.NOT_EQUALS,
                        SqlKind.IN,
                        SqlKind.NOT_IN,
                        SqlKind.IS_NULL,
                        SqlKind.IS_NOT_NULL,
                        SqlKind.IS_TRUE,
                        SqlKind.IS_NOT_TRUE,
                        SqlKind.IS_FALSE,
                        SqlKind.IS_NOT_FALSE,
                        SqlKind.LIKE,
                        SqlKind.RLIKE));
                selectKeyWordStructure.isComparator = basicCall.isA(comparators);
                selectKeyWordStructure.kind = basicCall.getKind().toString();
                selectKeyWordStructure.sqlFunctionOrOperator = basicCall.getOperator().getName();
                basicCall.getOperandList().forEach(x -> selectKeyWordStructure.sqlFunctionOperators.add(x.toString()));
                System.out.println(basicCall.getKind());
//                if (basicCall.getOperator() != null) System.out.println(basicCall.getOperator() instanceof SqlOperator);
//                System.out.println(basicCall.operand(0));
//                System.out.println(basicCall.getOperandList());
                parsedQueryElements.COLUMN_OPERATIONS.add(selectKeyWordStructure);
                return basicCall;
            default:
                i += 1;
                System.out.println("-------------------- DEFAULT -------------------");
                System.out.println("I : "+i);
                System.out.println(call.getKind());
                System.out.println(call.getOperator() instanceof SqlFunction);
                return call;
        }
    }

}
