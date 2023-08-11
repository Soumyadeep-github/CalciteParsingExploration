package succeeded.ApacheCalcite.parserExplorartion.calcite;

import org.checkerframework.checker.nullness.qual.Nullable;
import succeeded.ApacheCalcite.macroMapping.supportingClasses.CalciteSqlClasses;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import succeeded.ApacheCalcite.parserExplorartion.testingParsers.ParsedQueryElements;
import succeeded.ApacheCalcite.parserExplorartion.testingParsers.SelectKeyWordStructure;

import java.util.*;

public class SqlVisitorImplementation extends SqlBasicVisitor<SqlNode> {

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

    public SqlVisitorImplementation() {}

    private final ParsedQueryElements parsedQueryElements = new ParsedQueryElements();


    public ParsedQueryElements getParsedQueryElements() {
        return parsedQueryElements;
    }


    @Override
    public SqlNode visit(SqlCall call) {
        call.getOperator().acceptCall(this, call);
        CalciteSqlClasses z = CalciteSqlClasses.valueOf(call.getClass().getSimpleName());
        switch (z) {
            case SqlSelect:
                i += 1;
                System.out.println("-------------------- SqlSelect -------------------");
                SqlSelect select = (SqlSelect) call;
                System.out.println("I : "+i);
                assert select.getFrom() != null;
                if (select.getFrom()!= null && !select.getFrom().getKind().equals(SqlKind.JOIN)) {
                    System.out.println("FROM : "+select.getFrom());
                    if (!parsedQueryElements.FROM.contains(select.getFrom())) parsedQueryElements.FROM.add(select.getFrom());
                }
//                System.out.println("WHERE : "+select.getWhere());
                if (select.getWhere() != null) {
//                    System.out.println("WHERE KIND : " +select.getWhere().getKind());
                    parsedQueryElements.WHERE.add(select.getWhere());
                }
                if (select.getHaving() != null) {
//                    System.out.println("HAVING KIND : " +select.getHaving().getKind());
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
//                System.out.println("GROUP : "+select.getGroup());
//                System.out.println("ORDER LIST : "+select.getOrderList());
//                System.out.println("WINDOW LIST : "+select.getWindowList());
//                System.out.println("OPERATOR : "+select.getOperator());
//                System.out.println("KIND : "+select.getKind());
//                System.out.println("HINTS : "+select.getHints());
                if (select.getOrderList() != null) {
                    System.out.println("ORDER BY : "+select.getOrderList());
                    select.getOrderList().getList().forEach(x -> {if (x != null && !parsedQueryElements.ORDER_BY.contains(x)) parsedQueryElements.ORDER_BY.add(x);});
                }
                if (select.getSelectList() != null) {
//                    System.out.println("SELECT LIST : "+select.getSelectList());
//                    ArrayList<SqlNode> s = select.getSelectList().getList().stream().filter(Objects::nonNull).collect(Collectors.toCollection(ArrayList::new));
                    select.getSelectList().getList().forEach(x -> {if (x != null && !parsedQueryElements.SELECT.contains(x)) parsedQueryElements.SELECT.add(x);});
                }
                return select;
            case SqlJoin:
                i += 1;
                System.out.println("-------------------- SqlJoin -------------------");
                Map<String, String> map = new HashMap<>();
                SqlJoin join = (SqlJoin) call;
                System.out.println("I : "+i);
                map.put("JOIN_TYPE", join.getJoinType().toString());
                if(join.getCondition() != null) map.put("CONDITION", join.getCondition().toString());
//                System.out.println("CONDITION : "+join.getCondition());
//                System.out.println("JOIN TYPE : "+join.getJoinType());
//                System.out.println("JOIN OPERATOR : "+join.getOperator());
//                System.out.println("LEFT : "+join.getLeft());
//                System.out.println("RIGHT : "+join.getRight());
                if (!join.getLeft().getKind().equals(SqlKind.JOIN)) {
                    if (join.getLeft() instanceof SqlSelect) {
                        SqlSelect leftNode = ((SqlSelect) join.getLeft());
                        if (leftNode.getFrom() != null) map.put("LEFT_TBL_NAME", leftNode.getFrom().toString());
                    }
                }
                if (!join.getRight().getKind().equals(SqlKind.JOIN)) {
                    if (join.getRight() instanceof SqlSelect) {
                        SqlSelect rightNode = ((SqlSelect) join.getRight());
                        if (rightNode.getFrom() != null) map.put("RIGHT_TBL_NAME", rightNode.getFrom().toString());
                    }
                }
                if (!join.getLeft().getKind().equals(SqlKind.JOIN) ||
                    !join.getRight().getKind().equals(SqlKind.JOIN)) {
                    call.getOperator().acceptCall(this, call);
                }
//                System.out.println("LEFT : "+join.getLeft().getKind());
//                System.out.println("RIGHT : "+join.getRight().getKind());
//                System.out.println("CONDITION TYPE : "+join.getConditionType());
                parsedQueryElements.JOIN.add(map);
                return join;
            case SqlOrderBy:
                i += 1;
                System.out.println("-------------------- SqlOrderBy -------------------");
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
                    selectKeyWordStructure.alias = basicCall.getOperandList().get(1).toString();
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
                if (basicCall.getOperator() != null) System.out.println(basicCall.getOperator() instanceof SqlOperator);
                System.out.println(basicCall.operand(0));
                System.out.println(basicCall.getOperandList());
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
