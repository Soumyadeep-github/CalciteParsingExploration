package parserExplorartion.calcite;

import inprogress.ApacheCalcite.supportingClasses.CalciteSqlClasses;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlVisitorImplementation extends SqlBasicVisitor<SqlNode> {
/*
    private List<String> columns;

    private Map<String, String> kv;

    public SqlVisitorImplementation(List<String> columns, Map<String, String> keyValues) {
        this.columns = columns;
        this.kv = keyValues;
    }
*/
    @Override
    public SqlNode visit(SqlCall call) {
        call.getOperator().acceptCall(this, call);
        CalciteSqlClasses z = CalciteSqlClasses.valueOf(call.getClass().getSimpleName());
        switch (z) {
            case SqlSelect:
                System.out.println("-------------------- SqlSelect -------------------");
                SqlSelect select = (SqlSelect) call;
                System.out.println("FROM : "+select.getFrom());
                System.out.println("WHERE : "+select.getWhere());
                System.out.println("GROUP : "+select.getGroup());
                System.out.println("ORDER LIST : "+select.getOrderList());
                System.out.println("WINDOW LIST : "+select.getWindowList());
                System.out.println("SELECT LIST : "+select.getSelectList());
                System.out.println("OPERATOR : "+select.getOperator());
                System.out.println("KIND : "+select.getKind());
                System.out.println("OPERAND LIST : "+select.getOperandList());
                System.out.println("HINTS : "+select.getHints());
                return select;
            case SqlJoin:
                System.out.println("-------------------- SqlJoin -------------------");
                SqlJoin join = (SqlJoin) call;
                System.out.println("CONDITION : "+join.getCondition());
                System.out.println("JOIN TYPE : "+join.getJoinType());
                System.out.println("JOIN OPERATOR : "+join.getOperator());
                System.out.println("OPERAND LIST : "+join.getOperandList());
                System.out.println("LEFT : "+join.getLeft());
                System.out.println("RIGHT : "+join.getRight());
                System.out.println("CONDITION TYPE : "+join.getConditionType());
                return join;
            case SqlOrderBy:
                System.out.println("-------------------- SqlOrderBy -------------------");
                SqlOrderBy orderBy = (SqlOrderBy) call;
                System.out.println("ORDER BY OPERATOR : "+orderBy.getOperator());
                System.out.println("OPERAND LIST : "+orderBy.getOperandList());
                return orderBy;
            case SqlWindow:
                System.out.println("-------------------- SqlWindow -------------------");
                SqlWindow window = (SqlWindow) call;
                System.out.println("WINDOW ORDER LIST : "+window.getOrderList());
                System.out.println("WINDOW DECLARED NAME : "+window.getDeclName());
                System.out.println("WINDOW PARTITION LIST : "+window.getPartitionList());
                System.out.println("WINDOW CALL : "+window.getWindowCall());
                System.out.println("WINDOW LOWER BOUND : "+window.getLowerBound());
                System.out.println("WINDOW UPPER BOUND : "+window.getUpperBound());
                System.out.println("WINDOW REF NAME : "+window.getRefName());
                return window;
            case SqlWith:
                System.out.println("-------------------- SqlWith -------------------");
                SqlWith with = (SqlWith) call;
                System.out.println("WITH LIST : "+Arrays.toString(with.withList.toArray()));
                System.out.println("WITH BODY KIND : "+with.body.getKind());
                System.out.println("WITH OPERATOR : "+with.getOperator());
                System.out.println("OPERAND LIST : "+with.getOperandList());
                System.out.println("WITH KIND : " + with.getKind());
                return with;
            case SqlWithItem:
                System.out.println("-------------------- SqlWithItem -------------------");
                SqlWithItem withItem = (SqlWithItem) call;
                System.out.println("WITH ITEM OPERATOR : "+withItem.getOperator());
                System.out.println("OPERAND LIST : "+withItem.getOperandList());
                if (withItem.columnList != null) System.out.println("WITH ITEM LIST : "+Arrays.toString(withItem.columnList.toArray()));
                System.out.println("WITH ITEM NAMES : "+Arrays.toString(withItem.name.names.toArray()));
                System.out.println("WITH ITEM SIMPLE NAME : "+withItem.name.getSimple());
                System.out.println("WITH ITEM QUERY STRING : "+withItem.query.toString());
                return withItem;
            case SqlMerge:
                System.out.println("-------------------- SqlMerge -------------------");
                SqlMerge merge = (SqlMerge) call;
                merge.getAlias();
                merge.getCondition();
                merge.getSourceSelect();
                merge.getSourceTableRef();
                merge.getTargetTable();
                merge.getUpdateCall();
                merge.getOperandList();
                return merge;
            case SqlAlter:
                System.out.println("-------------------- SqlAlter -------------------");
                SqlAlter alter = (SqlAlter) call;
                alter.getScope();
                return alter;
            case SqlBasicCall:
                System.out.println("-------------------- SqlBasicCall -------------------");
                SqlBasicCall basicCall = (SqlBasicCall) call;
                basicCall.getKind();
                return basicCall;
            default:
                System.out.println("-------------------- DEFAULT -------------------");
                call.getKind();
                return call;
        }
    }

}
