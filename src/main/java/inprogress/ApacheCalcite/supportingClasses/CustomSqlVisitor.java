package inprogress.ApacheCalcite.supportingClasses;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class CustomSqlVisitor extends SqlBasicVisitor<SqlNode> {

    private List<String> columns;

    private Map<String, String> kv;

    private GenerateSqlNodes generateSqlNodes = new GenerateSqlNodes();
    public CustomSqlVisitor(List<String> columns, Map<String, String> keyValues) {
//        this.columns = new HashSet<>(columns);
        this.columns = columns;
        this.kv = keyValues;
    }

    @Override
    public SqlNode visit(SqlCall call) {
//        CalciteSqlClasses z = CalciteSqlClasses.valueOf(call.getClass().getSimpleName());
        call.getOperator().acceptCall(this, call);
//        System.out.println("OPERATOR : "+call.getOperator());
        if (call instanceof SqlSelect) {
            SqlSelect selectNode = (SqlSelect) call;
            if (kv.containsKey(selectNode.getFrom().toString())) {
                try {
                    selectNode.setFrom(generateSqlNodes
                            .getFromOrJoinNode(kv.get(selectNode.getFrom().toString())));
                } catch (SqlParseException e) {
                    throw new RuntimeException(e);
                }
            }
//            List<String> selectionMacros = selectNode.getSelectList().stream().map(SqlNode::toString)
//                    .collect(Collectors.toList());
            SqlParserPos a = call.getParserPosition();
            List<SqlNode> op = selectNode.getSelectList().stream().map(x -> {
                if (columns.contains(x.toString())) {
                    String val = kv.get(x.toString());
                    try {
                        return GenerateSqlNodes.getSelectNode(val);
                    } catch (SqlParseException e) {
                        throw new RuntimeException(e);
                    }
                }
                return x;
            }).collect(Collectors.toList());
            selectNode.setSelectList(new SqlNodeList(op, a));
            return selectNode;
        } else if (call instanceof SqlJoin) {
            System.out.println("INSIDE JOIN");
            System.out.println(call.getOperandList());
            System.out.println(((SqlJoin) call).getCondition());
            return call;
        }
        return call;
//        switch (z) {
//            case SqlSelect:
//                System.out.println("SELECT");
//                System.out.println(call.getOperator());
//                return true;
//            case SqlJoin:
//                System.out.println("JOIN");
//                System.out.println(call.getOperator());
//            default:
//                return false;
//        }

    }

}