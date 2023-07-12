package succeeded;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class VisitorChecksJ extends SqlBasicVisitor<List<SqlNode>> {

    public List<SqlNode> call(SqlNode call) {
        SqlNodeList l = SqlNodeList.of(call);
        return visit(l);
    }

    public List<SqlNode> visit(SqlNodeList nodeList) {
//        for (SqlNode node : nodeList) {
//            node.accept(this);
//            if (node instanceof SqlCall) {
//                return ((SqlCall) node).getOperandList().stream().filter(x -> x != null && x.getKind() == SqlKind.UNNEST).collect(Collectors.toList());
//
//            }
//        }
//        return new ArrayList<>();
        List<SqlNode> result = new ArrayList<>();
        nodeList.forEach(node -> {
            node.accept(this);
            if (node instanceof SqlCall) {
                ((SqlCall) node)
                 .getOperandList()
                 .stream()
                 .filter(x -> x != null && x.getKind() == SqlKind.UNNEST)
                 .forEach(result::add);
            }
        });
        return result;
    }


}

