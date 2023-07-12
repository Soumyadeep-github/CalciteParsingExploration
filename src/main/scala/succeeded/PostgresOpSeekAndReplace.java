package succeeded;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PostgresOpSeekAndReplace extends SqlBasicVisitor<List<SqlNode>> {

    List<SqlNode> a = new ArrayList<>();
    public List<SqlNode> call(SqlNode call) {
        return visit(SqlNodeList.of(call));
    }


    public List<SqlNode> visit(SqlNodeList nodeList) {
//        List<SqlNode> result = new ArrayList<>();
//        nodeList.forEach(node -> {
//            node.accept(this);
//            if (node instanceof SqlCall && ((SqlCall) node).getOperator().getName().equalsIgnoreCase("EXPLODE")) {
//                System.out.println("HIE");
//                result.add(node);
////                System.out.println("---- TOP LEVEL");
////                System.out.println(((SqlCall) node).getOperator().getName());
////                 ((SqlCall) node)
////                        .getOperandList()
////                        .forEach(x -> {
////                            if (x != null) {
////                                System.out.println(Objects.equals(x.toString(), ""));
////                                System.out.println(x);
////                                System.out.println(x.getKind());
////                                System.out.println("----");
////                            }
////                        });
////                ((SqlCall) node)
////                .getOperandList()
////                .stream()
////                .filter(x -> x != null && x.getKind() == SqlKind.OTHER)
////                .forEach(result::add);
//            }
//        });
//        System.out.println(result);
//        return result;
        for (SqlNode node : nodeList) {
            node.accept(this);
            if (node instanceof SqlCall && ((SqlCall) node).getOperator().getName().equalsIgnoreCase("EXPLODE")) {
                System.out.println(((SqlCall) node).getOperator().getName());
//                result.add(node);
                a.add(node);
//                return result;
//                System.out.println(result);
//                ((SqlCall) node)
//                        .getOperandList()
//                        .stream()
//                        .forEach(x -> {
//                            if (x != null) System.out.println("KIND : " + x);
//                        });
//                return ((SqlCall) node)
//                        .getOperandList()
//                        .stream()
//                        .filter(x -> x != null && x.getKind() == SqlKind.OTHER_FUNCTION)
//                        .collect(Collectors.toList());
            }
        }
//        System.out.println(a);
        return a;
    }
//    @Override public List<SqlNode> visit(SqlCall call) {
//        System.out.println("--------- Inside sqlcall ----------");
//        System.out.println(call.getOperandList());
//        System.out.println(call.getOperator().getName());
//        System.out.println(call.getOperator().kind);
//        call.getOperator().acceptCall(this, call);
//        return new ArrayList<>();
//    }
}
