package succeeded.ApacheCalcite.parserExplorartion.testingParsers;

import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

public class ParsedQueryElements {
    public List<SqlNode> GROUP_BY = new ArrayList<>();
    public List<SqlNode> ORDER_BY = new ArrayList<>();

    public List<SqlNode> FROM = new ArrayList<>();

    public List<SqlNode> SELECT = new ArrayList<>();

    public List<SelectKeyWordStructure> COLUMN_OPERATIONS = new ArrayList<>();

    public List<Map<String, String>> JOIN = new ArrayList<>();

    public List<SqlNode> WHERE = new ArrayList<>();

    public List<SqlNode> HAVING = new ArrayList<>();

}

