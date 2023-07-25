package succeeded.ApacheCalcite.parserExplorartion.testingParsers;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class SelectKeyWordStructure {

    public String alias;

    public String kind;
    public String sqlFunctionOrOperator;
    public List<String> sqlFunctionOperators = new ArrayList<>();
    public String columnName;
    public boolean isAlias;
    public boolean isComparator;

    @Override
    public String toString() {
        return String.format("{column: '%s', kind: '%s', alias: '%s', sqlFunctionName: '%s', functionOperators: [%s], " +
                        "isAlias: %b, isComparator: %b}",
                columnName,
                kind,
                alias,
                sqlFunctionOrOperator,
                sqlFunctionOperators.stream().collect(Collectors.joining(", ")),
                isAlias,
                isComparator);
    }


}
