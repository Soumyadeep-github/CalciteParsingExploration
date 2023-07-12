package inprogress.calciteToBeam;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SparkTable implements ScannableTable {
    /**
     * @param root
     * @return
     */
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return null;
    }

    /**
     * @param typeFactory Type factory with which to create the type
     * @return
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
    }

    /**
     * @return
     */
    @Override
    public Statistic getStatistic() {
        return null;
    }

    /**
     * @return
     */
    @Override
    public Schema.TableType getJdbcTableType() {
        return null;
    }

    /**
     * @param column
     * @return
     */
    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    /**
     * @param column The column name for which {@code isRolledUp} is true
     * @param call   The aggregate call
     * @param parent Parent node of {@code call} in the {@link SqlNode} tree
     * @param config Config settings. May be null
     * @return
     */
    @Override
    public boolean rolledUpColumnValidInsideAgg(
            String column,
            SqlCall call,
            @Nullable SqlNode parent,
            @Nullable CalciteConnectionConfig config) {
        return false;
    }
}
