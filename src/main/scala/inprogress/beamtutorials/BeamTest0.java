package inprogress.beamtutorials;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

public class BeamTest0 {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> aT = p.apply(TextIO.read().from("/Users/s0m0shx/Documents/data_for_demo/csv1/single_file_csvs/identity_graph.csv"));
    AbstractTable calciteTable = new AbstractTable() {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("id", SqlTypeName.INTEGER)
                    .add("value", SqlTypeName.VARCHAR)
                    .build();

        }
    };


    // Do stuff
//    pipeline.run();
}
