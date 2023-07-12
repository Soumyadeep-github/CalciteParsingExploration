package inprogress;

import com.google.common.collect.ImmutableMap;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import inprogress.ApacheFreeMarkerTemplates.SimpleAggregationTemplate;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.*;

public class SnowflakeCreateQuery {
    public static void main(String[] args) throws IOException, TemplateException {

        Configuration fmCfg = new Configuration(Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS);
        fmCfg.setClassForTemplateLoading(SimpleAggregationTemplate.class, "inprogress/ApacheVelocity/templates");

        fmCfg.setDefaultEncoding("UTF-8");
        fmCfg.setLocale(Locale.US);
        fmCfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        Map<String, Object> inputs = new HashMap<>();

        Map<String, String> availableCols = ImmutableMap.of("age", "INT",
                "departmentId", "VARCHAR(20)",
                "name", "VARCHAR(100)",
                "salary", "BIGINT");
        inputs.put("availableColumns", availableCols);
        inputs.put("tableName", "employees_table");

//        Map<String, String> aliasMap = new HashMap<>();
//        columns.forEach(x -> aliasMap.put(x, aliasesFromDB.getOrDefault(x, x)));

        Template template = fmCfg.getTemplate("createSnowflakeTable.ftl");

        // 2.3. Generate the output

        // Write output to the console
        Writer consoleWriter = new OutputStreamWriter(System.out);
        template.process(inputs, consoleWriter);

    }
}
