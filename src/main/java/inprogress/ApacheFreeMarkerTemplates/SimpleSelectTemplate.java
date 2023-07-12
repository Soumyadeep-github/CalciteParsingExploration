package inprogress.ApacheFreeMarkerTemplates;

import com.google.common.collect.ImmutableMap;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.*;

public class SimpleSelectTemplate {
    public static void main(String[] args) throws IOException, TemplateException {

        Configuration fmCfg = new Configuration(Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS);
        fmCfg.setClassForTemplateLoading(SimpleSelectTemplate.class, "inprogress/ApacheVelocity/templates");

        fmCfg.setDefaultEncoding("UTF-8");
        fmCfg.setLocale(Locale.US);
        fmCfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        Map<String, Object> inputs = new HashMap<>();

        List<String> columns = Arrays.asList("age", "departmentId", "name", "salary");

        Map<String, String> aliasesFromDB = ImmutableMap.of("age", "",
                                                      "departmentId", "deptId",
                                                      "name", "employeeName",
                                                      "salary", "");
        inputs.put("availableCols", aliasesFromDB);
        inputs.put("tableName", "employees_table");

        Template template = fmCfg.getTemplate("SimpleSelectSql.ftl");

        // 2.3. Generate the output

        // Write output to the console
        Writer consoleWriter = new OutputStreamWriter(System.out);
        template.process(inputs, consoleWriter);

    }
}
