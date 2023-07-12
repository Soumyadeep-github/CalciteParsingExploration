package inprogress.ApacheVelocity.code;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.context.Context;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Properties;

public class SqlAggVelocity {
    public static void main(String[] args) {

//        Properties props = new Properties();
//        String path = "./src/main/java/inprogress/ApacheVelocity/templates";
//        System.out.println(path);
//        props.put("file.resource.loader.path", path);

//        ve.init(props);
        Velocity.init("./src/main/java/inprogress/ApacheVelocity/velocity.properties");

        final Template template = Velocity.getTemplate("sqlAgg.vm");


        final Context context = new VelocityContext();

        String[] b = {"id", "dept", "salary"};
        context.put("myArray", b);
        context.put("tblName", "employeeTbl");
        String[] g = {"SUM:salary"};
        context.put("agg", g);

        /* Get Writer */
        final Writer writer = new StringWriter();

        /* Merge data into velocity template */
        template.merge(context, writer);

        System.out.println(writer.toString());
    }
}
