package inprogress.ApacheVelocity.code;

import inprogress.ApacheVelocity.code.bean.Product;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.context.Context;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Properties;

public class LoopsVelocity {
    public static void main(String[] args) {

        Properties props = new Properties();
        String path = System.getProperty("user.dir")+"/src/main/java/inprogress/ApacheVelocity/templates";
        props.put("file.resource.loader.path", path);
        props.setProperty("runtime.log.logsystem.class", "org.apache.velocity.runtime.log.NullLogSystem");
        Velocity.init(props);
        final Template template = Velocity.getTemplate("inprogress/ApacheVelocity/templates/loops.vm");

        final Context context = new VelocityContext();

        context.put("products", new ArrayList<Product>() {

            private static final long serialVersionUID = 1L;

            {
                add(new Product("Apple", 10));
                add(new Product("Orange", 12));
                add(new Product("Banana", 11));
            }
        });

        int[] a = {1, 2, 3};

        context.put("myArray1", a);
        context.put("str", "A B C");
        context.put("collector", "Collectors.joining");

        /* Get Writer */
        final Writer writer = new StringWriter();

        /* Merge data into velocity template */
        template.merge(context, writer);

        System.out.println(writer.toString());
//        List<String> tokens = Arrays.asList("How", "To", "Do", "In", "Java");
//
//        String joinedString = tokens.stream()
//                .collect(Collectors.joining(",", "[", "]"));

//        System.out.println(joinedString);
    }
}
