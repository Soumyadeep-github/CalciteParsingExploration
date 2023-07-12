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

public class HelloWorldVelocity {
    public static void main(String[] args) {
/*
        VelocityEngine ve = new VelocityEngine();
//        Path path = Paths.get("src/main/java/inprogress/ApacheVelocity/templates/");
        ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
        ve.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
//        ve.setProperty("file.resource.loader.path", path);

        ve.init();

        VelocityContext context = new VelocityContext();

        Template t = ve.getTemplate( "templates/helloWorld.vm" );
        context.put("name", "Vova");
        context.put("surname", "Ivanov");
        StringWriter writer = new StringWriter();
        t.merge( context, writer );
        */
        Properties props = new Properties();
        String path = System.getProperty("user.dir")+"/src/main/java/inprogress/ApacheVelocity/templates";
        props.put("file.resource.loader.path", path);
        props.setProperty("runtime.log.logsystem.class", "org.apache.velocity.runtime.log.NullLogSystem");
        Velocity.init(props);
        /* Get Velocity template */
        final Template template = Velocity.getTemplate("inprogress/ApacheVelocity/templates/helloWorld.vm");

        /* Get Velocity context */
        final Context context = new VelocityContext();

        context.put("name", "Soumyadeep");
        context.put("surname", "Mukhopadhyay");

        /* Get Writer */
        final Writer writer = new StringWriter();

        /* Merge data into velocity template */
        template.merge(context, writer);

        System.out.println(writer);
    }
}
