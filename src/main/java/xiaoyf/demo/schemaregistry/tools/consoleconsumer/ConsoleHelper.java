package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

public class ConsoleHelper {

    public void log(Object msg) {
        System.out.println(msg);
    }

    public void logf(String format, Object ... args) {
        System.out.printf(format, args);
    }
}
