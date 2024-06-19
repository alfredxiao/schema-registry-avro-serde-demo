package xiaoyf.demo.schemaregistry.tools.consoleconsumer;

public class MatchHelper {

    static boolean match(final String str, final String wildcardMatcher) {
        if (wildcardMatcher.contains("*") || wildcardMatcher.contains("?")) {
            return io.confluent.kafka.schemaregistry.utils.WildcardMatcher.match(str, wildcardMatcher);
        }

        return str.contains(wildcardMatcher);
    }
}
