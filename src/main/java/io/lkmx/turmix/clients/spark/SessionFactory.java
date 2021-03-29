package io.lkmx.turmix.clients.spark;

import io.lkmx.turmix.clients.spark.error.NoSessionAvailableException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SessionFactory {

    private static final String APP_NAME = "STENDHAL";

    // Constant to configure used factory, to be placed in config instead
    private static final String USED_KEY = "local";

    private static final Map<String, Supplier<SparkSession>> sessions;

    static {
        sessions = new HashMap<>();

        // Sets the factory for a local session which is at the moment
        // the only available session
        sessions.put(USED_KEY, () -> SparkSession.builder()
            .appName(APP_NAME)
            .master("local")
            .getOrCreate());
    }

    // Gets a Spark session using the set key
    public static SparkSession getSession() {
        Supplier<SparkSession> supplier;
        if ((supplier = sessions.get(USED_KEY)) != null) {
            // The map takes suppliers which are invoked every time because
            // Spark takes care of pulling the available session in the thread anyway
            return supplier.get();
        } else {
            throw new NoSessionAvailableException();
        }
    }

    public static void registerUdf(Consumer<UDFRegistration> consumer) {
        final SparkSession spark = getSession();
        consumer.accept(spark.udf());
    }
}
