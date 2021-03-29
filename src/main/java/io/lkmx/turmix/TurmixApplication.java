package io.lkmx.turmix;

import io.lkmx.turmix.annotations.*;
import io.lkmx.turmix.clients.spark.SessionFactory;
import io.lkmx.turmix.functions.*;
import org.apache.spark.sql.UDFRegistration;
import org.reflections.Reflections;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Class that runs a Turmix application.
 */
public final class TurmixApplication {

    private static final List<DataCollector> collectors = new ArrayList<>();

    private static final List<DataPreImprover> preImprovers = new ArrayList<>();

    private static final List<DataImprover> improvers = new ArrayList<>();

    private static final List<DataStore> stores = new ArrayList<>();

    private static final List<DataValidator> validators = new ArrayList<>();

    private static final List<DataExporter> exporters = new ArrayList<>();

    /**
     * Scans the package of the class of `appType` and runs the Turmix application
     *
     * @param appType Class declared in a package that will be scanned for {@link Provider}
     */
    public static void run(Class<?> appType) {
        scanPackage(appType.getPackage().getName());
        runApplication();
    }

    public static void run(Class<?> appType, Consumer<UDFRegistration> consumer) {
        SessionFactory.registerUdf(consumer);
        run(appType);
    }

    /**
     * Runs the application by using a {@link TurmixProcessor}
     */
    private static void runApplication() {
        final TurmixProcessor processor = TurmixProcessor.builder()
            .collectors(collectors)
            .preImprovers(preImprovers)
            .improvers(improvers)
            .stores(stores)
            .validators(validators)
            .exporters(exporters)
            .build();
        processor.processAll();
    }

    private static void scanPackage(String basePackage) {
        // This API seems to be fair enough
        final Reflections reflections = new Reflections(basePackage);

        // Scan for all the classes annotated with @Provider
        final Set<Class<?>> classes = reflections.getTypesAnnotatedWith(Provider.class);

        for (Class<?> type : classes) {

            try {
                // Create an instance of each class annotated with @Provider
                // in order to scan for the methods annotated that represents each step
                final Object provider = type.getDeclaredConstructors()[0].newInstance();

                // Scannig collectors and validators has its own implementation
                scanCollectors(provider);
                scanValidators(provider);

                // Scan for the different annotations and create proper instances for each type
                scanMethodsFor(provider, PreImprover.class, DataPreImprover.class, preImprovers);
                scanMethodsFor(provider, Improver.class, DataImprover.class, improvers);
                scanMethodsFor(provider, Store.class, DataStore.class, stores);
                scanMethodsFor(provider, Exporter.class, DataExporter.class, exporters);

            } catch (Exception e) {
                throw new RuntimeException(e); // ODO We might want to use an specific exception
            }
        }
    }

    private static void scanCollectors(Object provider) throws Exception {
        final List<Method> methods = getMethodsAnnotatedWith(provider.getClass(), Collector.class, DataCollector.class);

        for (Method method : methods) {
            final DataCollector collector = (DataCollector) method.invoke(provider);
            collectors.add(collector);
        }
    }

    private static void scanValidators(Object provider) {
        final List<Method> methods = getMethodsAnnotatedWith(provider.getClass(), Validator.class);
        methods.stream()
            .sorted((a, b) -> {
                final int aOrder = a.getAnnotation(Validator.class).value();
                final int bOrder = b.getAnnotation(Validator.class).value();
                return Integer.compare(aOrder, bOrder);
            })
            .forEach(method -> {
                final DataValidator validator = (DataValidator) newProxyInstance(provider, DataValidator.class, method);
                validators.add(validator);
            });
    }

    private static <A extends Annotation, R> void scanMethodsFor(Object provider, Class<A> annotation, Class<R> returnType, List<R> list) {
        // Get the provider methods
        final List<Method> providerMethods = getMethodsAnnotatedWith(provider.getClass(), annotation);

        // For each method, we create a proxy implementation in order to populate each of the step class types
        for (Method providerMethod : providerMethods) {
            R instance = (R) newProxyInstance(provider, returnType, providerMethod);
            list.add(instance);
        }
    }

    private static <R> Object newProxyInstance(Object provider, Class<R> returnType, Method providerMethod) {
        return Proxy.newProxyInstance(returnType.getClassLoader(), new Class<?>[]{returnType}, new InvocationHandler() {

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                final Method[] declaredMethods = returnType.getDeclaredMethods();

                // We don't really bother about the toString method
                if (method.getName().equals("toString")) return toString();

                final Method methodToExecute = declaredMethods[0];

                // In case the proxy method is equal to the one we want to execute from
                // the interface, let's run the method from the provider with the proper
                // arguments
                if (method.getName().equals(methodToExecute.getName())) {
                    providerMethod.invoke(provider, args[0]);
                }

                return null;
            }
        });
    }

    private static List<Method> getMethodsAnnotatedWith(Class<?> type, Class<? extends Annotation> annotation) {
        return getMethodsAnnotatedWith(type, annotation, void.class);
    }

    private static List<Method> getMethodsAnnotatedWith(Class<?> type, Class<? extends Annotation> annotation, Class<?> returnType) {
        final List<Method> methods = new ArrayList<>();
        for (Method method : type.getDeclaredMethods()) {
            // Checks if the method has the proper annotation and
            // the return type is equal in the annotated method
            if (method.isAnnotationPresent(annotation)
                && method.getReturnType().equals(returnType)) {
                methods.add(method);
            }
        }
        return methods;
    }
}
