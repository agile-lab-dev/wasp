package it.agilelab.bigdata.wasp.spark.plugins.nifi;

import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ReflectiveCall {

    private static final String BRIDGE_CLASS = "it.agilelab.bigdata.wasp.spark.plugins.nifi.Bridge";


    private static final ConcurrentMap<ClassLoader, Method> extensionManagerCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<ClassLoader, Method> flowCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<ClassLoader, Method> runCache = new ConcurrentHashMap<>();

    public static Object extensionManager(File extensionsDirectory, ClassLoader classLoader) throws Exception {
        final String EXTENSION_MANAGER_METHOD_NAME = "extensionManager";
        final Class[] EXTENSION_MANAGER_METHOD_PARAMETERS = new Class[]{File.class, ClassLoader.class};
        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        Method method = extensionManagerCache.computeIfAbsent(cl, k -> {
            try {
                return Class.forName(BRIDGE_CLASS, true, k)
                        .getMethod(EXTENSION_MANAGER_METHOD_NAME, EXTENSION_MANAGER_METHOD_PARAMETERS);
            } catch (Exception e) {
                throw new RuntimeException("cannot determine extensionManager method", e);
            }
        });

        return method.invoke(null, extensionsDirectory, classLoader);
    }


    public static Object flow(String flow, List<String> failurePorts, Map<String, String> variables, Object extensionManager) throws Exception {

        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        Method method = flowCache.computeIfAbsent(cl, k -> {
            try {

                Class<?> extensionManagerClass = Class.forName("org.apache.nifi.nar.ExtensionManager", true, k);

                final String FLOW_METHOD_NAME = "flow";
                final Class[] FLOW_METHOD_PARAMETERS = new Class[]{String.class, List.class, Map.class, extensionManagerClass};

                return Class.forName(BRIDGE_CLASS, true, k)
                        .getMethod(FLOW_METHOD_NAME, FLOW_METHOD_PARAMETERS);
            } catch (Exception e) {
                throw new RuntimeException("cannot determine flow method", e);
            }
        });

        return method.invoke(null, flow, failurePorts, variables, extensionManager);


    }


    public static String run(Object flow, String data, Map<String, String> attributes) throws Exception {

        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        Method method = runCache.computeIfAbsent(cl, k -> {
            try {

                Class<?> statelessFlowClass = Class.forName("org.apache.nifi.stateless.core.StatelessFlow", true, k);

                final String RUN_METHOD_NAME = "run";
                final Class[] RUN_METHOD_PARAMETERS = new Class[]{statelessFlowClass, String.class, Map.class};

                return Class.forName(BRIDGE_CLASS, true, k)
                        .getMethod(RUN_METHOD_NAME, RUN_METHOD_PARAMETERS);
            } catch (Exception e) {
                throw new RuntimeException("cannot determine run method", e);
            }
        });

        return (String) method.invoke(null, flow, data, attributes);

    }

}
