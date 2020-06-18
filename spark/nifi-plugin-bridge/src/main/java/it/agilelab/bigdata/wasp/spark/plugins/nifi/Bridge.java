package it.agilelab.bigdata.wasp.spark.plugins.nifi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.stateless.bootstrap.ExtensionDiscovery;
import org.apache.nifi.stateless.bootstrap.InMemoryFlowFile;
import org.apache.nifi.stateless.core.StatelessFlow;
import org.apache.nifi.stateless.core.StatelessFlowFile;
import org.apache.nifi.stateless.core.StatelessParameterContext;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class Bridge {

    public static ExtensionManager extensionManager(File extensionsDirectory, ClassLoader classLoader) throws Exception {
        return ExtensionDiscovery.discover(extensionsDirectory, classLoader);
    }

    public static StatelessFlow flow(String flow, List<String> failurePorts, Map<String, String> variables, ExtensionManager extensionManager) throws Exception {

        ObjectMapper mapper = new ObjectMapper();

        VersionedProcessGroup versionedProcessGroup = mapper.readValue(flow, VersionedProcessGroup.class);

        Map<VariableDescriptor, String> variableWithDescriptors = variables.entrySet().stream().collect(Collectors.toMap(d -> new VariableDescriptor(d.getKey()), Map.Entry::getValue));


        VariableRegistry registry = () -> variableWithDescriptors;

        StatelessParameterContext parameterContext = new StatelessParameterContext(Collections.emptySet());

        return new StatelessFlow(versionedProcessGroup, extensionManager, registry, failurePorts, true, null, parameterContext);
    }

    public static String run(StatelessFlow flow, String data, Map<String, String> attributes) throws Exception {

        flow.enqueueFlowFile(data.getBytes(StandardCharsets.UTF_8), attributes);

        Queue<InMemoryFlowFile> q = new LinkedList<>();

        if (flow.runOnce(q)) {
            return new String(((StatelessFlowFile) q.poll()).getDataArray(), StandardCharsets.UTF_8);
        } else {
            throw new Exception("Flow execution failed, output flow files: " + q);
        }

    }
}
