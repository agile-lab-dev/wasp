package it.agilelab.bigdata.wasp.core.utils;

import org.codehaus.jackson.JsonGenerator;

public abstract class CompatibilityJsonGenerator extends JsonGenerator {
    public JsonGenerator getJsonGenerator() {
        JsonGenerator jsonGenerator = this;
        return jsonGenerator;
    }
}