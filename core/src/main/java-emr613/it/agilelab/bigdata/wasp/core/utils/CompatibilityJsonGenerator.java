package it.agilelab.bigdata.wasp.core.utils;

import com.fasterxml.jackson.core.JsonGenerator;

public abstract class CompatibilityJsonGenerator extends JsonGenerator {
     public JsonGenerator getJsonGenerator() {
         return this;
    }
}