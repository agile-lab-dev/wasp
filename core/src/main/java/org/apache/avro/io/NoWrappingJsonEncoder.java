package org.apache.avro.io;

import it.agilelab.bigdata.wasp.core.utils.CompatibilityJsonGenerator;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Symbol;

import java.io.IOException;
import java.io.OutputStream;

public class NoWrappingJsonEncoder extends JsonEncoder {
    public NoWrappingJsonEncoder(Schema sc, OutputStream out) throws IOException {
        super(sc, out);
    }
/*
    public NoWrappingJsonEncoder(Schema sc, OutputStream out, boolean pretty) throws IOException {
        super(sc, out, pretty);
    }

    public NoWrappingJsonEncoder(Schema sc, CompatibilityJsonGenerator gen) throws IOException {
        super(sc, gen.getJsonGenerator());
    }
*/
    @Override
    public void writeIndex(int unionIndex) throws IOException {
        parser.advance(Symbol.UNION);
        Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
        Symbol symbol = top.getSymbol(unionIndex);
        parser.pushSymbol(symbol);
    }
}