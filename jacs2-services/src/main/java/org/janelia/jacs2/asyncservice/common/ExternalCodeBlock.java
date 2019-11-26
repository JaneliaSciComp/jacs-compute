package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.asyncservice.utils.ScriptWriter;

import java.io.IOException;
import java.io.Writer;

public class ExternalCodeBlock {
    private final StringBuilder cmdsBuffer = new StringBuilder();

    public static class Builder {
        private final ExternalCodeBlock codeBlock;
        private final ScriptWriter codeBlockWriter;

        private Builder() {
            this.codeBlock = new ExternalCodeBlock();
            this.codeBlockWriter = this.codeBlock.getCodeWriter();
        }

        public ExternalCodeBlock build() {
            return codeBlock;
        }

        public Builder add(String line) {
            codeBlockWriter.add(line);
            return this;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isEmpty() {
        return cmdsBuffer.length() == 0;
    }

    public ScriptWriter getCodeWriter() {
         return new ScriptWriter(new Writer() {

             @Override
             public void write(char[] cbuf, int off, int len) throws IOException {
                 cmdsBuffer.append(cbuf, off, len);
             }

             @Override
             public void flush() throws IOException {
             }

             @Override
             public void close() throws IOException {
             }
         });
    }

    public String toString() {
        return cmdsBuffer.toString();
    }
}
