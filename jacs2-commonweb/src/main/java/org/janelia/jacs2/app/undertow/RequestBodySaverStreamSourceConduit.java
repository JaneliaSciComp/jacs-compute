package org.janelia.jacs2.app.undertow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Consumer;

import org.xnio.channels.StreamSinkChannel;
import org.xnio.conduits.AbstractStreamSourceConduit;
import org.xnio.conduits.StreamSourceConduit;

/**
 * This is based on the FinishableStreamSourceConduit and DebuggingStreamSourceConduit from the undertow's set of StreamSourceConduit(s).
 */
class RequestBodySaverStreamSourceConduit extends AbstractStreamSourceConduit<StreamSourceConduit> {

    private final boolean isMultipart;
    private final String partBoundary;
    private final String lastBoundary;
    private final StringBuilder requestBodyBuffer;
    private final StringBuilder boundaryLookupBuffer;
    private final StringBuilder currentPartBuffer;
    private final Consumer<String> onDone;
    private String currentPartMimetype;
    private boolean finished;

    RequestBodySaverStreamSourceConduit(StreamSourceConduit next, boolean isMultipart, String boundary, Consumer<String> onDone) {
        super(next);
        this.isMultipart = isMultipart;
        if (isMultipart && boundary != null) {
            this.partBoundary = "--" + boundary + "\r\n";
            this.lastBoundary = "--" + boundary + "--" + "\r\n";
        } else {
            this.partBoundary = null;
            this.lastBoundary = null;
        }
        this.requestBodyBuffer = new StringBuilder();
        this.boundaryLookupBuffer = new StringBuilder();
        this.currentPartBuffer = new StringBuilder();
        this.onDone = onDone;
        this.currentPartMimetype = null;
        this.finished = false;
    }

    public long transferTo(final long position, final long count, final FileChannel target) throws IOException {
        long res = 0;
        try {
            return res = next.transferTo(position, count, target);
        } finally {
            exitRead(res);
        }
    }

    public long transferTo(final long count, final ByteBuffer throughBuffer, final StreamSinkChannel target) throws IOException {
        long res = 0;
        try {
            return res = next.transferTo(count, throughBuffer, target);
        } finally {
            exitRead(res);
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int pos = dst.position();
        int res = super.read(dst);
        try {
            if (res > 0) {
                byte[] d = new byte[res];
                for (int i = 0; i < res; ++i) {
                    d[i] = dst.get(i + pos);
                }
                if (isMultipart) {
                    if (partBoundary != null) {
                        handleMultipart(d);
                    }
                } else {
                    // simply add the bytes to the body
                    requestBodyBuffer.append(new String(d));
                }
            }
            return res;
        } finally {
            exitRead(res);
        }
    }

    private void handleMultipart(byte[] bytes) {
        int bytesLength = bytes.length;
        for (int ci = 0; ci < bytesLength; ci++) {
            char c = (char) bytes[ci];
            boundaryLookupBuffer.append(c);
            int boundaryIndex = boundaryStartIndex();
            if (boundaryIndex >= 0) {
                if (currentPartMimetype == null) {
                    currentPartBuffer.append(c);
                } else if (!currentPartMimetype.contains("application/octet-stream")) {
                    currentPartBuffer.append(c);
                    currentPartBuffer.setLength(currentPartBuffer.length() - boundaryLookupBuffer.length() + boundaryIndex);
                }
                if (currentPartMimetype != null) {
                    requestBodyBuffer.append(currentPartBuffer);
                    currentPartBuffer.setLength(0);
                    currentPartBuffer.append(boundaryLookupBuffer.substring(boundaryIndex));
                    currentPartMimetype = null;
                }
                boundaryLookupBuffer.setLength(0);
                continue;
            } else if (boundaryLookupBuffer.length() > 2 * partBoundary.length()) {
                // no boundary found  and the boundary lookup buffer is larger than twice the length of the boundary
                // trim to the length of the boundary by copying the last chars to the beginning of the buffer
                for (int i = boundaryLookupBuffer.length() - partBoundary.length(); i < boundaryLookupBuffer.length(); i++) {
                    boundaryLookupBuffer.setCharAt(
                            i - boundaryLookupBuffer.length() + partBoundary.length(),
                            boundaryLookupBuffer.charAt(i)
                    );
                }
                boundaryLookupBuffer.setLength(partBoundary.length());
            }
            if (currentPartMimetype == null) {
                currentPartBuffer.append(c);
                int contentTypeIndex = currentPartBuffer.indexOf("Content-Type:");
                if (contentTypeIndex >= 0) {
                    int nextLineIndex = currentPartBuffer.indexOf("\n", contentTypeIndex + "Content-Type:".length());
                    if (nextLineIndex > 0) {
                        currentPartMimetype = currentPartBuffer.substring(contentTypeIndex + "Content-Type:".length(), nextLineIndex).trim();
                        if (currentPartMimetype.contains("application/octet-stream")) {
                            // discard octet-stream parts
                            currentPartBuffer.setLength(nextLineIndex + 1);
                        }
                    }
                }
            } else if (!currentPartMimetype.contains("application/octet-stream")) {
                currentPartBuffer.append(c);
            }
        }
    }

    private int boundaryStartIndex() {
        int boundaryIndex = boundaryLookupBuffer.indexOf(partBoundary);
        if (boundaryIndex >= 0) {
            return boundaryIndex;
        } else {
            return boundaryLookupBuffer.indexOf(lastBoundary);
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offs, int len) throws IOException {
        for (int i = offs; i < len; ++i) {
            if (dsts[i].hasRemaining()) {
                int res = read(dsts[i]);
                try {
                    return res;
                } finally {
                    exitRead(res);
                }
            }
        }
        return 0;
    }

    private void exitRead(long consumed) {
        if (consumed == -1) {
            if (!finished) {
                finished = true;
                onDone.accept(requestBodyBuffer.toString());
            }
        }
    }

}
