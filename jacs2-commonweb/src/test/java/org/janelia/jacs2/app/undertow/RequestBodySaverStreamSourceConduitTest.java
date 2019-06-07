package org.janelia.jacs2.app.undertow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.xnio.conduits.StreamSourceConduit;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;

public class RequestBodySaverStreamSourceConduitTest {

    private static final String TEST_BOUNDARY = "boundary";
    private static final String BOUNDARY = "--" + TEST_BOUNDARY + "\r\n";
    private static final String TEXT_MIME_TYPE = "Content-Type: plain/text\r\n";
    private static final String BINARY_MIME_TYPE = "Content-Type: application/octet-stream\r\n";
    private static final String TEST_CONTENT = "Test content larger than 8 bytes\r\n";
    private static final String END_BOUNDARY = "--" + TEST_BOUNDARY + "--" + "\r\n";

    private static final String TEXT_PART = BOUNDARY + TEXT_MIME_TYPE + TEST_CONTENT;
    private static final String BINARY_PART = BOUNDARY + BINARY_MIME_TYPE + TEST_CONTENT;

    @Test
    public void multipartRequestWithValidBoundary() {
        Map<String, String> testData = ImmutableMap.<String, String>builder()
                .put(TEXT_PART + TEXT_PART + END_BOUNDARY, TEXT_PART + TEXT_PART)
                .put(BINARY_PART + BINARY_PART + END_BOUNDARY, BOUNDARY + BINARY_MIME_TYPE + BOUNDARY + BINARY_MIME_TYPE)
                .put(TEXT_PART + BINARY_PART + END_BOUNDARY, TEXT_PART + BOUNDARY + BINARY_MIME_TYPE)
                .put(TEXT_PART + BINARY_PART + BINARY_PART + END_BOUNDARY, TEXT_PART + BOUNDARY + BINARY_MIME_TYPE + BOUNDARY + BINARY_MIME_TYPE)
                .put(TEXT_PART + BINARY_PART + TEXT_PART + END_BOUNDARY, TEXT_PART + BOUNDARY + BINARY_MIME_TYPE + TEXT_PART)
                .put(BINARY_PART + BINARY_PART + TEXT_PART + END_BOUNDARY, BOUNDARY + BINARY_MIME_TYPE + BOUNDARY + BINARY_MIME_TYPE + TEXT_PART)
                .build();
        for (int bufferLength : new int[]{8, 16, 32, 256}) {
            verifyMultipartRequest(true, TEST_BOUNDARY, testData, bufferLength);
        }
    }

    @Test
    public void multipartRequestWithNullBoundary() {
        Map<String, String> testData = ImmutableMap.<String, String>builder()
                .put(TEXT_PART + TEXT_PART + END_BOUNDARY, "")
                .put(BINARY_PART + BINARY_PART + END_BOUNDARY, "")
                .put(TEXT_PART + BINARY_PART + END_BOUNDARY, "")
                .put(TEXT_PART + BINARY_PART + BINARY_PART + END_BOUNDARY, "")
                .put(TEXT_PART + BINARY_PART + TEXT_PART + END_BOUNDARY, "")
                .put(BINARY_PART + BINARY_PART + TEXT_PART + END_BOUNDARY, "")
                .build();
        for (int bufferLength : new int[]{8, 16, 32, 256}) {
            verifyMultipartRequest(true, null, testData, bufferLength);
        }
    }

    @Test
    public void nonMultipartRequest() {
        Map<String, String> testData = ImmutableMap.<String, String>builder()
                .put(TEST_CONTENT, TEST_CONTENT)
                .put("", "")
                .build();
        for (int bufferLength : new int[]{8, 16, 32, 256}) {
            verifyMultipartRequest(false, null, testData, bufferLength);
        }
    }

    private void verifyMultipartRequest(boolean isMultipart, String headerBoundary, Map<String, String> testData, int readBufferLength) {
        StreamSourceConduit mockSourceConduit = Mockito.mock(StreamSourceConduit.class);
        testData.forEach((req, savedReq) -> {
            RequestBodySaverStreamSourceConduit requestBodySaverStreamSourceConduit = new RequestBodySaverStreamSourceConduit(mockSourceConduit, isMultipart, headerBoundary,
                    rb -> {
                        assertThat(rb, equalTo(savedReq));
                    });
            prepareWrappedConduit(mockSourceConduit, req.getBytes());
            ByteBuffer byteBuffer = ByteBuffer.allocate(readBufferLength);
            exerciseStreamSource(requestBodySaverStreamSourceConduit, byteBuffer);
            Mockito.reset(mockSourceConduit);
        });
    }

    private void prepareWrappedConduit(StreamSourceConduit mockSourceConduit, byte[] requestBytes) {
        try {
            Mockito.when(mockSourceConduit.read(any(ByteBuffer.class)))
                    .then(new Answer<Integer>() {
                        int bufferIndex = 0;

                        @Override
                        public Integer answer(InvocationOnMock invocation) {
                            ByteBuffer buffer = invocation.getArgument(0);
                            if (bufferIndex >= requestBytes.length) {
                                return -1;
                            } else {
                                int n = 0;
                                while(buffer.hasRemaining() && bufferIndex < requestBytes.length) {
                                    buffer.put(requestBytes[bufferIndex++]);
                                    n++;
                                }
                                return n;
                            }
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void exerciseStreamSource(StreamSourceConduit streamSourceConduit, ByteBuffer byteBuffer) {
        try {
            while (streamSourceConduit.read(byteBuffer) != -1) {
                byteBuffer.clear();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
