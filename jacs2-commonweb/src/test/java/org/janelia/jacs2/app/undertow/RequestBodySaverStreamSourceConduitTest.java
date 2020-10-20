package org.janelia.jacs2.app.undertow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.xnio.conduits.StreamSourceConduit;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;

public class RequestBodySaverStreamSourceConduitTest {

    private static final String TEST_BOUNDARY = "boundary";
    private static final String BOUNDARY = "\r\n--" + TEST_BOUNDARY + "\r\n";
    private static final String TEXT_MIME_TYPE = "Content-Type: plain/text\r\n";
    private static final String BINARY_MIME_TYPE = "Content-Type: application/octet-stream\r\n";
    private static final String TEST_CONTENT = "Test content larger than 8 bytes";
    private static final String BINARY_TEST_CONTENT = "Test binary content larger than 8 bytes";
    private static final String END_BOUNDARY = "\r\n--" + TEST_BOUNDARY + "--" + "\r\n";

    private static final String TEXT_PART = BOUNDARY + TEXT_MIME_TYPE + TEST_CONTENT;
    private static final String BINARY_PART = BOUNDARY + BINARY_MIME_TYPE + BINARY_TEST_CONTENT;

    private static class TestData {
        private final String requestMimeType;
        private final String boundary;
        private final byte[] requestBody;
        private final List<RequestBodyPart> expectedRequestBodyParts;

        TestData(String requestMimeType, String boundary, String requestBody, List<RequestBodyPart> expectedRequestBodyParts) {
            this.requestMimeType = requestMimeType;
            this.boundary = boundary;
            this.requestBody = requestBody != null ? requestBody.getBytes() : null;
            this.expectedRequestBodyParts = expectedRequestBodyParts;
        }
    }

    @Test
    public void multipartRequestWithValidBoundary() {
        TestData[] testData = new TestData[] {
                new TestData("multipart/mixed", TEST_BOUNDARY, TEXT_PART + TEXT_PART + END_BOUNDARY,
                        ImmutableList.of(
                                requestBodyPart("plain/text", TEST_CONTENT),
                                requestBodyPart("plain/text", TEST_CONTENT)
                        )),
                new TestData("multipart/mixed", TEST_BOUNDARY, BINARY_PART + BINARY_PART + END_BOUNDARY,
                        ImmutableList.of(
                                requestBodyPart("application/octet-stream", null),
                                requestBodyPart("application/octet-stream", null)
                        )),
                new TestData("multipart/mixed", TEST_BOUNDARY, TEXT_PART + BINARY_PART + END_BOUNDARY,
                        ImmutableList.of(
                                requestBodyPart("plain/text", TEST_CONTENT),
                                requestBodyPart("application/octet-stream", null)
                        )),
                new TestData("multipart/mixed", TEST_BOUNDARY, TEXT_PART + BINARY_PART + BINARY_PART + END_BOUNDARY,
                        ImmutableList.of(
                                requestBodyPart("plain/text", TEST_CONTENT),
                                requestBodyPart("application/octet-stream", null),
                                requestBodyPart("application/octet-stream", null)
                        )),
                new TestData("multipart/mixed", TEST_BOUNDARY, TEXT_PART + BINARY_PART + TEXT_PART + END_BOUNDARY,
                        ImmutableList.of(
                                requestBodyPart("plain/text", TEST_CONTENT),
                                requestBodyPart("application/octet-stream", null),
                                requestBodyPart("plain/text", TEST_CONTENT)
                        )),
                new TestData("multipart/mixed", TEST_BOUNDARY, BINARY_PART + BINARY_PART + TEXT_PART + END_BOUNDARY,
                        ImmutableList.of(
                                requestBodyPart("application/octet-stream", null),
                                requestBodyPart("application/octet-stream", null),
                                requestBodyPart("plain/text", TEST_CONTENT)
                        )),
        };
        for (int bufferLength : new int[]{8, 16, 32, 256}) {
            verifyMultipartRequest(Arrays.asList(testData), bufferLength);
        }
    }

    @Test
    public void multipartRequestWithNullBoundary() {
        TestData[] testData = new TestData[] {
                new TestData("multipart/mixed", null, TEXT_PART + TEXT_PART + END_BOUNDARY, ImmutableList.of()),
                new TestData("multipart/mixed", null, BINARY_PART + BINARY_PART + END_BOUNDARY, ImmutableList.of()),
                new TestData("multipart/mixed", null, TEXT_PART + BINARY_PART + END_BOUNDARY, ImmutableList.of()),
                new TestData("multipart/mixed", null, TEXT_PART + BINARY_PART + BINARY_PART + END_BOUNDARY, ImmutableList.of()),
                new TestData("multipart/mixed", null, TEXT_PART + BINARY_PART + TEXT_PART + END_BOUNDARY, ImmutableList.of()),
                new TestData("multipart/mixed", null, BINARY_PART + BINARY_PART + TEXT_PART + END_BOUNDARY, ImmutableList.of()),
        };
        for (int bufferLength : new int[]{8, 16, 32, 256}) {
            verifyMultipartRequest(Arrays.asList(testData), bufferLength);
        }
    }

    @Test
    public void nonMultipartRequest() {
        TestData[] testData = new TestData[] {
                new TestData("plain/text", null, TEST_CONTENT, ImmutableList.of(requestBodyPart("plain/text", TEST_CONTENT))),
                new TestData("plain/text", null, "", ImmutableList.of())
        };
        for (int bufferLength : new int[]{8, 16, 32, 256}) {
            verifyNonMultipartRequest(Arrays.asList(testData), bufferLength);
        }
    }

    private RequestBodyPart requestBodyPart(String mimeType, String body) {
        RequestBodyPart rb = new RequestBodyPart(mimeType);
        if (body != null) rb.partBodyBuilder.append(body);
        return rb;
    }

    private void verifyMultipartRequest(List<TestData> testData, int readBufferLength) {
        StreamSourceConduit mockSourceConduit = Mockito.mock(StreamSourceConduit.class);
        testData.forEach(td -> {
            RequestBodySaverStreamSourceConduit requestBodySaverStreamSourceConduit = new RequestBodySaverStreamSourceConduit(mockSourceConduit, true, td.boundary,
                    () -> new RequestBodyPart(null),
                    rb -> MatcherAssert.assertThat(rb, equalTo(td.expectedRequestBodyParts)));
            prepareWrappedConduit(mockSourceConduit, td.requestBody);
            ByteBuffer byteBuffer = ByteBuffer.allocate(readBufferLength);
            exerciseStreamSource(requestBodySaverStreamSourceConduit, byteBuffer);
            Mockito.reset(mockSourceConduit);
        });
    }

    private void verifyNonMultipartRequest(List<TestData> testData, int readBufferLength) {
        StreamSourceConduit mockSourceConduit = Mockito.mock(StreamSourceConduit.class);
        testData.forEach(td -> {
            RequestBodySaverStreamSourceConduit requestBodySaverStreamSourceConduit = new RequestBodySaverStreamSourceConduit(mockSourceConduit, false, null,
                    () -> new RequestBodyPart(td.requestMimeType),
                    rb -> MatcherAssert.assertThat(rb, equalTo(td.expectedRequestBodyParts)));
            prepareWrappedConduit(mockSourceConduit, td.requestBody);
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
