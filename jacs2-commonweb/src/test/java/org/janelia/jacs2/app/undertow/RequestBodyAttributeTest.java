package org.janelia.jacs2.app.undertow;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;

import io.undertow.server.HttpServerExchange;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RequestBodyAttributeTest {

    private static class TestData {
        private final List<RequestBodyPart> requestBodyParts;
        private final String expectedResult;

        TestData(List<RequestBodyPart> requestBodyParts, String expectedResult) {
            this.requestBodyParts = requestBodyParts;
            this.expectedResult = expectedResult;
        }
    }

    private static final char[] LARGE_REQUEST = new char[512];
    private static final char[] SMALL_REQUEST = new char[10];

    @Before
    public void setUp() {
        Arrays.fill(LARGE_REQUEST, 'l');
        Arrays.fill(SMALL_REQUEST, 's');
    }

    @Test
    public void withLength() {
        RequestBodyAttribute requestBodyAttribute = new RequestBodyAttribute(16);
        HttpServerExchange testExchange = new HttpServerExchange(null);
        TestData[] testData = new TestData[] {
                new TestData(null, null),
                new TestData(ImmutableList.of(), ""),
                new TestData(
                        ImmutableList.of(
                                requestBodyPart(new String(SMALL_REQUEST))),
                        new StringBuilder().append(SMALL_REQUEST).toString()
                ),
                new TestData(
                        ImmutableList.of(
                                requestBodyPart(new String(SMALL_REQUEST)),
                                requestBodyPart(new String(SMALL_REQUEST)),
                                requestBodyPart(new String(SMALL_REQUEST))),
                        new StringBuilder()
                                .append(SMALL_REQUEST)
                                .append(SMALL_REQUEST)
                                .append(SMALL_REQUEST)
                                .substring(0, 16) + "... truncated ..."
                ),
        };
        for (TestData td : testData) {
            testExchange.putAttachment(SavedRequestBodyHandler.SAVED_REQUEST_BODY, td.requestBodyParts);
            String attrValue = requestBodyAttribute.readAttribute(testExchange);
            if (td.expectedResult == null) {
                assertNull(attrValue);
            } else {
                assertEquals(td.expectedResult, attrValue);
            }
        }
    }

    @Test
    public void withoutLength() {
        RequestBodyAttribute requestBodyAttribute = new RequestBodyAttribute(null);
        HttpServerExchange testExchange = new HttpServerExchange(null);
        TestData[] testData = new TestData[] {
                new TestData(null, null),
                new TestData(ImmutableList.of(), ""),
                new TestData(
                        ImmutableList.of(
                                requestBodyPart(new String(SMALL_REQUEST))),
                        new StringBuilder().append(SMALL_REQUEST).toString()
                ),
                new TestData(
                        ImmutableList.of(
                                requestBodyPart(new String(LARGE_REQUEST)),
                                requestBodyPart(new String(SMALL_REQUEST)),
                                requestBodyPart(new String(LARGE_REQUEST))),
                        new StringBuilder().append(LARGE_REQUEST).append(SMALL_REQUEST).append(LARGE_REQUEST).toString()
                )
        };
        for (TestData td : testData) {
            testExchange.putAttachment(SavedRequestBodyHandler.SAVED_REQUEST_BODY, td.requestBodyParts);
            String attrValue = requestBodyAttribute.readAttribute(testExchange);
            if (td.expectedResult == null) {
                assertNull(attrValue);
            } else {
                assertEquals(td.expectedResult, attrValue);
            }
        }
    }

    private RequestBodyPart requestBodyPart(CharSequence body) {
        RequestBodyPart rb = new RequestBodyPart("some/mime");
        if (body != null) rb.partBodyBuilder.append(body);
        return rb;
    }

}
