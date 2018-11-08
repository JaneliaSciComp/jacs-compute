package org.janelia.jacs2.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class JwtDecoderTest {

    private JwtDecoder jwtDecoder;

    @Before
    public void setUp() {
        jwtDecoder = new JwtDecoder(ObjectMapperFactory.instance());
    }

    @Test
    public void decode() {
        final String testToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1NDE3OTk5NTcsInVzZXJfbmFtZSI6ImphY3MifQ.1DXuzqXVRMn5N3s7dnyEjsn0ZaybttMxuX8cQQWWPqo";
        JWT jwt = jwtDecoder.decode(testToken);
        assertNotNull(jwt);
        assertNotNull(jwt.userName);
        assertNotNull(jwt.expInSeconds);
    }

}
