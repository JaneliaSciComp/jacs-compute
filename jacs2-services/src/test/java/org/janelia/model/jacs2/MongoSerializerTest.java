package org.janelia.model.jacs2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.util.Utils;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class MongoSerializerTest {

    @Test
    public void testDomainObjectWrapper() throws JsonProcessingException {

        ObjectMapper objectMapper = ObjectMapperFactory.instance().newMongoCompatibleObjectMapper();

        LSMImage lsm = new LSMImage();

        DomainObjectWrapper t = new DomainObjectWrapper();
        t.setDomainObject(lsm);

        String json = objectMapper.writeValueAsString(t);
        System.out.println(json);
        assertTrue(json.contains("class"));
    }

    @Test
    public void testDomainObjectMapWrapper() throws JsonProcessingException {

        ObjectMapper objectMapper = ObjectMapperFactory.instance().newMongoCompatibleObjectMapper();

        LSMImage lsm = new LSMImage();

        DomainObjectMapWrapper t = new DomainObjectMapWrapper();
        Map<String, DomainObject> map = Maps.newHashMap();
        map.put("lsm", lsm);
        t.setObjectMap(map);

        String json = objectMapper.writeValueAsString(t);
        System.out.println(json);
        assertTrue(json.contains("class"));
    }

    @Test
    public void testObjectMapWrapper() throws JsonProcessingException {

        ObjectMapper objectMapper = ObjectMapperFactory.instance().newMongoCompatibleObjectMapper();

        LSMImage lsm = new LSMImage();

        ObjectMapWrapper t = new ObjectMapWrapper();
        t.setObjectMap(Utils.strObjMap("lsm", lsm));

        String json = objectMapper.writeValueAsString(t);
        System.out.println(json);
        assertTrue(json.contains("class"));
    }

    public class DomainObjectWrapper {

        private DomainObject domainObject;

        public DomainObject getDomainObject() {
            return domainObject;
        }

        public void setDomainObject(DomainObject domainObject) {
            this.domainObject = domainObject;
        }
    }

    public class DomainObjectMapWrapper {

        private Map<String,DomainObject> objectMap;

        public Map<String, DomainObject> getObjectMap() {
            return objectMap;
        }

        public void setObjectMap(Map<String, DomainObject> objectMap) {
            this.objectMap = objectMap;
        }
    }

    public class ObjectMapWrapper {

        private Map<String,Object> objectMap;

        public Map<String, Object> getObjectMap() {
            return objectMap;
        }

        public void setObjectMap(Map<String, Object> objectMap) {
            this.objectMap = objectMap;
        }
    }

}
