package org.janelia.jacs2.asyncservice.utils;

import org.janelia.model.domain.compute.ContainerizedService;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ContainerizedServiceUtilsTest {

    @Test
    public void testSortByVersion() {

        List<String> versions = Arrays.asList("1.0-BETA", "1.0", "2.0", "2.1", "2.1.1", "3.0", "3.0.3");

        List<ContainerizedService> services = versions.stream().map(version -> {
            ContainerizedService service = new ContainerizedService();
            service.setName("test");
            service.setVersion(version);
            return service;
        }).collect(Collectors.toList());

        Collections.shuffle(services);

        ContainerizedServiceUtils.sortByVersion(services);
        List<String> sortedVersions = services.stream().map(s -> s.getVersion()).collect(Collectors.toList());

        Assert.assertEquals(versions, sortedVersions);
    }


    @Test
    public void testGetByVersion() {

        List<String> versions = Arrays.asList("1.0-BETA", "1.0", "2.0", "2.1", "2.1.1", "3.0", "3.0.3");

        List<ContainerizedService> services = versions.stream().map(version -> {
            ContainerizedService service = new ContainerizedService();
            service.setName("test");
            service.setVersion(version);
            return service;
        }).collect(Collectors.toList());

        for (String version : versions) {
            ContainerizedService service = ContainerizedServiceUtils.getContainer(services, "test", version);
            Assert.assertNotNull(service);
            Assert.assertEquals(version, service.getVersion());
        }
    }
}
