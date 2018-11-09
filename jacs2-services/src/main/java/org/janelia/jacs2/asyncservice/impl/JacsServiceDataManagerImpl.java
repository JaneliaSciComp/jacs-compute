package org.janelia.jacs2.asyncservice.impl;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.lob.ReaderInputStream;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsServiceData;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

public class JacsServiceDataManagerImpl implements JacsServiceDataManager {

    private static final Logger LOG = LoggerFactory.getLogger(JacsServiceDataManagerImpl.class);

    private final JacsServiceDataPersistence jacsServiceDataPersistence;

    @Inject
    public JacsServiceDataManagerImpl(JacsServiceDataPersistence jacsServiceDataPersistence) {
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
    }

    @Override
    public JacsServiceData retrieveServiceById(Number instanceId) {
        return jacsServiceDataPersistence.findServiceHierarchy(instanceId);
    }

    @Override
    public long countServices(JacsServiceData ref, DataInterval<Date> creationInterval) {
        return jacsServiceDataPersistence.countMatchingServices(ref, creationInterval);
    }

    @Override
    public PageResult<JacsServiceData> searchServices(JacsServiceData ref, DataInterval<Date> creationInterval, PageRequest pageRequest) {
        return jacsServiceDataPersistence.findMatchingServices(ref, creationInterval, pageRequest);
    }

    @Override
    public long getServiceStdOutputSize(JacsServiceData serviceData) {
        return getServiceOutputSize(serviceData.getOutputPath());
    }

    @Override
    public long getServiceStdErrorSize(JacsServiceData serviceData) {
        return getServiceOutputSize(serviceData.getErrorPath());
    }

    private long getServiceOutputSize(String outputDir) {
        if (StringUtils.isBlank(outputDir)) {
            return 0L;
        } else {
            return FileUtils.lookupFiles(Paths.get(outputDir), 1, "glob:**/*")
                    .map(outputPath -> {
                        try {
                            return Files.size(outputPath);
                        } catch (IOException e) {
                            LOG.error("Error get the size of {}", outputPath, e);
                            throw new UncheckedIOException(e);
                        }
                    })
                    .reduce((s1, s2) -> s1 + s2)
                    .orElse(0L);
        }
    }

    @Override
    public Stream<InputStream> streamServiceStdOutput(JacsServiceData serviceData) {
        return streamServiceOutputFiles(serviceData.getOutputPath());
    }

    @Override
    public Stream<InputStream> streamServiceStdError(JacsServiceData serviceData) {
        return streamServiceOutputFiles(serviceData.getErrorPath());
    }

    private Stream<InputStream> streamServiceOutputFiles(String outputDir) {
        if (StringUtils.isBlank(outputDir)) {
            return Stream.of();
        } else {
            return FileUtils.lookupFiles(Paths.get(outputDir), 1, "glob:**/*")
                    .map(outputPath -> {
                        try {
                            return new ReaderInputStream(Files.newBufferedReader(outputPath));
                        } catch (IOException e) {
                            LOG.error("Error streaming {}", outputPath, e);
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }

    @Override
    public JacsServiceData updateService(Number instanceId, JacsServiceData serviceData) {
        JacsServiceData existingService = jacsServiceDataPersistence.findServiceHierarchy(instanceId);
        if (existingService == null) {
            return null;
        }
        Map<String, EntityFieldValueHandler<?>> updates = new LinkedHashMap<>();
        if (serviceData.getState() != null) {
            updates.putAll(existingService.updateState(serviceData.getState()));
        }
        if (serviceData.getServiceTimeout() != null) {
            existingService.setServiceTimeout(serviceData.getServiceTimeout());
            updates.put("serviceTimeout", new SetFieldValueHandler<>(serviceData.getServiceTimeout()));
        }
        if (StringUtils.isNotBlank(serviceData.getWorkspace())) {
            existingService.setWorkspace(serviceData.getWorkspace());
            updates.put("workspace", new SetFieldValueHandler<>(serviceData.getWorkspace()));
        }
        if (!updates.isEmpty()) {
            jacsServiceDataPersistence.update(existingService, updates);
        }
        if (serviceData.getPriority() != null) {
            Map<JacsServiceData, Integer> newPriorities = existingService.getNewServiceHierarchyPriorities(serviceData.getPriority());
            newPriorities.entrySet().forEach(sdpEntry -> {
                JacsServiceData sd = sdpEntry.getKey();
                sd.setPriority(sdpEntry.getValue());
                jacsServiceDataPersistence.update(sd, ImmutableMap.of("priority", new SetFieldValueHandler<>(sd.getPriority())));
            });
        }
        return existingService;
    }
}
