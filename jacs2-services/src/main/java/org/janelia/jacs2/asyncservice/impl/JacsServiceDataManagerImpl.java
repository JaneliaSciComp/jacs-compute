package org.janelia.jacs2.asyncservice.impl;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.data.NamedData;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsServiceData;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
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
import java.util.function.Supplier;
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

    private Stream<Path> streamOutputDir(String outputDir) {
        if (StringUtils.isBlank(outputDir) || Files.notExists(Paths.get(outputDir))) {
            return Stream.of();
        } else {
            return FileUtils.lookupFiles(Paths.get(outputDir), 1, "glob:**/*")
                    .filter(outputPath -> Files.isRegularFile(outputPath))
                    .filter(outputPath -> {
                        try {
                            return !Files.isHidden(outputPath);
                        } catch (IOException e) {
                            LOG.error("Error reading hidden attribute for {}", outputPath);
                            return false;
                        }
                    })
                    ;
        }
    }

    private long getServiceOutputSize(String outputDir) {
        return streamOutputDir(outputDir)
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

    @Override
    public Stream<Supplier<NamedData<InputStream>>> streamServiceStdOutput(JacsServiceData serviceData) {
        return streamServiceOutputFiles(serviceData.getOutputPath());
    }

    @Override
    public Stream<Supplier<NamedData<InputStream>>> streamServiceStdError(JacsServiceData serviceData) {
        return streamServiceOutputFiles(serviceData.getErrorPath());
    }

    private Stream<Supplier<NamedData<InputStream>>> streamServiceOutputFiles(String outputDir) {
        return streamOutputDir(outputDir)
                .map(outputPath -> () -> {
                    try {
                        return new NamedData<>(outputPath.toString(), Files.newInputStream(outputPath));
                    } catch (IOException e) {
                        LOG.error("Error streaming {}", outputPath, e);
                        throw new UncheckedIOException(e);
                    }
                });
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
