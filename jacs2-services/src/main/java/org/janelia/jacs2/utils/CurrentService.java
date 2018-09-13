package org.janelia.jacs2.utils;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.security.util.SubjectUtils;
import org.janelia.model.service.JacsServiceData;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Singleton which offers access to metadata about the service that is executing in the current thread.
 *
 * This only works for services extending AbstractServiceProcessor2.
 */
@Singleton
public class CurrentService {

    private static final String ERROR_MESSAGE = "CurrentService has no JacsServiceData. " +
            "Make sure you're using the AbstractServiceProcessor2 service hierarchy.";

    private final ThreadLocal<JacsServiceData> threadLocal = new ThreadLocal<>();

    @Inject
    @PropertyValue(name = "service.DefaultWorkingDir")
    protected String defaultWorkingDir;

    @Inject
    private JacsServiceDataPersistence jacsServiceDataPersistence;

    public JacsServiceData getJacsServiceData() {
        JacsServiceData sd = threadLocal.get();
        if (sd==null) {
            throw new ComputationException(ERROR_MESSAGE);
        }
        return sd;
    }

    public void setJacsServiceData(JacsServiceData jacsServiceData) {
        synchronized (threadLocal) {
            // Refresh the service data every time it's set.
            // Inefficient, but at least we know it's fresh.
            threadLocal.set(refresh(jacsServiceData));
        }
    }

    private JacsServiceData refresh(JacsServiceData sd) {
        JacsServiceData refreshedSd = jacsServiceDataPersistence.findById(sd.getId());
        if (refreshedSd == null) {
            throw new IllegalStateException("Service data not found for "+sd.getId());
        }
        return refreshedSd;
    }

    public Object getInput(String key) {
        return getJacsServiceData().getDictionaryArgs().get(key);
    }

    public Long getId() {
        return getJacsServiceData().getId().longValue();
    }

    public Long getParentId() {
        JacsServiceData sd = getJacsServiceData();
        return sd.getParentServiceId() == null ? null : sd.getParentServiceId().longValue();
    }

    public Long getRootId() {
        JacsServiceData sd = getJacsServiceData();
        return sd.getRootServiceId() == null ? null : sd.getRootServiceId().longValue();
    }

    public String getOwnerKey() {
        return getJacsServiceData().getOwnerKey();
    }

    public String getOwnerName() {
        return getJacsServiceData().getOwnerName();
    }

    public Path getServicePath() {
        return getServiceFolder().getServiceFolder();
    }

    public JacsServiceFolder getServiceFolder() {
        JacsServiceData jacsServiceData = getJacsServiceData();
        if (StringUtils.isNotBlank(jacsServiceData.getWorkspace())) {
            // The service has a workspace, so we create a file node inside of it
            return new JacsServiceFolder(null, Paths.get(jacsServiceData.getWorkspace()), jacsServiceData);
        }
        else if (StringUtils.isNotBlank(defaultWorkingDir)) {
            // There is no workspace, so create the service folder directly in the filestore
            return new JacsServiceFolder(getServicePath(defaultWorkingDir, jacsServiceData), null, jacsServiceData);
        }
        else {
            // There is no filestore, so use the temp directory
            return new JacsServiceFolder(getServicePath(System.getProperty("java.io.tmpdir"), jacsServiceData), null, jacsServiceData);
        }
    }

    /**
     * Build a filestore path like this:
     * /[rootPath]/[owner]/[serviceName]/[xxx]/[yyy]/[GUID]
     * Where xxxyyy are the last 6 digits of the GUID.
     * @param baseDir filestore directory containing usernames
     * @param jacsServiceData service metadata
     * @return path where the service can store data
     */
    private Path getServicePath(String baseDir, JacsServiceData jacsServiceData) {
        ImmutableList.Builder<String> pathElemsBuilder = ImmutableList.builder();
        if (StringUtils.isNotBlank(jacsServiceData.getOwnerKey())) {
            String name = SubjectUtils.getSubjectName(jacsServiceData.getOwnerKey());
            pathElemsBuilder.add(name);
        }
        pathElemsBuilder.add(StringUtils.capitalize(jacsServiceData.getName()));
        if (jacsServiceData.hasId()) {
            pathElemsBuilder.addAll(FileUtils.getTreePathComponentsForId(jacsServiceData.getId()));
        }
        return Paths.get(baseDir, pathElemsBuilder.build().toArray(new String[0])).toAbsolutePath();
    }
}