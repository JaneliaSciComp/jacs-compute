package org.janelia.jacs2.asyncservice.maintenanceservices;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.service.JacsNotification;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceLifecycleStage;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;

@Named("dbMaintenance")
public class DbMaintenanceProcessor extends AbstractServiceProcessor<Void> {

    public static class DbMaintenanceArgs extends ServiceArgs {
        @Parameter(names = "-refreshIndexes", arity = 0, description = "Refresh database indexes")
        boolean refreshIndexes = false;
        @Parameter(names = "-refreshPermissions", arity = 0, description = "Refresh permissions")
        boolean refreshPermissions = false;
        DbMaintenanceArgs() {
            super("Database maintenance service. It performs index refresh and/or update access permissions");
        }
    }

    private final JacsNotificationDao jacsNotificationDao;
    private final DbMainainer dbMainainer;

    @Inject
    DbMaintenanceProcessor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @Any Instance<ExternalProcessRunner> serviceRunners,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           JacsNotificationDao jacsNotificationDao,
                           DbMainainer dbMainainer,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.jacsNotificationDao = jacsNotificationDao;
        this.dbMainainer = dbMainainer;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DbMaintenanceProcessor.class, new DbMaintenanceArgs());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        DbMaintenanceArgs args = getArgs(jacsServiceData);
        if (args.refreshIndexes) {
            logger.info("Service {} perform database reindexing", jacsServiceData);
            logMaintenanceEvent("Reindexing", jacsServiceData.getId());
            dbMainainer.ensureIndexes();
        } else {
            logger.info("Service {} skip database reindexing", jacsServiceData);
            logMaintenanceEvent("Skip reindexing", jacsServiceData.getId());
        }
        if (args.refreshPermissions) {
            logger.info("Service {} perform permission refresh", jacsServiceData);
            logMaintenanceEvent("Refresh permissions", jacsServiceData.getId());
            dbMainainer.refreshPermissions();
        } else {
            logger.info("Service {} skip permission refresh", jacsServiceData);
            logMaintenanceEvent("Skip permissions refresh", jacsServiceData.getId());
        }
        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
    }

    private void logMaintenanceEvent(String maintenanceEvent, Number serviceId) {
        JacsNotification jacsNotification = new JacsNotification();
        jacsNotification.setEventName(maintenanceEvent);
        jacsNotification.addNotificationData("serviceInstance", serviceId.toString());
        jacsNotification.setNotificationStage(JacsServiceLifecycleStage.PROCESSING);
        jacsNotificationDao.save(jacsNotification);
    }

    private DbMaintenanceArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DbMaintenanceArgs());
    }

}