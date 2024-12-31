package org.janelia.jacs2.asyncservice.maintenanceservices;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.service.JacsNotification;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceLifecycleStage;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Named("dbMaintenance")
public class DbMaintenanceProcessor extends AbstractServiceProcessor<Void> {

    public static class DbMaintenanceArgs extends ServiceArgs {
        @Parameter(names = "-refreshIndexes", arity = 0, description = "Refresh database indexes")
        boolean refreshIndexes = false;
        @Parameter(names = "-refreshPermissions", arity = 0, description = "Refresh permissions")
        boolean refreshPermissions = false;
        @Parameter(names = "-refreshPermissionsForNeurons", arity = 0, description = "If refreshing permissions, include neurons as well, otherwise ignore the neurons")
        boolean refreshPermissionsForNeurons = false;
        @Parameter(names = "-refreshPermissionsFor", arity = 0, description = "If refreshing permissions, include fragments as well, otherwise ignore the fragments")
        boolean refreshPermissionsForFragments = false;
        @Parameter(names = "-refreshTmSampleSync", arity = 0, description = "Refresh filesystem synchronization for TmSamples")
        boolean refreshTmSampleSync = false;
        DbMaintenanceArgs() {
            super("Database maintenance service. It performs index refresh and/or update access permissions");
        }
    }

    private final JacsNotificationDao jacsNotificationDao;
    private final DbMaintainer dbMainainer;

    @Inject
    DbMaintenanceProcessor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           DbMaintainer dbMainainer,
                           JacsNotificationDao jacsNotificationDao,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.jacsNotificationDao = jacsNotificationDao;
        this.dbMainainer = dbMainainer;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DbMaintenanceProcessor.class, new DbMaintenanceArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        DbMaintenanceArgs args = getArgs(jacsServiceData);
        StringBuffer processingFailureMessage = new StringBuffer();
        if (args.refreshIndexes) {
            logger.info("Service {} perform database reindexing", jacsServiceData);
            logMaintenanceEvent("Reindexing", jacsServiceData.getId());
            try {
                dbMainainer.ensureIndexes();
                jacsServiceDataPersistence.addServiceEvent(jacsServiceData, JacsServiceData.createServiceEvent(JacsServiceEventTypes.STEP_COMPLETED, "Completed database re-indexing refresh"));
                logger.info("Database re-indexing completed for {}", jacsServiceData.getShortName());
            } catch (Exception e) {
                processingFailureMessage.append("database re-indexing failed - ").append(e.getMessage()).append(';');
                logger.error("Database re-indexing failed", e);
                jacsServiceDataPersistence.addServiceEvent(jacsServiceData, JacsServiceData.createServiceEvent(JacsServiceEventTypes.STE_FAILED, String.format("Database re-indexing failed: %s", e.getMessage())));
            }
        } else {
            logger.info("Service {} skip database reindexing", jacsServiceData);
            logMaintenanceEvent("Skip reindexing", jacsServiceData.getId());
        }
        if (args.refreshPermissions) {
            logger.info("Service {} perform permission refresh", jacsServiceData);
            logMaintenanceEvent("Refresh permissions", jacsServiceData.getId());
            try {
                dbMainainer.refreshPermissions(args.refreshPermissionsForFragments, args.refreshPermissionsForNeurons);
                jacsServiceDataPersistence.addServiceEvent(jacsServiceData, JacsServiceData.createServiceEvent(JacsServiceEventTypes.STEP_COMPLETED, "Completed permissions refresh"));
                logger.info("Permission refresh completed for {}", jacsServiceData.getShortName());
            } catch (Exception e) {
                processingFailureMessage.append("permissions refresh failed - ").append(e.getMessage()).append(';');
                logger.error("Permissions refresh failed", e);
                jacsServiceDataPersistence.addServiceEvent(jacsServiceData, JacsServiceData.createServiceEvent(JacsServiceEventTypes.STE_FAILED, String.format("Refresh permissions failed: %s", e.getMessage())));
            }
        } else {
            logger.info("Service {} skip permission refresh", jacsServiceData);
            logMaintenanceEvent("Skip permissions refresh", jacsServiceData.getId());
        }
        if (args.refreshTmSampleSync) {
            logger.info("Service {} perform TmSample filesystem sync refresh", jacsServiceData);
            logMaintenanceEvent("Refresh TmSample filesystem sync", jacsServiceData.getId());
            try {
                dbMainainer.refreshTmSampleSync();
                jacsServiceDataPersistence.addServiceEvent(jacsServiceData, JacsServiceData.createServiceEvent(JacsServiceEventTypes.STEP_COMPLETED, "Completed TM sample sync"));
                logger.info("TmSample sync completed for {}", jacsServiceData.getShortName());
            } catch (Exception e) {
                processingFailureMessage.append("TM sample sync failed - ").append(e.getMessage()).append(';');
                logger.error("TM sample sync failed", e);
                jacsServiceDataPersistence.addServiceEvent(jacsServiceData, JacsServiceData.createServiceEvent(JacsServiceEventTypes.STE_FAILED, String.format("TM sample sync failed: %s", e.getMessage())));
            }
        } else {
            logger.info("Service {} skip TmSample filesystem sync refresh", jacsServiceData);
            logMaintenanceEvent("Skip TmSample filesystem sync refresh", jacsServiceData.getId());
        }
        if (processingFailureMessage.length() == 0) {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
        } else {
            return computationFactory.newFailedComputation(new ComputationException(jacsServiceData, processingFailureMessage.toString()));
        }
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
