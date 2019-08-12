package org.janelia.jacs2.asyncservice.maintenanceservices;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.service.JacsNotification;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceLifecycleStage;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

@Named("dbMaintenance")
public class DbMaintenanceProcessor extends AbstractServiceProcessor<Void> {

    public static class DbMaintenanceArgs extends ServiceArgs {
        @Parameter(names = "-refreshIndexes", arity = 0, description = "Refresh database indexes")
        boolean refreshIndexes = false;
        @Parameter(names = "-refreshPermissions", arity = 0, description = "Refresh permissions")
        boolean refreshPermissions = false;
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
                           @Any Instance<ExternalProcessRunner> serviceRunners,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           JacsNotificationDao jacsNotificationDao,
                           DbMaintainer dbMainainer,
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
        if (args.refreshTmSampleSync) {
            logger.info("Service {} perform TmSample filesystem sync refresh", jacsServiceData);
            logMaintenanceEvent("Refresh TmSample filesystem sync", jacsServiceData.getId());
            try {
                dbMainainer.refreshTmSampleSync();
            }
            catch (Exception e) {
                throw new ComputationException(jacsServiceData, e);
            }
        } else {
            logger.info("Service {} skip TmSample filesystem sync refresh", jacsServiceData);
            logMaintenanceEvent("Skip TmSample filesystem sync refresh", jacsServiceData.getId());
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
