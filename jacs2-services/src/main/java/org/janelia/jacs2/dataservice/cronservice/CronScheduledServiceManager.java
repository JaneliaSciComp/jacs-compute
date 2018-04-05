package org.janelia.jacs2.dataservice.cronservice;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsScheduledServiceDataPersistence;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsScheduledServiceData;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CronScheduledServiceManager {

    private static final Logger LOG = LoggerFactory.getLogger(CronScheduledServiceManager.class);

    private final JacsScheduledServiceDataPersistence jacsScheduledServiceDataPersistence;
    private final JacsServiceDataPersistence jacsServiceDataPersistence;
    private final String queueId;
    private final CronParser cronParser;

    @Inject
    public CronScheduledServiceManager(JacsScheduledServiceDataPersistence jacsScheduledServiceDataPersistence,
                                       JacsServiceDataPersistence jacsServiceDataPersistence,
                                       @PropertyValue(name = "service.queue.id") String queueId) {
        this.jacsScheduledServiceDataPersistence = jacsScheduledServiceDataPersistence;
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.queueId = queueId;
        CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);
        this.cronParser = new CronParser(cronDefinition);
    }

    public JacsScheduledServiceData createScheduledService(JacsScheduledServiceData scheduledServiceData) {
        validateScheduledService(scheduledServiceData);
        return jacsScheduledServiceDataPersistence.createEntity(scheduledServiceData);
    }

    public JacsScheduledServiceData updateScheduledService(JacsScheduledServiceData scheduledServiceData) {
        validateScheduledService(scheduledServiceData);
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> scheduledServiceUpdatesBuilder = ImmutableMap.<String, EntityFieldValueHandler<?>>builder()
                .put("name", new SetFieldValueHandler<>(scheduledServiceData.getName()))
                .put("description", new SetFieldValueHandler<>(scheduledServiceData.getDescription()))
                .put("serviceName", new SetFieldValueHandler<>(scheduledServiceData.getServiceName()))
                .put("serviceVersion", new SetFieldValueHandler<>(scheduledServiceData.getServiceVersion()))
                .put("serviceArgs", new SetFieldValueHandler<>(scheduledServiceData.getServiceArgs()))
                .put("serviceDictionaryArgs", new SetFieldValueHandler<>(scheduledServiceData.getServiceDictionaryArgs()))
                .put("serviceResources", new SetFieldValueHandler<>(scheduledServiceData.getServiceResources()))
                .put("servicePriority", new SetFieldValueHandler<>(scheduledServiceData.getServicePriority()))
                .put("cronScheduleDescriptor", new SetFieldValueHandler<>(scheduledServiceData.getCronScheduleDescriptor()))
                .put("runServiceAs", new SetFieldValueHandler<>(scheduledServiceData.getRunServiceAs()))
                .put("serviceQueueId", new SetFieldValueHandler<>(scheduledServiceData.getServiceQueueId()))
                .put("serviceProcessingLocation", new SetFieldValueHandler<>(scheduledServiceData.getServiceProcessingLocation()))
                .put("disabled", new SetFieldValueHandler<>(scheduledServiceData.isDisabled()));
        if (scheduledServiceData.getNextStartTime() != null) {
            scheduledServiceUpdatesBuilder.put("nextStartTime", new SetFieldValueHandler<>(scheduledServiceData.getNextStartTime()));
        }
        jacsScheduledServiceDataPersistence.update(scheduledServiceData, scheduledServiceUpdatesBuilder.build());
        return scheduledServiceData;
    }

    private void validateScheduledService(JacsScheduledServiceData scheduledServiceData) {
        if (scheduledServiceData.isDisabled()) {
            return; // no check is done if the service is not enabled - it assumes that the setting process is not completed
        }
        if (StringUtils.isBlank(scheduledServiceData.getCronScheduleDescriptor())) {
            throw new IllegalArgumentException("Empty schedule descriptor: " + scheduledServiceData);
        }
        cronParser.parse(scheduledServiceData.getCronScheduleDescriptor());
    }

    public JacsScheduledServiceData getScheduledServiceById(Number id) {
        return jacsScheduledServiceDataPersistence.findById(id);
    }

    public PageResult<JacsScheduledServiceData> listAllScheduledServices(PageRequest pageRequest) {
        return jacsScheduledServiceDataPersistence.findAll(pageRequest);
    }

    public void removeScheduledServiceById(JacsScheduledServiceData scheduledServiceData) {
        jacsScheduledServiceDataPersistence.delete(scheduledServiceData);
    }

    public List<JacsServiceData> scheduleServices(Duration checkInterval) {
        ZonedDateTime now = ZonedDateTime.now();
        Date nowAsDate = Date.from(now.toInstant());
        List<JacsScheduledServiceData> scheduledCandidates = jacsScheduledServiceDataPersistence.findServicesScheduledAtOrBefore(queueId, nowAsDate)
                .stream()
                .filter(scheduledService -> StringUtils.isNotBlank(scheduledService.getCronScheduleDescriptor()))
                .filter(scheduledService -> StringUtils.isNotBlank(scheduledService.getServiceName()))
                .flatMap(scheduledService -> {
                    Cron cron = cronParser.parse(scheduledService.getCronScheduleDescriptor());
                    ExecutionTime executionTime = ExecutionTime.forCron(cron);
                    if (scheduledService.getNextStartTime() != null || executionTime.isMatch(now)) {
                        return executionTime.nextExecution(now.plus(checkInterval))
                                .map(nextTime -> {
                                    scheduledService.setNextStartTime(Date.from(nextTime.toInstant()));
                                    return Stream.of(scheduledService);
                                })
                                .orElse(Stream.of());
                    } else {
                        // this service has never been scheduled yet and the current time doesn't match
                        // the configured cron fields
                        return executionTime.nextExecution(now)
                                .map(nextTime -> {
                                    scheduledService.setNextStartTime(Date.from(nextTime.toInstant()));
                                    jacsScheduledServiceDataPersistence.update(scheduledService, ImmutableMap.of(
                                            "nextStartTime", new SetFieldValueHandler<>(scheduledService.getNextStartTime())
                                    ));
                                    return Stream.<JacsScheduledServiceData>of();
                                })
                                .orElse(Stream.of());
                    }
                })
                .collect(Collectors.toList());
        if (!scheduledCandidates.isEmpty()) {
            LOG.debug("Service candidates to run at {}: {}", nowAsDate, scheduledCandidates);
        }
        List<JacsScheduledServiceData> scheduledServices = jacsScheduledServiceDataPersistence.updateServicesScheduledAtOrBefore(scheduledCandidates, nowAsDate);
        if (!scheduledServices.isEmpty()) {
            LOG.debug("Services scheduled to run at {}: {}", nowAsDate, scheduledServices);
        }
        return scheduledServices.stream()
                .map(scheduledService -> scheduledService.createServiceInstance())
                .map(sd -> jacsServiceDataPersistence.createEntity(sd))
                .collect(Collectors.toList());
    }
}
