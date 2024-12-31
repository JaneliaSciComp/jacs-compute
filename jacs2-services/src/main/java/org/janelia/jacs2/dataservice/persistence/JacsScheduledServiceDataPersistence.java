package org.janelia.jacs2.dataservice.persistence;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.janelia.model.access.dao.JacsScheduledServiceDataDao;
import org.janelia.model.service.JacsScheduledServiceData;

public class JacsScheduledServiceDataPersistence extends AbstractDataPersistence<JacsScheduledServiceDataDao, JacsScheduledServiceData, Number> {

    @Inject
    public JacsScheduledServiceDataPersistence(Instance<JacsScheduledServiceDataDao> daoSource) {
        super(daoSource);
    }

    public List<JacsScheduledServiceData> findServicesScheduledAtOrBefore(String queueId, Date scheduledTime) {
        JacsScheduledServiceDataDao dataDao = daoSource.get();
        try {
            return dataDao.findServicesScheduledAtOrBefore(queueId, scheduledTime, false);
        } finally {
            daoSource.destroy(dataDao);
        }
    }

    public List<JacsScheduledServiceData> updateServicesScheduledAtOrBefore(List<JacsScheduledServiceData> serviceCandidates, Date scheduledTime) {
        JacsScheduledServiceDataDao dataDao = daoSource.get();
        try {
            return serviceCandidates.stream()
                    .flatMap(sd -> {
                        return dataDao.updateServiceScheduledTime(sd, scheduledTime)
                                .map(updatedService -> Stream.of(updatedService))
                                .orElse(Stream.of());
                    })
                    .collect(Collectors.toList());
        } finally {
            daoSource.destroy(dataDao);
        }
    }

}
