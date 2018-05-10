package org.janelia.jacs2.dataservice.persistence;

import org.janelia.model.access.dao.JacsScheduledServiceDataDao;
import org.janelia.model.access.dao.JacsServiceDataDao;
import org.janelia.model.service.JacsScheduledServiceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
