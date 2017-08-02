package org.janelia.jacs2.model.jacsservice;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.it.jacs.model.domain.interfaces.HasIdentifier;
import org.janelia.it.jacs.model.domain.support.MongoMapping;
import org.janelia.jacs2.model.BaseEntity;
import org.janelia.jacs2.model.EntityFieldValueHandler;
import org.janelia.jacs2.model.SetFieldValueHandler;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@MongoMapping(collectionName="jacsService", archiveCollectionName = "jacsServiceHistory", label="JacsService")
public class JacsServiceData implements BaseEntity, HasIdentifier {

    public static JacsServiceEvent createServiceEvent(JacsServiceEventTypes name, String value) {
        JacsServiceEvent se = new JacsServiceEvent();
        se.setName(name.name());
        se.setValue(value);
        return se;
    }

    @JsonProperty("_id")
    private Number id;
    private String name;
    private String description;
    private String version;
    private ProcessingLocation processingLocation = ProcessingLocation.LOCAL;
    private JacsServiceState state = JacsServiceState.CREATED;
    private Integer priority = 0;
    private String owner;
    private String queueId;
    private String inputPath;
    private String outputPath;
    private String errorPath;
    private List<String> args = new ArrayList<>();
    private Map<String, String> env = new LinkedHashMap<>();
    private Map<String, String> resources = new LinkedHashMap<>(); // this could/should be used for grid jobs resources
    private Object serializableResult;
    private String workspace;
    private Number parentServiceId;
    private Number rootServiceId;
    private List<JacsServiceEvent> events;
    private Date processStartTime = new Date();
    private Date creationDate = new Date();
    private Date modificationDate = new Date();
    private RegisteredJacsNotification processingNotification;
    private Map<String, RegisteredJacsNotification> processingStagedNotifications = new HashMap<>();
    @JsonIgnore
    private JacsServiceData parentService;
    @JsonIgnore
    private Set<JacsServiceData> dependencies = new LinkedHashSet<>();
    private Set<Number> dependenciesIds = new LinkedHashSet<>();
    private Long serviceTimeout;

    @Override
    public Number getId() {
        return id;
    }

    @Override
    public void setId(Number id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public ProcessingLocation getProcessingLocation() {
        return processingLocation;
    }

    public void setProcessingLocation(ProcessingLocation processingLocation) {
        this.processingLocation = processingLocation;
    }

    public JacsServiceState getState() {
        return state;
    }

    public void setState(JacsServiceState state) {
        this.state = state;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public String getQueueId() {
        return queueId;
    }

    public void setQueueId(String queueId) {
        this.queueId = queueId;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getErrorPath() {
        return errorPath;
    }

    public void setErrorPath(String errorPath) {
        this.errorPath = errorPath;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    @JsonIgnore
    public String[] getArgsArray() {
        if (args == null) {
            return new String[0];
        } else {
            return args.toArray(new String[0]);
        }
    }

    public void addArg(String arg) {
        if (this.args == null) {
            this.args = new ArrayList<>();
        }
        this.args.add(arg);
    }

    public void clearArgs() {
        if (this.args != null) {
            this.args.clear();
        }
    }

    public String getWorkspace() {
        return workspace;
    }

    public void setWorkspace(String workspace) {
        this.workspace = workspace;
    }

    public Number getParentServiceId() {
        return parentServiceId;
    }

    public boolean hasParentServiceId() {
        return parentServiceId != null;
    }

    public void setParentServiceId(Number parentServiceId) {
        this.parentServiceId = parentServiceId;
    }

    public Number getRootServiceId() {
        return rootServiceId;
    }

    public void setRootServiceId(Number rootServiceId) {
        this.rootServiceId = rootServiceId;
    }

    public List<JacsServiceEvent> getEvents() {
        return events;
    }

    public void setEvents(List<JacsServiceEvent> events) {
        this.events = events;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public Date getProcessStartTime() {
        return processStartTime;
    }

    public void setProcessStartTime(Date processStartTime) {
        this.processStartTime = processStartTime;
    }

    public Date getModificationDate() {
        return modificationDate;
    }

    public void setModificationDate(Date modificationDate) {
        this.modificationDate = modificationDate;
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public void setEnv(Map<String, String> env) {
        this.env = env;
    }

    public void addToEnv(String name, String value) {
        this.env.put(name, value);
    }

    public void clearEnv() {
        this.env.clear();
    }

    public Map<String, String> getResources() {
        return resources;
    }

    public void setResources(Map<String, String> resources) {
        this.resources = resources;
    }

    public void addToResources(String name, String value) {
        this.resources.put(name, value);
    }

    public void clearResources() {
        this.resources.clear();
    }

    public RegisteredJacsNotification getProcessingNotification() {
        return processingNotification;
    }

    public void setProcessingNotification(RegisteredJacsNotification processingNotification) {
        this.processingNotification = processingNotification;
    }

    public Map<String, RegisteredJacsNotification> getProcessingStagedNotifications() {
        return processingStagedNotifications;
    }

    public void setProcessingStagedNotifications(Map<String, RegisteredJacsNotification> processingStagedNotifications) {
        this.processingStagedNotifications = processingStagedNotifications;
    }

    public Optional<RegisteredJacsNotification> getProcessingStageNotification(String processingStage, RegisteredJacsNotification defaultNotification) {
        RegisteredJacsNotification stageNotification = processingStagedNotifications.get(processingStage);
        if (stageNotification == null) {
            if (defaultNotification == null)
                return Optional.empty();
            else
                return Optional.of(defaultNotification);
        } else
            return Optional.of(new RegisteredJacsNotification(stageNotification));
    }

    public void setProcessingStageNotification(String processingStage, RegisteredJacsNotification notification) {
        Preconditions.checkArgument(StringUtils.isNotBlank(processingStage));
        Preconditions.checkArgument(notification != null);
        processingStagedNotifications.put(processingStage, notification);
    }

    public Object getSerializableResult() {
        return serializableResult;
    }

    public void setSerializableResult(Object serializableResult) {
        this.serializableResult = serializableResult;
    }

    public void addNewEvent(JacsServiceEvent se) {
        if (this.events == null) {
            this.events = new ArrayList<>();
        }
        this.events.add(se);
    }

    public Set<JacsServiceData> getDependencies() {
        return dependencies;
    }

    public void addServiceDependency(JacsServiceData dependency) {
        dependencies.add(dependency);
        addServiceDependencyId(dependency);
        dependency.updateParentService(this);
    }

    public Set<Number> getDependenciesIds() {
        return dependenciesIds;
    }

    public void addServiceDependencyId(JacsServiceData dependency) {
        if (dependency.getId() != null) {
            dependenciesIds.add(dependency.getId());
        }
    }

    public void addServiceDependencyId(Number dependencyId) {
        if (dependencyId != null) {
            dependenciesIds.add(dependencyId);
        }
    }

    public JacsServiceData getParentService() {
        return parentService;
    }

    public Map<String, EntityFieldValueHandler<?>> updateParentService(JacsServiceData parentService) {
        Map<String, EntityFieldValueHandler<?>> updatedFields = new LinkedHashMap<>();
        if (parentService != null) {
            if (this.parentService == null) {
                this.parentService = parentService;
                parentService.addServiceDependencyId(this);
            }
            if (this.getParentServiceId() == null) {
                setParentServiceId(parentService.getId());
                updatedFields.put("parentServiceId", new SetFieldValueHandler<>(getParentServiceId()));
            }
            if (parentService.getRootServiceId() == null) {
                setRootServiceId(parentService.getId());
            } else {
                setRootServiceId(parentService.getRootServiceId());
            }
            updatedFields.put("rootServiceId", new SetFieldValueHandler<>(getRootServiceId()));
            if (priority == null || priority() <= parentService.priority()) {
                priority = parentService.priority() + 1;
                updatedFields.put("priority", new SetFieldValueHandler<>(priority));
            }
        } else {
            this.parentService = null;
            setParentServiceId(null);
            setRootServiceId(null);
            updatedFields.put("parentServiceId", null);
            updatedFields.put("rootServiceId", null);
        }
        return updatedFields;
    }

    public Stream<JacsServiceData> serviceHierarchyStream() {
        return serviceHierarchyStream(new LinkedHashSet<>());
    }

    private Stream<JacsServiceData> serviceHierarchyStream(Set<JacsServiceData> collectedDependencies) {
        return Stream.concat(
                Stream.of(this),
                dependencies.stream()
                        .filter(sd -> !collectedDependencies.contains(sd))
                        .flatMap(sd -> {
                            collectedDependencies.add(sd);
                            return sd.serviceHierarchyStream(collectedDependencies);
                        })
        );
    }

    public int priority() {
        return priority != null ? priority.intValue() : 0;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .append("state", state)
                .append("queueId", queueId)
                .append("args", args)
                .toString();
    }

    public boolean hasNeverBeenProcessed() {
        return state == JacsServiceState.CREATED || state == JacsServiceState.QUEUED;
    }

    public boolean hasCompleted() {
        return hasCompletedSuccessfully() || hasCompletedUnsuccessfully();
    }

    public boolean hasCompletedUnsuccessfully() {
        return state == JacsServiceState.CANCELED || state == JacsServiceState.ERROR || state == JacsServiceState.TIMEOUT;
    }

    public boolean hasCompletedSuccessfully() {
        return state == JacsServiceState.SUCCESSFUL;
    }

    public boolean hasBeenSuspended() {
        return state == JacsServiceState.SUSPENDED;
    }

    public Long getServiceTimeout() {
        return serviceTimeout;
    }

    public void setServiceTimeout(Long serviceTimeout) {
        this.serviceTimeout = serviceTimeout;
    }

    @JsonIgnore
    public long timeout() {
        return serviceTimeout != null && serviceTimeout > 0L ? serviceTimeout : -1;
    }

    public Optional<JacsServiceData> findSimilarDependency(JacsServiceData dependency) {
        return dependencies.stream()
                        .filter(s -> s.getName().equals(dependency.getName()))
                        .filter(s -> s.getArgs().equals(dependency.getArgs()))
                        .findFirst();
    }

    /**
     * Get the new priorities for the entire service hierarchy given a new priority for the current node.
     * @param newPriority
     */
    public Map<JacsServiceData, Integer> getNewServiceHierarchyPriorities(int newPriority) {
        int currentPriority = this.priority();
        int priorityDiff = newPriority - currentPriority;
        return this.serviceHierarchyStream().collect(Collectors.toMap(sd -> sd, sd -> sd.priority() + priorityDiff));
    }


    public void updateServicePriority(int newPriority) {
        Map<JacsServiceData, Integer> newPriorities = getNewServiceHierarchyPriorities(newPriority);
        newPriorities.entrySet().forEach(sdpEntry -> {
            JacsServiceData sd = sdpEntry.getKey();
            sd.setPriority(sdpEntry.getValue());
        });
    }
}
