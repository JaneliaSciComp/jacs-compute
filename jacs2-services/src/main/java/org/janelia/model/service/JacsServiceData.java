package org.janelia.model.service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.model.domain.entityannotations.EntityId;
import org.janelia.model.jacs2.AppendFieldValueHandler;
import org.janelia.model.jacs2.BaseEntity;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.domain.interfaces.HasIdentifier;
import org.janelia.model.jacs2.domain.support.MongoMapping;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

@MongoMapping(collectionName="jacsService", label="JacsService")
public class JacsServiceData implements BaseEntity, HasIdentifier {

    public static JacsServiceEvent createServiceEvent(JacsServiceEventTypes eventType, String value) {
        JacsServiceEvent se = new JacsServiceEvent();
        se.setName(eventType.name());
        se.setValue(value);
        return se;
    }

    @EntityId
    private Number id;
    private String name;
    private String description;
    private String version;
    private ProcessingLocation processingLocation;
    private JacsServiceState state = JacsServiceState.CREATED;
    private Integer priority = 0;
    private String authKey;
    private String ownerKey;
    private String queueId;
    private String outputPath;
    private String errorPath;
    private List<String> args = new ArrayList<>();
    private List<String> actualArgs;
    private final Map<String, Object> dictionaryArgs = new LinkedHashMap<>();
    private Map<String, Object> serviceArgs;
    private Map<String, String> env = new LinkedHashMap<>();
    private Map<String, String> resources = new LinkedHashMap<>(); // this could/should be used for grid jobs resources
    private List<String> tags = new ArrayList<>();
    private Object serializableResult;
    private String workspace;
    private Number parentServiceId;
    private Number rootServiceId;
    // accessId is used for implementing an optimistic locking mechanism for updating service data
    private int accessId;
    private List<JacsServiceEvent> events;
    private Date processStartTime = new Date();
    private Date creationDate = new Date();
    private Date modificationDate = new Date();
    private RegisteredJacsNotification processingNotification;
    private Map<String, RegisteredJacsNotification> processingStagedNotifications = new HashMap<>();
    private Set<String> successEmailNotifications = new LinkedHashSet<>();
    private Set<String> failureEmailNotifications = new LinkedHashSet<>();
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

    public JacsServiceData withId(Number id) {
        this.id = id;
        return this;
    }

    @JsonProperty("serviceId")
    public String getServiceId() {
        return hasId() ? id.toString() : null;
    }

    @JsonIgnore
    public void setServiceId(String serviceId) {
        // do nothing
    }

    @JsonIgnore
    public String getEntityRefId() {
        return getEntityName() + "#" + (hasId() ? getId() : "");
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

    public String getAuthKey() {
        return authKey;
    }

    public void setAuthKey(String authKey) {
        this.authKey = authKey;
    }

    public String getOwnerKey() {
        return ownerKey;
    }

    public void setOwnerKey(String ownerKey) {
        this.ownerKey = ownerKey;
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

    /**
     * Service arguments as passed in by the caller application. The arguments may contain placeholders for
     * results of the current service' dependencies. The format of a placeholder argument is:
     * "|>${serviceName_serviceId.fieldName}"
     *
     * @return
     */
    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
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

    public List<String> getActualArgs() {
        return actualArgs;
    }

    public void setActualArgs(List<String> actualArgs) {
        this.actualArgs = actualArgs;
    }

    public Map<String, EntityFieldValueHandler<?>> updateActualListArgs(List<String> actualArgs) {
        Map<String, EntityFieldValueHandler<?>> dataUpdates = new HashMap<>();
        this.actualArgs = actualArgs;
        dataUpdates.put("actualArgs", new SetFieldValueHandler<>(actualArgs));
        return dataUpdates;
    }

    /**
     * Service dictionary arguments as passed in by the caller application. Just like the list argument,
     * dictionary arguments may also contain placeholders for results of the current service' dependencies.
     *
     * @see #getArgs() for the placeholder format
     */
    public Map<String, Object> getDictionaryArgs() {
        return dictionaryArgs;
    }

    public void setDictionaryArgs(Map<String, Object> dictionaryArgs) {
        this.dictionaryArgs.clear();
        if (dictionaryArgs != null) {
            this.dictionaryArgs.putAll(dictionaryArgs);
        }
    }

    @JsonIgnore
    public Map<String, Object> getActualDictionaryArgs() {
        if (serviceArgs == null) {
            return dictionaryArgs;
        } else {
            Map<String, Object> actualDictionaryArgs = new LinkedHashMap<>();
            dictionaryArgs.forEach((k, v) -> {
                actualDictionaryArgs.put(k, serviceArgs.containsKey(k) ? serviceArgs.get(k) : v);
            });
            return actualDictionaryArgs;
        }
    }

    public JacsServiceData addDictionaryArg(String argName, String argValue) {
        if (argValue != null) {
            dictionaryArgs.put(argName, argValue);
        }
        return this;
    }

    public String getDictionaryArgAsString(String argName) {
        return (String) dictionaryArgs.get(argName);
    }

    public Map<String, Object> getServiceArgs() {
        return serviceArgs;
    }

    public void setServiceArgs(Map<String, Object> serviceArgs) {
        this.serviceArgs = serviceArgs;
    }

    public Map<String, EntityFieldValueHandler<?>> addServiceArg(String arg, Object val) {
        Map<String, EntityFieldValueHandler<?>> dataUpdates = new HashMap<>();
        if (this.serviceArgs == null) {
            this.serviceArgs = new LinkedHashMap<>();
            this.serviceArgs.put(arg, val);
            dataUpdates.put("serviceArgs", new SetFieldValueHandler<>(this.serviceArgs));
        } else {
            this.serviceArgs.put(arg, val);
            dataUpdates.put("serviceArgs." + arg, new SetFieldValueHandler<>(val));
        }
        return dataUpdates;
    }

    public Map<String, EntityFieldValueHandler<?>> addServiceArgs(Map<String, Object> serviceArgs) {
        Map<String, EntityFieldValueHandler<?>> dataUpdates = new LinkedHashMap<>();
        if (this.serviceArgs == null) {
            this.serviceArgs = new LinkedHashMap<>();
            this.serviceArgs.putAll(serviceArgs);
            dataUpdates.put("serviceArgs", new SetFieldValueHandler<>(this.serviceArgs));
        } else {
            this.serviceArgs.putAll(serviceArgs);
            serviceArgs.forEach((arg, val) -> dataUpdates.put("serviceArgs." + arg, new SetFieldValueHandler<>(val)));
        }
        return dataUpdates;
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

    public boolean hasRootServiceId() {
        return rootServiceId != null;
    }

    public int getAccessId() {
        return accessId;
    }

    public void setAccessId(int accessId) {
        this.accessId = accessId;
    }

    public void initAccessId() {
        this.setAccessId(1);
    }

    public JacsServiceData withAccessId(int accessId) {
        this.accessId = accessId;
        return this;
    }

    public int nextAccessId() {
        return accessId + 1;
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

    public void addToEnv(String varName, String value) {
        this.env.put(varName, value);
    }

    public void addAllToEnv(Map<String, String> env) {
        this.env.putAll(env);
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

    public void addToResources(String resourceName, String resourceVal) {
        this.resources.put(resourceName, resourceVal);
    }

    public void clearResources() {
        this.resources.clear();
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public void addTags(String... tags) {
        Collections.addAll(this.tags, tags);
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

    public Set<String> getSuccessEmailNotifications() {
        return successEmailNotifications;
    }

    public void setSuccessEmailNotifications(Set<String> successEmailNotifications) {
        this.successEmailNotifications = successEmailNotifications;
    }

    public void addSuccessEmailNotifications(Collection<String> successEmailNotifications) {
        this.successEmailNotifications.addAll(successEmailNotifications);
    }

    public Set<String> getFailureEmailNotifications() {
        return failureEmailNotifications;
    }

    public void setFailureEmailNotifications(Set<String> failureEmailNotifications) {
        this.failureEmailNotifications = failureEmailNotifications;
    }

    public void addFailureEmailNotifications(Collection<String> failureEmailNotifications) {
        this.failureEmailNotifications.addAll(failureEmailNotifications);
    }

    public Object getSerializableResult() {
        return serializableResult;
    }

    public void setSerializableResult(Object serializableResult) {
        this.serializableResult = serializableResult;
    }

    public Map<String, EntityFieldValueHandler<?>> addNewEvent(JacsServiceEvent se) {
        Map<String, EntityFieldValueHandler<?>> dataUpdates = new HashMap<>();
        dataUpdates.put("events", new AppendFieldValueHandler<>(se));
        if (this.events == null) {
            this.events = new ArrayList<>();
        }
        this.events.add(se);
        return dataUpdates;
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

    public String getShortName() {
        return getName()+"#"+getId();
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

    public boolean hasNotBeenWaitingForDependencies() {
        return state != JacsServiceState.WAITING_FOR_DEPENDENCIES;
    }

    public boolean hasBeenCanceled() {
        return state == JacsServiceState.CANCELED;
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
    public long timeoutInMillis() {
        return serviceTimeout != null && serviceTimeout > 0L ? serviceTimeout : -1;
    }

    @JsonIgnore
    public int timeoutInMins() {
        return serviceTimeout != null && serviceTimeout > 0L ? (int) Duration.ofMillis(serviceTimeout).toMinutes() : -1;
    }

    public Optional<JacsServiceData> findSimilarDependency(JacsServiceData dependency) {
        return dependencies.stream()
                .filter(s -> s.getName().equals(dependency.getName()))
                .filter(s -> s.getArgs().equals(dependency.getArgs()))
                .filter(s -> s.getDependencies().stream() // compare dependencies but ignore the ones that are generated while processing s
                        .filter(sd -> !sd.getParentServiceId().equals(s.getId()))
                        .map(sd -> sd.getId())
                        .collect(Collectors.toSet()).equals(dependency.getDependenciesIds()))
                .findFirst();
    }

    public boolean doesItDependOn(Number serviceId) {
        return dependenciesIds.contains(serviceId);
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

    public Map<String, EntityFieldValueHandler<?>> updateOutputPath(String outputPath) {
        this.outputPath = outputPath;
        return ImmutableMap.of("outputPath", new SetFieldValueHandler<>(outputPath));
    }

    public Map<String, EntityFieldValueHandler<?>> updateErrorPath(String errorPath) {
        this.errorPath = errorPath;
        return ImmutableMap.of("errorPath", new SetFieldValueHandler<>(errorPath));
    }

    public Map<String, EntityFieldValueHandler<?>> updateState(JacsServiceState state) {
        Map<String, EntityFieldValueHandler<?>> dataUpdates = new LinkedHashMap<>();

        Preconditions.checkArgument(state != null);
        if (state != this.state) {
            JacsServiceEvent updateStateEvent = JacsServiceData.createServiceEvent(JacsServiceEventTypes.UPDATE_STATE, "Update state from " + this.state + " -> " + state);
            dataUpdates.putAll(addNewEvent(updateStateEvent));
            this.state = state;
            dataUpdates.put("state", new SetFieldValueHandler<>(state));
        }
        return dataUpdates;
    }
}
