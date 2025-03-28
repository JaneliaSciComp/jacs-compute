package org.janelia.model.access.dao;

import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.Preference;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.ReverseReference;
import org.janelia.model.domain.enums.PipelineStatus;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.gui.cdmip.ColorDepthLibrary;
import org.janelia.model.domain.gui.cdmip.ColorDepthResult;
import org.janelia.model.domain.ontology.Annotation;
import org.janelia.model.domain.ontology.Ontology;
import org.janelia.model.domain.ontology.OntologyTerm;
import org.janelia.model.domain.ontology.OntologyTermReference;
import org.janelia.model.domain.orders.IntakeOrder;
import org.janelia.model.domain.sample.DataSet;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.sample.LineRelease;
import org.janelia.model.domain.sample.NeuronFragment;
import org.janelia.model.domain.sample.NeuronSeparation;
import org.janelia.model.domain.sample.Sample;
import org.janelia.model.domain.sample.SampleLock;
import org.janelia.model.domain.sample.StatusTransition;
import org.janelia.model.domain.workspace.Node;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.domain.workspace.Workspace;
import org.jongo.MongoCursor;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public interface LegacyDomainDao {

    class DaoIndex {
        private final String keys;
        private final String options;

        public DaoIndex(String keys) {
            this(keys, null);
        }

        public DaoIndex(String keys, String options) {
            this.keys = keys;
            this.options = options;
        }

        public String getKeys() {
            return keys;
        }

        public String getOptions() {
            return options;
        }
    }

    List<Workspace> getWorkspaces(String subjectKey);

    /**
     * Return all the preferences for a given subject.
     */
    List<Preference> getPreferences(String subjectKey);

    List<Preference> getPreferences(String subjectKey, String category) throws Exception;

    Preference getPreference(String subjectKey, String category, String key);

    Object getPreferenceValue(String subjectKey, String category, String key);

    void setPreferenceValue(String subjectKey, String category, String key, Object value) throws Exception;

    /**
     * Saves the given subject preference.
     *
     * @param subjectKey
     * @param preference
     * @return
     * @throws Exception
     */
    Preference save(String subjectKey, Preference preference) throws Exception;

    /**
     * Returns any nodes which reference the given object.
     *
     * @param domainObject
     * @return boolean
     * @throws Exception
     */
    List<Reference> getAllNodeContainerReferences(DomainObject domainObject) throws Exception;

    /**
     * Returns the count of tree nodes which reference the given object.
     *
     * @param domainObject
     * @return
     * @throws Exception
     */
    long getTreeNodeContainerReferenceCount(DomainObject domainObject) throws Exception;

    /**
     * Returns the count of tree nodes which reference the given object.
     *
     * @param references
     * @return
     * @throws Exception
     */
    long getTreeNodeContainerReferenceCount(Collection<Reference> references) throws Exception;

    List<TreeNode> getTreeNodeContainers(String subjectKey, Collection<Reference> references) throws Exception;

    Stream<? extends DomainObject> iterateDomainObjects(Collection<Reference> references);

    <T extends DomainObject> Stream<T> iterateDomainObjects(Class<T> domainClass);

    /**
     * Create a list of the result set in iteration order.
     */
    <T> List<T> toList(MongoCursor<? extends T> cursor);

    /**
     * Create a list of the result set in the order of the given id list. If ids is null then
     * return the result set in the order it comes back.
     */
    <T extends DomainObject> List<T> toList(MongoCursor<T> cursor, Collection<Long> ids);

    <T extends DomainObject> void deleteDomainObject(String subjectKey, Class<T> domainClass, Long id);

    <T extends DomainObject> boolean deleteDomainObjects(String subjectKey, Class<T> domainClass, List<Long> ids);

    /**
     * Retrieve a refresh copy of the given domain object from the database.
     */
    @SuppressWarnings("unchecked")
    <T extends DomainObject> T getDomainObject(String subjectKey, T domainObject);

    /**
     * Get the domain object referenced by the collection name and id.
     */
    @SuppressWarnings("unchecked")
    <T extends DomainObject> T getDomainObject(String subjectKey, Class<T> domainClass, Long id);

    /**
     * Get the domain object referenced by the given Reference.
     */
    DomainObject getDomainObject(String subjectKey, Reference reference);

    <T extends DomainObject> List<T> getDomainObjectsAs(List<Reference> references, Class<T> clazz);

    <T extends DomainObject> List<T> getDomainObjectsAs(String subjectKey, List<Reference> references, Class<T> clazz);

    List<DomainObject> getDomainObjects(String subjectKey, List<Reference> references);

    /**
     * Get the domain objects referenced by the given list of References.
     */
    List<DomainObject> getDomainObjects(String subjectKey, List<Reference> references, boolean ownedOnly);

    /**
     * Get the domain objects of a single class with the specified ids.
     *
     * @param subjectKey
     * @param className
     * @param ids
     * @return
     */
    <T extends DomainObject> List<T> getDomainObjects(String subjectKey, String className, Collection<Long> ids);

    <T extends DomainObject> List<T> getDomainObjects(String subjectKey, Class<T> domainClass);

    /**
     * Get the domain objects in the given collection name with the specified ids.
     */
    <T extends DomainObject> List<T> getDomainObjects(String subjectKey, Class<T> domainClass, Collection<Long> ids);

    <T extends DomainObject> List<T> getUserDomainObjects(String subjectKey, Class<T> domainClass);

    <T extends DomainObject> List<T> getUserDomainObjects(String subjectKey, String className, Collection<Long> ids);

    /**
     * Get the domain objects owned by the given user, in the given collection name, with the specified ids.
     */
    <T extends DomainObject> List<T> getUserDomainObjects(String subjectKey, Class<T> domainClass, Collection<Long> ids);

    /**
     * Get the domain objects referenced by the given reverse reference.
     *
     * @param subjectKey
     * @param reverseRef
     * @return
     */
    List<DomainObject> getDomainObjects(String subjectKey, ReverseReference reverseRef);

    /**
     * Get the domain object by name.
     */
    <T extends DomainObject> List<T> getDomainObjectsByName(String subjectKey, Class<T> domainClass, String name);

    /**
     * Get the domain object by name.
     */
    <T extends DomainObject> List<T> getUserDomainObjectsByName(String subjectKey, Class<T> domainClass, String name);

    /**
     * Get domain objects of a given type with a given specified property value.
     */
    <T extends DomainObject> List<T> getDomainObjectsWithProperty(String subjectKey, Class<T> domainClass, String propName, String propValue);

    List<Annotation> getAnnotations(String subjectKey, Reference reference);

    List<Annotation> getAnnotations(String subjectKey, Collection<Reference> references);

    List<Ontology> getOntologies(String subjectKey);

    OntologyTerm getErrorOntologyCategory();

    List<DataSet> getDataSets();

    List<DataSet> getDataSets(String subjectKey);

    List<DataSet> getUserDataSets(String subjectKey);

    DataSet getDataSetByIdentifier(String subjectKey, String dataSetIdentifier);

    List<String> getColorDepthPaths(String subjectKey, String libraryIdentifier, String alignmentSpace);

    ColorDepthImage getColorDepthImageByPath(String subjectKey, String filepath);

    List<ColorDepthLibrary> getLibrariesWithColorDepthImages(String subjectKey, String alignmentSpace);

    DataSet createDataSet(String subjectKey, DataSet dataSet) throws Exception;

    /**
     * Attempts to lock a sample for the given task id an owner. The caller must check the return value of this method. If null is returned,
     * then the sample could not be locked. Only if a non-null SampleLock is returned can the sample be considered locked.
     * @param subjectKey
     * @param sampleId
     * @param taskId
     * @param description
     * @return
     */
    SampleLock lockSample(String subjectKey, Long sampleId, Long taskId, String description);

    /**
     * Attempts to unlock a sample, given the lock holder's task id and owner.
     * @param subjectKey
     * @param sampleId
     * @param taskId
     * @return
     */
    boolean unlockSample(String subjectKey, Long sampleId, Long taskId);

    List<Sample> getActiveSamplesByDataSet(String subjectKey, String dataSetIdentifier);

    List<Sample> getSamplesByDataSet(String subjectKey, String dataSetIdentifier);

    List<LSMImage> getActiveLsmsByDataSet(String subjectKey, String dataSetIdentifier);

    List<LSMImage> getLsmsByDataSet(String subjectKey, String dataSetIdentifier);

    List<Sample> getSamplesBySlideCode(String subjectKey, String dataSetIdentifier, String slideCode);

    Sample getActiveSampleBySlideCode(String subjectKey, String dataSetIdentifier, String slideCode);

    List<Sample> getUserSamplesBySlideCode(String subjectKey, String dataSetIdentifier, String slideCode);

    List<LSMImage> getActiveLsmsBySampleId(String subjectKey, Long sampleId);

    List<LSMImage> getInactiveLsmsBySampleId(String subjectKey, Long sampleId);

    List<LSMImage> getAllLsmsBySampleId(String subjectKey, Long sampleId);

    LSMImage getActiveLsmBySageId(String subjectKey, Integer sageId);

    List<LSMImage> getUserLsmsBySageId(String subjectKey, Integer sageId);

    List<NeuronFragment> getNeuronFragmentsBySampleId(String subjectKey, Long sampleId);

    List<NeuronFragment> getNeuronFragmentsBySeparationId(String subjectKey, Long separationId);

    Sample getSampleBySeparationId(String subjectKey, Long separationId);

    NeuronSeparation getNeuronSeparation(String subjectKey, Long separationId) throws Exception;

    TreeNode getTreeNodeById(String subjectKey, Long id);

    TreeNode getParentTreeNodes(String subjectKey, Reference ref);

    boolean isAdmin(String subjectKey);

    /**
     * Create the given object, with the given id. Dangerous to use if you don't know what you're doing! Use save() instead.
     * @param subjectKey
     * @param domainObject
     * @return
     * @throws Exception
     */
    <T extends DomainObject> T createWithPrepopulatedId(String subjectKey, T domainObject) throws Exception;

    /**
     * Saves the given object and returns a saved copy.
     *
     * @param subjectKey The subject saving the object. If this is a new object, then this subject becomes the owner of the new object.
     * @param domainObject The object to be saved. If the id is not set, then a new object is created.
     * @return a copy of the saved object
     * @throws Exception
     */
    <T extends DomainObject> T save(String subjectKey, T domainObject) throws Exception;

    void remove(String subjectKey, DomainObject domainObject) throws Exception;

    Ontology reorderTerms(String subjectKey, Long ontologyId, Long parentTermId, int[] order) throws Exception;

    Ontology addTerms(String subjectKey, Long ontologyId, Long parentTermId, Collection<OntologyTerm> terms, Integer index) throws Exception;

    Ontology removeTerm(String subjectKey, Long ontologyId, Long parentTermId, Long termId) throws Exception;

    TreeNode getOrCreateDefaultTreeNodeFolder(String subjectKey, String folderName) throws Exception;

    Node reorderChildren(String subjectKey, Node nodeArg, int[] order) throws Exception;

    List<DomainObject> getChildren(String subjectKey, Node node);

    Node addChildren(String subjectKey, Node nodeArg, Collection<Reference> references) throws Exception;

    Node addChildren(String subjectKey, Node nodeArg, Collection<Reference> references, Integer index) throws Exception;

    Node removeChildren(String subjectKey, Node nodeArg, Collection<Reference> references) throws Exception;

    Node removeReference(String subjectKey, Node nodeArg, Reference reference) throws Exception;

    <T extends DomainObject> T updateProperty(String subjectKey, Class<T> clazz, Long id, String propName, Object propValue, Class<?> propType) throws Exception;

    default <T extends DomainObject> T updateProperty(String subjectKey, Class<T> clazz, Long id, String propName, Object propValue) throws Exception {
        return updateProperty(subjectKey, clazz, id, propName, propValue, propValue != null ? propValue.getClass() : Object.class);
    }

    DomainObject updateProperty(String subjectKey, String className, Long id, String propName, Object propValue, Class<?> propType) throws Exception;

    <T extends DomainObject> void deleteProperty(String ownerKey, Class<T> clazz, String propName);

    void addPermissions(String ownerKey, String className, Long id, DomainObject permissionTemplate, boolean forceChildUpdates) throws Exception;

    void setPermissions(String ownerKey, String className, Long id, String grantee, boolean read, boolean write, boolean forceChildUpdates) throws Exception;

    void giveOwnerReadWriteToAllFromCollection(String collectionName);

    void addPipelineStatusTransition(Long sampleId, PipelineStatus source, PipelineStatus target, String orderNo,
                                     String process, Map<String, Object> parameters) throws Exception;

    List<StatusTransition> getPipelineStatusTransitionsBySampleId(Long sampleId) throws Exception;

    void addIntakeOrder(String orderNo, String owner) throws Exception;

    void addOrUpdateIntakeOrder(IntakeOrder order) throws Exception;

    // returns order information (including Sample Ids) given a number of hours time window
    List<IntakeOrder> getIntakeOrders(Calendar cutoffDate) throws Exception;

    // returns specific order information
    IntakeOrder getIntakeOrder(String orderNo) throws Exception;

    void addSampleToOrder(String orderNo, Long sampleId) throws Exception;

    // add SampleIds to order as they get processed
    void addSampleIdsToOrder(String orderNo, List<Long> sampleIds) throws Exception;

    Long getNewId();

    List<LineRelease> getLineReleases(String subjectKey);

    LineRelease createLineRelease(String subjectKey, String name) throws Exception;

    /**
     * Sum the disk space usage of all the samples in the given data set, and cache it within the corresponding DataDet object.
     * @param dataSetIdentifier unique identifier of the data set to update
     * @throws Exception
     */
    void updateDataSetDiskspaceUsage(String dataSetIdentifier) throws Exception;

    void addColorDepthSearchResult(String subjectKey, Long searchId, ColorDepthResult result);

    public List<ColorDepthImage> getColorDepthImages(String subjectKey, String libraryIdentifier, String alignmentSpace);

    public Map<String,Map<String,Integer>> getColorDepthCounts();

    public void updateColorDepthCounts(Map<String,Map<String,Integer>> counts) throws Exception;

    <T extends DomainObject> List<T> fullTextSearch(String subjectKey, Class<T> domainClass, String text);

    void ensureCollectionIndex(String collectionName, List<DaoIndex> indexes);
}
