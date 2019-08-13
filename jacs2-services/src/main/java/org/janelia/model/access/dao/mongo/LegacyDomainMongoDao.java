package org.janelia.model.access.dao.mongo;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.mongodb.DBCursor;
import org.apache.commons.lang3.StringUtils;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.DomainDAO;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.DomainUtils;
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
import org.janelia.model.security.Group;
import org.janelia.model.security.GroupRole;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LegacyDomainMongoDao implements LegacyDomainDao {

    private static final Logger log = LoggerFactory.getLogger(LegacyDomainMongoDao.class);

    private final DomainDAO dao;
    private final DomainObjectIndexer domainObjectIndexer;

    @Inject
    public LegacyDomainMongoDao(DomainDAO dao,
                                @AsyncIndex DomainObjectIndexer domainObjectIndexer) {
        this.dao = dao;
        this.domainObjectIndexer = domainObjectIndexer;
    }

    @Override
    public Subject save(Subject subject) {
        return dao.save(subject);
    }

    @Override
    public List<Subject> getSubjects() {
        return dao.getSubjects();
    }

    @Override
    public List<Subject> getUsers() throws Exception {
        return dao.getUsers();
    }

    @Override
    public List<Subject> getGroups() throws Exception {
        return dao.getGroups();
    }

    @Override
    public Set<String> getReaderSet(String subjectKey) {
        return dao.getReaderSet(subjectKey);
    }

    @Override
    public Set<String> getWriterSet(String subjectKey) {
        return dao.getWriterSet(subjectKey);
    }

    @Override
    public Subject getSubjectByKey(String subjectKey) {
        return dao.getSubjectByKey(subjectKey);
    }

    @Override
    public Subject getSubjectByName(String subjectName) {
        return dao.getSubjectByName(subjectName);
    }

    @Override
    public Subject getSubjectByNameOrKey(String subjectNameOrKey) {
        return dao.getSubjectByNameOrKey(subjectNameOrKey);
    }

    @Override
    public User getUserByNameOrKey(String subjectNameOrKey) {
        return dao.getUserByNameOrKey(subjectNameOrKey);
    }

    @Override
    public Group getGroupByNameOrKey(String subjectNameOrKey) {
        return dao.getGroupByNameOrKey(subjectNameOrKey);
    }

    @Override
    public User createUser(String name, String fullName, String email) throws Exception {
        return dao.createUser(name, fullName, email);
    }

    @Override
    public Group createGroup(String name, String fullName) throws Exception {
        return dao.createGroup(name, fullName);
    }

    @Override
    public void remove(Subject subject) throws Exception {
        dao.remove(subject);
    }

    @Override
    public void removeUser(String userNameOrKey) throws Exception {
        dao.removeUser(userNameOrKey);
    }

    @Override
    public void removeGroup(String groupNameOrKey) throws Exception {
        dao.removeGroup(groupNameOrKey);
    }

    @Override
    public void addUserToGroup(String userNameOrKey, String groupNameOrKey, GroupRole role) throws Exception {
        dao.addUserToGroup(userNameOrKey, groupNameOrKey, role);
    }

    @Override
    public void removeUserFromGroup(String userNameOrKey, String groupNameOrKey) throws Exception {
        dao.removeUserFromGroup(userNameOrKey, groupNameOrKey);
    }

    @Override
    public void createWorkspace(String ownerKey) throws Exception {
        dao.createWorkspace(ownerKey);
    }

    @Override
    public List<Workspace> getWorkspaces(String subjectKey) {
        return dao.getWorkspaces(subjectKey);
    }

    @Override
    public Workspace getDefaultWorkspace(String subjectKey) {
        return dao.getDefaultWorkspace(subjectKey);
    }

    @Override
    public List<Preference> getPreferences(String subjectKey) {
        return dao.getPreferences(subjectKey);
    }

    @Override
    public List<Preference> getPreferences(String subjectKey, String category) throws Exception {
        return dao.getPreferences(subjectKey, category);
    }

    @Override
    public Preference getPreference(String subjectKey, String category, String key) {
        return dao.getPreference(subjectKey, category, key);
    }

    @Override
    public Object getPreferenceValue(String subjectKey, String category, String key) {
        return dao.getPreferenceValue(subjectKey, category, key);
    }

    @Override
    public void setPreferenceValue(String subjectKey, String category, String key, Object value) throws Exception {
        dao.setPreferenceValue(subjectKey, category, key, value);
    }

    @Override
    public Preference save(String subjectKey, Preference preference) throws Exception {
        return dao.save(subjectKey, preference);
    }

    @Override
    public List<Reference> getAllNodeContainerReferences(DomainObject domainObject) throws Exception {
        return dao.getAllNodeReferences(domainObject);
    }

    @Override
    public long getTreeNodeContainerReferenceCount(DomainObject domainObject) throws Exception {
        return dao.getTreeNodeContainerReferenceCount(domainObject);
    }

    @Override
    public long getTreeNodeContainerReferenceCount(Collection<Reference> references) throws Exception {
        return dao.getTreeNodeContainerReferenceCount(references);
    }

    @Override
    public List<TreeNode> getTreeNodeContainers(String subjectKey, Collection<Reference> references) throws Exception {
        return dao.getTreeNodeContainers(subjectKey, references);
    }

    @Override
    public <T extends DomainObject> Stream<T> iterateDomainObjects(Class<T> domainClass) {
        Spliterator<T> iterator = new Spliterator<T>() {
            Iterator<T> cursor;
            {
                setCursor();
            }

            private void setCursor() {
                cursor = dao.getCollectionByClass(domainClass).find(
                        "{$or:[{class:{$exists:0}},{class:#}]}", domainClass.getName())
                        .with((DBCursor cursor) -> cursor.noCursorTimeout(true))
                        .as(domainClass)
                        ;
            }

            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                if (cursor.hasNext()) {
                    action.accept(cursor.next());
                }
                return cursor.hasNext();
            }

            @Override
            public Spliterator<T> trySplit() {
                return null;
            }

            @Override
            public long estimateSize() {
                int estimateSize = dao.getCollectionByClass(domainClass).find(
                        "{$or:[{class:{$exists:0}},{class:#}]}", domainClass.getName())
                        .with((DBCursor cursor) -> cursor.noCursorTimeout(true))
                        .as(domainClass)
                        .count();
                return estimateSize;
            }

            @Override
            public int characteristics() {
                return 0;
            }
        };
        return StreamSupport.stream(iterator, false);
    }

    @Override
    public <T> List<T> toList(MongoCursor<? extends T> cursor) {
        return dao.toList(cursor);
    }

    @Override
    public <T extends DomainObject> List<T> toList(MongoCursor<T> cursor, Collection<Long> ids) {
        return dao.toList(cursor, ids);
    }

    @Override
    public <T extends DomainObject> void deleteDomainObject(String subjectKey, Class<T> domainClass, Long id) {
        dao.deleteDomainObject(subjectKey, domainClass, id);
        domainObjectIndexer.removeDocument(id);
    }

    @Override
    public <T extends DomainObject> boolean deleteDomainObjects(String subjectKey, Class<T> domainClass, List<Long> ids) {
        boolean deleted = dao.deleteDomainObjects(subjectKey, domainClass, ids);
        domainObjectIndexer.removeDocumentStream(ids.stream());
        return deleted;
    }

    @Override
    public <T extends DomainObject> T getDomainObject(String subjectKey, T domainObject) {
        return dao.getDomainObject(subjectKey, domainObject);
    }

    @Override
    public <T extends DomainObject> T getDomainObject(String subjectKey, Class<T> domainClass, Long id) {
        return dao.getDomainObject(subjectKey, domainClass, id);
    }

    @Override
    public DomainObject getDomainObject(String subjectKey, Reference reference) {
        return dao.getDomainObject(subjectKey, reference);
    }

    @Override
    public <T extends DomainObject> List<T> getDomainObjectsAs(List<Reference> references, Class<T> clazz) {
        return dao.getDomainObjectsAs(references, clazz);
    }

    @Override
    public <T extends DomainObject> List<T> getDomainObjectsAs(String subjectKey, List<Reference> references, Class<T> clazz) {
        return dao.getDomainObjectsAs(subjectKey, references, clazz);
    }

    @Override
    public List<DomainObject> getDomainObjects(String subjectKey, List<Reference> references) {
        return dao.getDomainObjects(subjectKey, references);
    }

    @Override
    public List<DomainObject> getDomainObjects(String subjectKey, List<Reference> references, boolean ownedOnly) {
        return dao.getDomainObjects(subjectKey, references, ownedOnly);
    }

    @Override
    public <T extends DomainObject> List<T> getDomainObjects(String subjectKey, String className, Collection<Long> ids) {
        return dao.getDomainObjects(subjectKey, className, ids);
    }

    @Override
    public <T extends DomainObject> List<T> getDomainObjects(String subjectKey, Class<T> domainClass) {
        return dao.getDomainObjects(subjectKey, domainClass);
    }

    @Override
    public <T extends DomainObject> List<T> getDomainObjects(String subjectKey, Class<T> domainClass, Collection<Long> ids) {
        return dao.getDomainObjects(subjectKey, domainClass, ids);
    }

    @Override
    public <T extends DomainObject> List<T> getUserDomainObjects(String subjectKey, Class<T> domainClass) {
        return dao.getUserDomainObjects(subjectKey, domainClass);
    }

    @Override
    public <T extends DomainObject> List<T> getUserDomainObjects(String subjectKey, String className, Collection<Long> ids) {
        return dao.getUserDomainObjects(subjectKey, className, ids);
    }

    @Override
    public <T extends DomainObject> List<T> getUserDomainObjects(String subjectKey, Class<T> domainClass, Collection<Long> ids) {
        return dao.getUserDomainObjects(subjectKey, domainClass, ids);
    }

    @Override
    public List<DomainObject> getDomainObjects(String subjectKey, ReverseReference reverseRef) {
        return dao.getDomainObjects(subjectKey, reverseRef);
    }

    @Override
    public <T extends DomainObject> List<T> getDomainObjectsByName(String subjectKey, Class<T> domainClass, String name) {
        return dao.getDomainObjectsByName(subjectKey, domainClass, name);
    }

    @Override
    public <T extends DomainObject> List<T> getUserDomainObjectsByName(String subjectKey, Class<T> domainClass, String name) {
        return dao.getUserDomainObjectsByName(subjectKey, domainClass, name);
    }

    @Override
    public <T extends DomainObject> List<T> getDomainObjectsWithProperty(String subjectKey, Class<T> domainClass, String propName, String propValue) {
        return dao.getDomainObjectsWithProperty(subjectKey, domainClass, propName, propValue);
    }

    @Override
    public List<Annotation> getAnnotations(String subjectKey, Reference reference) {
        return dao.getAnnotations(subjectKey, reference);
    }

    @Override
    public List<Annotation> getAnnotations(String subjectKey, Collection<Reference> references) {
        return dao.getAnnotations(subjectKey, references);
    }

    @Override
    public List<Ontology> getOntologies(String subjectKey) {
        return dao.getOntologies(subjectKey);
    }

    @Override
    public OntologyTerm getErrorOntologyCategory() {
        return dao.getErrorOntologyCategory();
    }

    @Override
    public Annotation createAnnotation(String subjectKey, Reference target, OntologyTermReference ontologyTermReference, Object value) throws Exception {
        Annotation annotation = dao.createAnnotation(subjectKey, target, ontologyTermReference, value);
        domainObjectIndexer.indexDocument(annotation);
        return annotation;
    }

    @Override
    public List<DataSet> getDataSets() {
        return dao.getDataSets();
    }

    @Override
    public List<DataSet> getDataSets(String subjectKey) {
        return dao.getDataSets(subjectKey);
    }

    @Override
    public List<DataSet> getUserDataSets(String subjectKey) {
        return dao.getUserDataSets(subjectKey);
    }

    @Override
    public DataSet getDataSetByIdentifier(String subjectKey, String dataSetIdentifier) {
        return dao.getDataSetByIdentifier(subjectKey, dataSetIdentifier);
    }

    @Override
    public List<String> getColorDepthPaths(String subjectKey, String libraryIdentifier, String alignmentSpace) {
        return dao.getColorDepthPaths(subjectKey, libraryIdentifier, alignmentSpace);
    }

    @Override
    public ColorDepthImage getColorDepthImageByPath(String subjectKey, String filepath) {
        return dao.getColorDepthImageByPath(subjectKey, filepath);
    }

    @Override
    public List<ColorDepthLibrary> getLibrariesWithColorDepthImages(String subjectKey, String alignmentSpace) {
        return dao.getLibrariesWithColorDepthImages(subjectKey, alignmentSpace);
    }

    @Override
    public DataSet createDataSet(String subjectKey, DataSet dataSet) throws Exception {
        DataSet ds = dao.createDataSet(subjectKey, dataSet);
        domainObjectIndexer.indexDocument(ds);
        return ds;
    }

    @Override
    public SampleLock lockSample(String subjectKey, Long sampleId, Long taskId, String description) {
        return dao.lockSample(subjectKey, sampleId, taskId, description);
    }

    @Override
    public boolean unlockSample(String subjectKey, Long sampleId, Long taskId) {
        return dao.unlockSample(subjectKey, sampleId, taskId);
    }

    @Override
    public List<Sample> getActiveSamplesByDataSet(String subjectKey, String dataSetIdentifier) {
        return dao.getActiveSamplesByDataSet(subjectKey, dataSetIdentifier);
    }

    @Override
    public List<Sample> getSamplesByDataSet(String subjectKey, String dataSetIdentifier) {
        return dao.getSamplesByDataSet(subjectKey, dataSetIdentifier);
    }

    @Override
    public List<LSMImage> getActiveLsmsByDataSet(String subjectKey, String dataSetIdentifier) {
        return dao.getActiveLsmsByDataSet(subjectKey, dataSetIdentifier);
    }

    @Override
    public List<LSMImage> getLsmsByDataSet(String subjectKey, String dataSetIdentifier) {
        return dao.getLsmsByDataSet(subjectKey, dataSetIdentifier);
    }

    @Override
    public List<Sample> getSamplesBySlideCode(String subjectKey, String dataSetIdentifier, String slideCode) {
        return dao.getSamplesBySlideCode(subjectKey, dataSetIdentifier, slideCode);
    }

    @Override
    public Sample getActiveSampleBySlideCode(String subjectKey, String dataSetIdentifier, String slideCode) {
        return dao.getActiveSampleBySlideCode(subjectKey, dataSetIdentifier, slideCode);
    }

    @Override
    public List<Sample> getUserSamplesBySlideCode(String subjectKey, String dataSetIdentifier, String slideCode) {
        return dao.getUserSamplesBySlideCode(subjectKey, dataSetIdentifier, slideCode);
    }

    @Override
    public List<LSMImage> getActiveLsmsBySampleId(String subjectKey, Long sampleId) {
        return dao.getActiveLsmsBySampleId(subjectKey, sampleId);
    }

    @Override
    public List<LSMImage> getInactiveLsmsBySampleId(String subjectKey, Long sampleId) {
        return dao.getInactiveLsmsBySampleId(subjectKey, sampleId);
    }

    @Override
    public List<LSMImage> getAllLsmsBySampleId(String subjectKey, Long sampleId) {
        return dao.getAllLsmsBySampleId(subjectKey, sampleId);
    }

    @Override
    public LSMImage getActiveLsmBySageId(String subjectKey, Integer sageId) {
        return dao.getActiveLsmBySageId(subjectKey, sageId);
    }

    @Override
    public List<LSMImage> getUserLsmsBySageId(String subjectKey, Integer sageId) {
        return dao.getUserLsmsBySageId(subjectKey, sageId);
    }

    @Override
    public List<NeuronFragment> getNeuronFragmentsBySampleId(String subjectKey, Long sampleId) {
        return dao.getNeuronFragmentsBySampleId(subjectKey, sampleId);
    }

    @Override
    public List<NeuronFragment> getNeuronFragmentsBySeparationId(String subjectKey, Long separationId) {
        return dao.getNeuronFragmentsBySeparationId(subjectKey, separationId);
    }

    @Override
    public Sample getSampleBySeparationId(String subjectKey, Long separationId) {
        return dao.getSampleBySeparationId(subjectKey, separationId);
    }

    @Override
    public NeuronSeparation getNeuronSeparation(String subjectKey, Long separationId) throws Exception {
        return dao.getNeuronSeparation(subjectKey, separationId);
    }

    @Override
    public TreeNode getTreeNodeById(String subjectKey, Long id) {
        return dao.getTreeNodeById(subjectKey, id);
    }

    @Override
    public TreeNode getParentTreeNodes(String subjectKey, Reference ref) {
        return dao.getParentTreeNodes(subjectKey, ref);
    }

    @Override
    public boolean isAdmin(String subjectKey) {
        return dao.isAdmin(subjectKey);
    }

    @Override
    public <T extends DomainObject> T createWithPrepopulatedId(String subjectKey, T domainObject) throws Exception {
        T persistedDomainObject = dao.createWithPrepopulatedId(subjectKey, domainObject);
        domainObjectIndexer.indexDocument(persistedDomainObject);
        return persistedDomainObject;
    }

    @Override
    public <T extends DomainObject> T save(String subjectKey, T domainObject) throws Exception {
        T persistedDomainObject = dao.save(subjectKey, domainObject);
        domainObjectIndexer.indexDocument(persistedDomainObject);
        return persistedDomainObject;
    }

    @Override
    public void remove(String subjectKey, DomainObject domainObject) throws Exception {
        dao.remove(subjectKey, domainObject);
        domainObjectIndexer.removeDocument(domainObject.getId());
    }

    @Override
    public Ontology reorderTerms(String subjectKey, Long ontologyId, Long parentTermId, int[] order) throws Exception {
        return dao.reorderTerms(subjectKey, ontologyId, parentTermId, order);
    }

    @Override
    public Ontology addTerms(String subjectKey, Long ontologyId, Long parentTermId, Collection<OntologyTerm> terms, Integer index) throws Exception {
        Ontology updatedOntoly = dao.addTerms(subjectKey, ontologyId, parentTermId, terms, index);
        domainObjectIndexer.indexDocument(updatedOntoly);
        return updatedOntoly;
    }

    @Override
    public Ontology removeTerm(String subjectKey, Long ontologyId, Long parentTermId, Long termId) throws Exception {
        Ontology updatedOntoly = dao.removeTerm(subjectKey, ontologyId, parentTermId, termId);
        domainObjectIndexer.indexDocument(updatedOntoly);
        return updatedOntoly;
    }

    @Override
    public TreeNode getOrCreateDefaultTreeNodeFolder(String subjectKey, String folderName) throws Exception {
        return dao.getOrCreateDefaultTreeNodeFolder(subjectKey, folderName);
    }

    @Override
    public Node reorderChildren(String subjectKey, Node nodeArg, int[] order) throws Exception {
        return dao.reorderChildren(subjectKey, nodeArg, order);
    }

    @Override
    public List<DomainObject> getChildren(String subjectKey, Node node) {
        return dao.getChildren(subjectKey, node);
    }

    @Override
    public Node addChildren(String subjectKey, Node treeNodeArg, Collection<Reference> references) throws Exception {
        Node updatedNode = dao.addChildren(subjectKey, treeNodeArg, references);
        domainObjectIndexer.updateDocsAncestors(
                references.stream().map(ref -> ref.getTargetId()).collect(Collectors.toSet()),
                updatedNode.getId());
        return updatedNode;
    }

    @Override
    public Node addChildren(String subjectKey, Node nodeArg, Collection<Reference> references, Integer index) throws Exception {
        Node updatedNode = dao.addChildren(subjectKey, nodeArg, references, index);
        domainObjectIndexer.updateDocsAncestors(
                references.stream().map(ref -> ref.getTargetId()).collect(Collectors.toSet()),
                updatedNode.getId());
        return updatedNode;
    }

    @Override
    public Node removeChildren(String subjectKey, Node nodeArg, Collection<Reference> references) throws Exception {
        Node updatedNode = dao.removeChildren(subjectKey, nodeArg, references);
        List<DomainObject> children = dao.getDomainObjects(subjectKey, ImmutableList.copyOf(references));
        domainObjectIndexer.indexDocumentStream(children.stream()); // reindex the children
        return updatedNode;
    }

    @Override
    public Node removeReference(String subjectKey, Node nodeArg, Reference reference) throws Exception {
        Node updatedNode = dao.removeReference(subjectKey, nodeArg, reference);
        domainObjectIndexer.updateDocsAncestors(ImmutableSet.of(reference.getTargetId()), updatedNode.getId());
        return updatedNode;
    }

    @Override
    public <T extends DomainObject> T updateProperty(String subjectKey, Class<T> clazz, Long id, String propName, Object propValue) throws Exception {
        T updatedDomainObject = dao.updateProperty(subjectKey, clazz, id, propName, propValue);
        domainObjectIndexer.indexDocument(updatedDomainObject);
        return updatedDomainObject;
    }

    @Override
    public DomainObject updateProperty(String subjectKey, String className, Long id, String propName, Object propValue) throws Exception {
        DomainObject updatedDomainObject = dao.updateProperty(subjectKey, className, id, propName, propValue);
        domainObjectIndexer.indexDocument(updatedDomainObject);
        return updatedDomainObject;
    }

    @Override
    public <T extends DomainObject> void deleteProperty(String ownerKey, Class<T> clazz, String propName) {
        Collection<? extends DomainObject> affectedDomainObjects = dao.deleteProperty(ownerKey, clazz, propName);
        domainObjectIndexer.indexDocumentStream(affectedDomainObjects.stream());
    }

    @Override
    public void addPermissions(String ownerKey, String className, Long id, DomainObject permissionTemplate, boolean forceChildUpdates) throws Exception {
        Collection<? extends DomainObject> affectedDomainObjects = dao.addPermissions(ownerKey, className, id, permissionTemplate, forceChildUpdates);
        domainObjectIndexer.indexDocumentStream(affectedDomainObjects.stream());
    }

    @Override
    public void setPermissions(String ownerKey, String className, Long id, String grantee, boolean read, boolean write, boolean forceChildUpdates) throws Exception {
        DomainObject affectedDomainObject = dao.setPermissions(ownerKey, className, id, grantee, read, write, forceChildUpdates);
        domainObjectIndexer.indexDocument(affectedDomainObject);
    }

    @Override
    public void giveOwnerReadWriteToAllFromCollection(String collectionName) {
        Class<?> baseClass = DomainUtils.getBaseClass(collectionName);
        List<DomainObject> toIndex = new LinkedList<>();
        if (DomainObject.class.isAssignableFrom(baseClass)) {
            MongoCollection mongoCollection = dao.getCollectionByName(collectionName);
            Iterable<?> iterable = mongoCollection.find().as(baseClass);
            if (iterable != null) {
                for (Object obj : iterable) {
                    DomainObject domainObject = (DomainObject) obj;
                    toIndex.add(domainObject);
                    String ownerKey = domainObject.getOwnerKey();
                    if (StringUtils.isNotBlank(ownerKey)) {
                        mongoCollection.update("{_id:#}", domainObject.getId()).with("{$addToSet:{readers:#,writers:#}}", ownerKey, ownerKey);
                    }
                }
                domainObjectIndexer.indexDocumentStream(toIndex.stream());
            }
        }
    }

    @Override
    public void addPipelineStatusTransition(Long sampleId, PipelineStatus source, PipelineStatus target, String orderNo, String process, Map<String, Object> parameters) throws Exception {
        dao.addPipelineStatusTransition(sampleId, source, target, orderNo, process, parameters);
    }

    @Override
    public List<StatusTransition> getPipelineStatusTransitionsBySampleId(Long sampleId) throws Exception {
        return dao.getPipelineStatusTransitionsBySampleId(sampleId);
    }

    @Override
    public void addIntakeOrder(String orderNo, String owner) throws Exception {
        dao.addIntakeOrder(orderNo, owner);
    }

    @Override
    public void addOrUpdateIntakeOrder(IntakeOrder order) throws Exception {
        dao.addOrUpdateIntakeOrder(order);
    }

    @Override
    public List<IntakeOrder> getIntakeOrders(Calendar cutoffDate) throws Exception {
        return dao.getIntakeOrders(cutoffDate);
    }

    @Override
    public IntakeOrder getIntakeOrder(String orderNo) throws Exception {
        return dao.getIntakeOrder(orderNo);
    }

    @Override
    public void addSampleToOrder(String orderNo, Long sampleId) throws Exception {
        dao.addSampleToOrder(orderNo, sampleId);
    }

    @Override
    public void addSampleIdsToOrder(String orderNo, List<Long> sampleIds) throws Exception {
        dao.addSampleIdsToOrder(orderNo, sampleIds);
    }

    @Override
    public Long getNewId() {
        return dao.getNewId();
    }

    @Override
    public List<LineRelease> getLineReleases(String subjectKey) {
        return dao.getLineReleases(subjectKey);
    }

    @Override
    public LineRelease createLineRelease(String subjectKey, String name, Date releaseDate, Integer lagTimeMonths, List<String> dataSets) throws Exception {
        return dao.createLineRelease(subjectKey, name, releaseDate, lagTimeMonths, dataSets);
    }

    @Override
    public void updateDataSetDiskspaceUsage(String dataSetIdentifier) throws Exception {
        dao.updateDataSetDiskspaceUsage(dataSetIdentifier);
    }

    @Override
    public void addColorDepthSearchResult(String subjectKey, Long searchId, ColorDepthResult result) {
        dao.addColorDepthSearchResult(subjectKey, searchId, result);
    }

    @Override
    public <T extends DomainObject> List<T> fullTextSearch(String subjectKey, Class<T> domainClass, String text) {
        return dao.fullTextSearch(subjectKey, domainClass, text);
    }

    @Override
    public void ensureCollectionIndex(String collectionName, List<DaoIndex> indexes) {
        MongoCollection mongoCollection = dao.getCollectionByName(collectionName);
        log.info("Creating indexes on {}", collectionName);
        for (DaoIndex index : indexes) {
            if (StringUtils.isBlank(index.getOptions())) {
                mongoCollection.ensureIndex(index.getKeys());
            } else {
                mongoCollection.ensureIndex(index.getKeys(), index.getOptions());
            }
            if (log.isInfoEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append(index.getKeys());
                if (index.getOptions() != null) {
                    sb.append(", ").append(index.getOptions());
                }
                log.info("  {}", sb);
            }
        }
    }
}
