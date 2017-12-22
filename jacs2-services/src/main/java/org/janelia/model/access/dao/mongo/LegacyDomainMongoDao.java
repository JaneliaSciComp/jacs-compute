package org.janelia.model.access.dao.mongo;

import com.mongodb.MongoClient;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.DomainDAO;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.Preference;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.ReverseReference;
import org.janelia.model.domain.enums.PipelineStatus;
import org.janelia.model.domain.ontology.Annotation;
import org.janelia.model.domain.ontology.Ontology;
import org.janelia.model.domain.ontology.OntologyTerm;
import org.janelia.model.domain.ontology.OntologyTermReference;
import org.janelia.model.domain.orders.IntakeOrder;
import org.janelia.model.domain.sample.*;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.domain.workspace.Workspace;
import org.janelia.model.security.Group;
import org.janelia.model.security.GroupRole;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.jongo.MongoCursor;

import javax.inject.Inject;
import java.util.*;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LegacyDomainMongoDao implements LegacyDomainDao {

    private DomainDAO dao;

    @Inject
    public LegacyDomainMongoDao(MongoClient mongoClient,
                                @PropertyValue(name = "MongoDB.Database") String databaseName) {
        this.dao = new DomainDAO(mongoClient, databaseName);
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
    public List<Reference> getContainerReferences(DomainObject domainObject) throws Exception {
        return dao.getContainerReferences(domainObject);
    }

    @Override
    public long getContainerReferenceCount(DomainObject domainObject) throws Exception {
        return dao.getContainerReferenceCount(domainObject);
    }

    @Override
    public long getContainerReferenceCount(Collection<Reference> references) throws Exception {
        return dao.getContainerReferenceCount(references);
    }

    @Override
    public <T extends DomainObject> List<TreeNode> getContainers(String subjectKey, Collection<Reference> references) throws Exception {
        return dao.getContainers(subjectKey, references);
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
    }

    @Override
    public <T extends DomainObject> boolean deleteDomainObjects(String subjectKey, Class<T> domainClass, List<Long> ids) {
        return dao.deleteDomainObjects(subjectKey, domainClass, ids);
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
        return dao.createAnnotation(subjectKey, target, ontologyTermReference, value);
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
    public DataSet createDataSet(String subjectKey, DataSet dataSet) throws Exception {
        return dao.createDataSet(subjectKey, dataSet);
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
    public List<Sample> getActiveSamplesForDataSet(String subjectKey, String dataSetIdentifier) {
        return dao.getActiveSamplesForDataSet(subjectKey, dataSetIdentifier);
    }

    @Override
    public List<Sample> getSamplesForDataSet(String subjectKey, String dataSetIdentifier) {
        return dao.getSamplesForDataSet(subjectKey, dataSetIdentifier);
    }

    @Override
    public List<LSMImage> getActiveLsmsForDataSet(String subjectKey, String dataSetIdentifier) {
        return dao.getActiveLsmsForDataSet(subjectKey, dataSetIdentifier);
    }

    @Override
    public List<LSMImage> getLsmsForDataSet(String subjectKey, String dataSetIdentifier) {
        return dao.getLsmsForDataSet(subjectKey, dataSetIdentifier);
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
        return dao.createWithPrepopulatedId(subjectKey, domainObject);
    }

    @Override
    public <T extends DomainObject> T save(String subjectKey, T domainObject) throws Exception {
        return dao.save(subjectKey, domainObject);
    }

    @Override
    public void remove(String subjectKey, DomainObject domainObject) throws Exception {
        dao.remove(subjectKey, domainObject);
    }

    @Override
    public Ontology reorderTerms(String subjectKey, Long ontologyId, Long parentTermId, int[] order) throws Exception {
        return dao.reorderTerms(subjectKey, ontologyId, parentTermId, order);
    }

    @Override
    public Ontology addTerms(String subjectKey, Long ontologyId, Long parentTermId, Collection<OntologyTerm> terms, Integer index) throws Exception {
        return dao.addTerms(subjectKey, ontologyId, parentTermId, terms, index);
    }

    @Override
    public Ontology removeTerm(String subjectKey, Long ontologyId, Long parentTermId, Long termId) throws Exception {
        return dao.removeTerm(subjectKey, ontologyId, parentTermId, termId);
    }

    @Override
    public TreeNode getOrCreateDefaultFolder(String subjectKey, String folderName) throws Exception {
        return dao.getOrCreateDefaultFolder(subjectKey, folderName);
    }

    @Override
    public TreeNode reorderChildren(String subjectKey, TreeNode treeNodeArg, int[] order) throws Exception {
        return dao.reorderChildren(subjectKey, treeNodeArg, order);
    }

    @Override
    public List<DomainObject> getChildren(String subjectKey, TreeNode treeNode) {
        return dao.getChildren(subjectKey, treeNode);
    }

    @Override
    public TreeNode addChildren(String subjectKey, TreeNode treeNodeArg, Collection<Reference> references) throws Exception {
        return dao.addChildren(subjectKey, treeNodeArg, references);
    }

    @Override
    public TreeNode addChildren(String subjectKey, TreeNode treeNodeArg, Collection<Reference> references, Integer index) throws Exception {
        return dao.addChildren(subjectKey, treeNodeArg, references, index);
    }

    @Override
    public TreeNode removeChildren(String subjectKey, TreeNode treeNodeArg, Collection<Reference> references) throws Exception {
        return dao.removeChildren(subjectKey, treeNodeArg, references);
    }

    @Override
    public TreeNode removeReference(String subjectKey, TreeNode treeNodeArg, Reference reference) throws Exception {
        return dao.removeReference(subjectKey, treeNodeArg, reference);
    }

    @Override
    public <T extends DomainObject> T updateProperty(String subjectKey, Class<T> clazz, Long id, String propName, Object propValue) throws Exception {
        return dao.updateProperty(subjectKey, clazz, id, propName, propValue);
    }

    @Override
    public DomainObject updateProperty(String subjectKey, String className, Long id, String propName, Object propValue) throws Exception {
        return dao.updateProperty(subjectKey, className, id, propName, propValue);
    }

    @Override
    public <T extends DomainObject> void deleteProperty(String ownerKey, Class<T> clazz, String propName) {
        dao.deleteProperty(ownerKey, clazz, propName);
    }

    @Override
    public void addPermissions(String ownerKey, String className, Long id, DomainObject permissionTemplate, boolean forceChildUpdates) throws Exception {
        dao.addPermissions(ownerKey, className, id, permissionTemplate, forceChildUpdates);
    }

    @Override
    public void setPermissions(String ownerKey, String className, Long id, String grantee, boolean read, boolean write, boolean forceChildUpdates) throws Exception {
        dao.setPermissions(ownerKey, className, id, grantee, read, write, forceChildUpdates);
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
    public <T extends DomainObject> List<T> fullTextSearch(String subjectKey, Class<T> domainClass, String text) {
        return dao.fullTextSearch(subjectKey, domainClass, text);
    }
}
