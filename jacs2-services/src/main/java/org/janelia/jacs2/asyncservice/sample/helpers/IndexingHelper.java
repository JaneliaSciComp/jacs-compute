package org.janelia.jacs2.asyncservice.sample.helpers;

import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.workspace.Node;

 /**
 * Helper class for asynchronous indexing.
 *
 * TODO: reimplement this for JACSv2
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class IndexingHelper {
//
//	private static final Boolean ENABLE_INDEXING = SystemConfigurationProperties.getBoolean("Solr.EnableIndexing");
//	private static final String queueName = "queue/indexing";
//
//	private static Logger logger = Logger.getLogger(IndexingHelper.class);

	public static void sendReindexingMessage(DomainObject domainObject) {
//		if (!ENABLE_INDEXING) return;
//
//        if (domainObject.getClass().getAnnotation(SearchType.class)==null) {
//            logger.trace("Cannot index domain object without @SearchType annotation: "+domainObject);
//            return;
//        }
//
//		try {
//			logger.debug("Sending message for reindexing " + domainObject);
//			AsyncMessageInterface messageInterface = JmsUtil.createAsyncMessageInterface();
//			messageInterface.startMessageSession(queueName, messageInterface.localConnectionType);
//			ObjectMessage message = messageInterface.createObjectMessage();
//			message.setLongProperty("OBJECT_ID", domainObject.getId());
//			message.setStringProperty("OBJECT_CLASS", domainObject.getClass().getName());
//			message.setStringProperty("OPERATION", "UPDATE");
//			messageInterface.sendMessageWithinTransaction(message);
//			messageInterface.commit();
//			messageInterface.endMessageSession();
//		}
//		catch (Exception e) {
//			logger.error("Error sending reindexing message for "+domainObject.getId(),e);
//		}
	}

	public static void sendRemoveFromIndexMessage(DomainObject domainObject) {
//		if (!ENABLE_INDEXING) return;
//
//        if (domainObject.getClass().getAnnotation(SearchType.class)==null) {
//            logger.trace("Cannot deindex domain object without @SearchType annotation: "+domainObject);
//            return;
//        }
//
//		try {
//			logger.debug("Sending message for removing " + domainObject);
//			AsyncMessageInterface messageInterface = JmsUtil.createAsyncMessageInterface();
//			messageInterface.startMessageSession(queueName, messageInterface.localConnectionType);
//			ObjectMessage message = messageInterface.createObjectMessage();
//			message.setLongProperty("OBJECT_ID", domainObject.getId());
//			message.setStringProperty("OBJECT_CLASS", domainObject.getClass().getName());
//			message.setStringProperty("OPERATION", "REMOVE");
//			messageInterface.sendMessageWithinTransaction(message);
//			messageInterface.commit();
//			messageInterface.endMessageSession();
//		}
//		catch (Exception e) {
//			logger.error("Error sending remove from index message for "+ domainObject,e);
//		}
	}

	public static void sendAddAncestorMessage(DomainObject domainObject, Node newAncestor) {
//		if (!ENABLE_INDEXING) return;
//
//        if (domainObject.getClass().getAnnotation(SearchType.class)==null) {
//            logger.trace("Cannot index ancestor for domain object without @SearchType annotation: "+domainObject);
//            return;
//        }
//
//		try {
//			logger.debug("Sending message for add ancestor " + newAncestor + " to " + domainObject);
//			AsyncMessageInterface messageInterface = JmsUtil.createAsyncMessageInterface();
//			messageInterface.startMessageSession(queueName, messageInterface.localConnectionType);
//			ObjectMessage message = messageInterface.createObjectMessage();
//			message.setObjectProperty("OBJECT_ID", domainObject.getId());
//			message.setLongProperty("NEW_ANCESTOR_ID", newAncestor.getId());
//			message.setStringProperty("OPERATION", "ANCESTOR");
//			messageInterface.sendMessageWithinTransaction(message);
//        	messageInterface.commit();
//       		messageInterface.endMessageSession();
//		}
//		catch (Exception e) {
//			logger.error("Error sending add ancestor message for "+domainObject.getId(),e);
//		}
	}
}
