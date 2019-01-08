package org.janelia.jacs2.dataservice.search;

/**
 * Enumeration of the types of documents stored in Solr. This type is stored in the "doc_type" field.
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public enum SolrDocType {
	ENTITY,
	DOCUMENT,
	SAGE_TERM
}
