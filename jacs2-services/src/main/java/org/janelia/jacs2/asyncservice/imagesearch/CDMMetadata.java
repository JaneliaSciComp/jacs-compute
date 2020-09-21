package org.janelia.jacs2.asyncservice.imagesearch;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;

public class CDMMetadata {
    private String id;
    private String libraryName;
    private String alignmentSpace;
    private String cdmPath;
    private String imagePath;
    private String sampleRef;
    private String relatedImageRefId;
    @JsonProperty
    private Map<String, String> variants;

    public String getId() {
        return id;
    }

    public CDMMetadata setId(String id) {
        this.id = id;
        return this;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public CDMMetadata setLibraryName(String libraryName) {
        this.libraryName = libraryName;
        return this;
    }

    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public CDMMetadata setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
        return this;
    }

    public String getCdmPath() {
        return cdmPath;
    }

    public CDMMetadata setCdmPath(String cdmPath) {
        this.cdmPath = cdmPath;
        return this;
    }

    public String getImagePath() {
        return imagePath;
    }

    public CDMMetadata setImagePath(String imagePath) {
        this.imagePath = imagePath;
        return this;
    }

    public String getSampleRef() {
        return sampleRef;
    }

    public CDMMetadata setSampleRef(String sampleRef) {
        this.sampleRef = sampleRef;
        return this;
    }

    public String getRelatedImageRefId() {
        return relatedImageRefId;
    }

    public CDMMetadata setRelatedImageRefId(String relatedImageRefId) {
        this.relatedImageRefId = relatedImageRefId;
        return this;
    }

    public boolean hasVariant(String variant) {
        return variants != null && StringUtils.isNotBlank(variants.get(variant));
    }

    public void addVariant(String variant, String variantLocation) {
        if (StringUtils.isBlank(variantLocation)) {
            return;
        }
        if (StringUtils.isBlank(variant)) {
            throw new IllegalArgumentException("Variant type for " + variantLocation + " cannot be blank");
        }
        if (variants == null) {
            variants = new LinkedHashMap<>();
        }
        variants.put(variant, variantLocation);
    }

    CDMMetadata copyFrom(CDMMetadata other) {
        this.id = other.id;
        this.libraryName = other.libraryName;
        this.alignmentSpace = other.alignmentSpace;
        this.cdmPath = other.cdmPath;
        this.imagePath = other.imagePath;
        this.sampleRef = other.sampleRef;
        this.relatedImageRefId = other.relatedImageRefId;
        return this;
    }
}
