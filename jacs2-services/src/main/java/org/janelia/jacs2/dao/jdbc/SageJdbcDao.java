package org.janelia.jacs2.dao.jdbc;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.cdi.qualifier.Sage;
import org.janelia.jacs2.dao.SageDao;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;
import org.janelia.jacs2.model.sage.ControlledVocabulary;
import org.janelia.jacs2.model.sage.SlideImage;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SageJdbcDao implements SageDao {

    private Connection connection;

    @Inject
    public SageJdbcDao(@Sage Connection connection) {
        this.connection = connection;
    }

    public long countAll() {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = connection.prepareStatement("select count(1) from image");
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            } else {
                throw new IllegalStateException("Empty result set for count image query");
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ignore) {
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Exception ignore) {
                }
            }
        }
    }

    @Override
    public List<SlideImage> findSlideImagesByDatasetAndLsmNames(String dataset, List<String> lsmNames, int offset, int length) {
        List<SlideImage> slideImages = new ArrayList<>(lsmNames.size());

        List<ControlledVocabulary> lineCvs = findAllUsedLineVocabularyTerms(ImmutableList.of("line", "light_imagery"));
        List<ControlledVocabulary> dsCvs = findAllUsedDatasetVocabularyTerms(dataset, lsmNames);

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            StringBuilder queryBuilder = new StringBuilder();
            ImmutableList.Builder<String> fieldListBuilder = ImmutableList.builder();
            fieldListBuilder.add("im.name")
                    .addAll(IMAGE_ATTR)
                    .addAll(LINE_ATTR);
            ImmutableList.Builder<Integer> cvFieldValuesBuilder = ImmutableList.builder();
            lineCvs.forEach(cv -> {
                StringBuilder fieldNameBuilder = new StringBuilder();
                String fieldAlias = "ln_" + cv.getVocabularyName() + "_" + cv.getVocabularyTerm();
                fieldNameBuilder
                        .append("max")
                        .append("(")
                        .append("IF(lp.type_id = ?, lp.value, null)")
                        .append(") ")
                        .append(fieldAlias)
                        .append(' ');
                fieldListBuilder.add(fieldNameBuilder.toString());
                cvFieldValuesBuilder.add(cv.getTermId());
            });
            dsCvs.forEach(cv -> {
                StringBuilder fieldNameBuilder = new StringBuilder();
                String fieldAlias = "ip_" + cv.getVocabularyName() + "_" + cv.getVocabularyTerm();
                fieldNameBuilder
                        .append("max")
                        .append("(")
                        .append("IF(ip.type_id = ?, ip.value, null)")
                        .append(") ")
                        .append(fieldAlias)
                        .append(' ');
                fieldListBuilder.add(fieldNameBuilder.toString());
                cvFieldValuesBuilder.add(cv.getTermId());
            });

            List<String> fields = fieldListBuilder.build();
            List<Integer> cvFieldValues = cvFieldValuesBuilder.build();

            queryBuilder
                    .append("select ")
                    .append(Joiner.on(',').join(fields))
                    .append(' ')
                    .append("from image im ")
                    .append("join line ln on ln.id = im.line_id ")
                    .append("join line_property lp on lp.line_id = im.line_id ")
                    .append("join image_property ip on ip.image_id = im.id ")
                    .append("join ")
                    .append('(')
                    .append(ALL_IMAGE_IDS_FOR_DATASET_QUERY)
                    .append(')')
                    .append(' ')
                    .append("ds_ims on ds_ims.image_id = im.id ")
            ;
            if (CollectionUtils.isNotEmpty(lsmNames)) {
                queryBuilder
                        .append("where im.name in ")
                        .append('(')
                        .append(Joiner.on(',').join(Collections.nCopies(lsmNames.size(), '?')))
                        .append(')')
                        .append(' ');
            }
            queryBuilder.append("group by ln.id, im.id ");
            if (offset > 0 && length > 0) {
                queryBuilder.append("limit ? offset ?");
            } else if (length > 0) {
                queryBuilder.append("limit ?");
            }

            pstmt = connection.prepareStatement(queryBuilder.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            int fieldIndex = 1;
            for (Integer cvFieldValue : cvFieldValues) {
                pstmt.setInt(fieldIndex++, cvFieldValue);
            }
            pstmt.setString(fieldIndex++, dataset);
            for (String lsmName : lsmNames) pstmt.setString(fieldIndex++, lsmName);

            if (offset > 0 && length > 0) {
                pstmt.setInt(fieldIndex++, length);
                pstmt.setInt(fieldIndex++, offset);
            } else if (length > 0) {
                pstmt.setInt(fieldIndex++, length);
            }

            rs = pstmt.executeQuery();

            if (offset > 0 && length <= 0) {
                rs.absolute(offset);
            }

            while (rs.next()) {
                SlideImage si = new SlideImage();
                si.setId(rs.getInt("im_id"));
                si.setName(rs.getString("im_name"));
                si.setPath(rs.getString("im_path"));
                si.setUrl(rs.getString("im_url"));
                si.setJfsPath(rs.getString("im_jfs_path"));
                si.setCaptureDate(rs.getTimestamp("im_capture_date"));
                si.setCreatedBy(rs.getString("im_created_by"));
                si.setCreateDate(rs.getTimestamp("im_create_date"));
                si.setDataset(rs.getString("dsprop_value"));
                si.setLineName(rs.getString("ln_name"));
                slideImages.add(si);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ignore) {
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Exception ignore) {
                }
            }
        }
        return slideImages;
    }

    public List<ControlledVocabulary> findAllUsedDatasetVocabularyTerms (String dataset, List<String> lsmNames) {
        List<ControlledVocabulary> cvs = new ArrayList<>();

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("select distinct ")
                    .append("cv.id as cv_id,")
                    .append("cv.name as cv_name,")
                    .append("cv_term.id cv_term_id,")
                    .append("cv_term.name cv_term_name ")
                    .append("from cv_term ")
                    .append("join cv on cv.id = cv_term.cv_id ")
                    .append("join image_property ip on ip.type_id = cv_term.id ")
                    .append("join ")
                    .append('(')
                    .append(ALL_IMAGE_IDS_FOR_DATASET_QUERY)
                    .append(')')
                    .append(' ')
                    .append("ds_ims on ds_ims.image_id = ip.image_id ");
            if (CollectionUtils.isNotEmpty(lsmNames)) {
                queryBuilder
                        .append("join image im on im.id = ip.image_id ")
                        .append("where im.name in ")
                        .append('(')
                        .append(Joiner.on(',').join(Collections.nCopies(lsmNames.size(), '?')))
                        .append(')');
            }
            pstmt = connection.prepareStatement(queryBuilder.toString());
            int fieldIndex = 1;
            pstmt.setString(fieldIndex++, dataset);
            for (String lsmName : lsmNames) pstmt.setString(fieldIndex++, lsmName);

            rs = pstmt.executeQuery();
            while (rs.next()) {
                ControlledVocabulary cv = extractVocabularyTerm(rs);
                cvs.add(cv);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ignore) {
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Exception ignore) {
                }
            }
        }
        return cvs;
    }

    public List<ControlledVocabulary> findAllUsedLineVocabularyTerms(List<String> vocabularyNames) {
        List<ControlledVocabulary> cvs = new ArrayList<>();

        if (CollectionUtils.isEmpty(vocabularyNames)) return cvs;

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = connection.prepareStatement("select distinct "
                            + "cv.id as cv_id,"
                            + "cv.name as cv_name,"
                            + "cv_term.id cv_term_id,"
                            + "cv_term.name cv_term_name "
                            + "from cv_term "
                            + "join cv on cv.id = cv_term.cv_id "
                            + "join line_property lp on lp.type_id = cv_term.id "
                            + "where cv.name in "
                            + "("
                            + Joiner.on(',').join(Collections.nCopies(vocabularyNames.size(), '?'))
                            + ")"
            );
            int fieldIndex = 1;
            for (String cvName : vocabularyNames) pstmt.setString(fieldIndex++, cvName);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                ControlledVocabulary cv = extractVocabularyTerm(rs);
                cvs.add(cv);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ignore) {
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Exception ignore) {
                }
            }
        }
        return cvs;
    }

    private ControlledVocabulary extractVocabularyTerm(ResultSet rs) throws SQLException {
        ControlledVocabulary cv = new ControlledVocabulary();
        cv.setVocabularyId(rs.getInt("cv_id"));
        cv.setTermId(rs.getInt("cv_term_id"));
        cv.setVocabularyName(rs.getString("cv_name"));
        cv.setVocabularyTerm(rs.getString("cv_term_name"));
        return cv;
    }

    private static final List<String> IMAGE_ATTR =
            ImmutableList.<String>builder()
                    .add("im.id im_id", "im.name im_name", "im.url im_url", "im.path im_path")
                    .add("im.jfs_path im_jfs_path", "im.line_id im_line_id", "im.family_id im_family_id")
                    .add("im.capture_date im_capture_date", "im.representative im_representative")
                    .add("im.created_by im_created_by", "im.create_date im_create_date")
                    .build();

    private static final List<String> LINE_ATTR =
            ImmutableList.<String>builder()
                    .add("ln.id ln_id", "ln.name ln_name", "ln.lab_id ln_lab_id", "ln.gene_id ln_gene_id")
                    .add("ln.organism_id ln_organism_id", "ln.genotype ln_genotype")
                    .build();

    private static final String ALL_IMAGE_IDS_FOR_DATASET_QUERY =
            "select ip1.image_id image_id " +
            "from image_property ip1 " +
            "join cv_term ds_term on ds_term.id = ip1.type_id and ds_term.name = 'data_set' " +
            "where ip1.value = ?";

}
