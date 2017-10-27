package org.janelia.model.access.dao.jdbc;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.Sage;
import org.janelia.model.jacs2.dao.SageDao;
import org.janelia.model.jacs2.sage.ControlledVocabulary;
import org.janelia.model.jacs2.sage.SlideImage;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class SageJdbcDao implements SageDao {

    private DataSource dataSource;
    private Logger logger;

    @Inject
    public SageJdbcDao(@Sage DataSource dataSource, Logger logger) {
        this.dataSource = dataSource;
        this.logger = logger;
    }

    @Override
    public List<SlideImage> findMatchingSlideImages(String dataset, String line, List<String> slideCodes, List<String> lsmNames, int offset, int length) {
        List<SlideImage> slideImages = new ArrayList<>();
        if (StringUtils.isBlank(dataset) &&
                StringUtils.isBlank(line) &&
                CollectionUtils.isEmpty(slideCodes) &&
                CollectionUtils.isEmpty(lsmNames)) {
            throw new IllegalArgumentException("Exhaustive search is not allowed - at least one filtering parameter must be specied");
        }
        // for line it only retrieves "line" and "light_imagery" CVs and for image it retrieves "fly" and "light_imagery"
        List<ControlledVocabulary> lineCvs = findAllUsedCVTerms(ImmutableList.of("line", "light_imagery"), this::createLineCVTermsQuery);
        List<ControlledVocabulary> imageCvs = findAllUsedCVTerms(ImmutableList.of("fly", "light_imagery"), this::createImageCVTermsQuery);

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            StringBuilder queryBuilder = new StringBuilder();
            ImmutableList.Builder<String> fieldListBuilder = ImmutableList.builder();
            fieldListBuilder
                    .addAll(IMAGE_ATTR)
                    .addAll(LINE_ATTR)
                    .add("lab_term.name as lab_value")
                    .add("ds_ip.value as ds_value")
                    .add("sc_ip.value as sc_value")
                    .add("area_ip.value as area_value")
                    .add("objective_ip.value as objective_value")
                    .add("tile_ip.value as tile_value")
            ;

            ImmutableList.Builder<Integer> cvFieldValuesBuilder = ImmutableList.builder();

            Function<ControlledVocabulary, String> lnTermFieldNameGenerator = cv -> "lp_" + cv.getVocabularyName() + "_" + cv.getVocabularyTerm();
            lineCvs.forEach(cv -> {
                fieldListBuilder.add("max" + "(" + "IF(lp.type_id = ?, lp.value, null)" + ") " + lnTermFieldNameGenerator.apply(cv) + ' ');
                cvFieldValuesBuilder.add(cv.getTermId());
            });

            Function<ControlledVocabulary, String> imTermFieldNameGenerator = cv -> "ip_" + cv.getVocabularyName() + "_" + cv.getVocabularyTerm();
            imageCvs.forEach(cv -> {
                fieldListBuilder.add("max" + "(" + "IF(ip.type_id = ?, ip.value, null)" + ") " + imTermFieldNameGenerator.apply(cv) + ' ');
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
                    .append("join cv_term lab_term on ln.lab_id = lab_term.id ")
                    .append(imagePropertyJoin("ds", "data_set"))
                    .append(imagePropertyJoin("sc", "slide_code"))
                    .append(imagePropertyJoin("area", "area"))
                    .append(imagePropertyJoin("objective", "objective"))
                    .append(imagePropertyJoin("tile", "tile"));

            ImmutableList.Builder<String> whereBuilder = ImmutableList.builder();
            if (StringUtils.isNotBlank(dataset)) {
                whereBuilder.add("ds_ip.value = ?");
            }
            if (StringUtils.isNotBlank(line)) {
                whereBuilder.add("ln.name = ?");
            }
            if (CollectionUtils.isNotEmpty(slideCodes)) {
                whereBuilder.add("sc_ip.value in (" + Joiner.on(',').join(Collections.nCopies(slideCodes.size(), '?')) + ")");
            }
            List<String> fullLsmNames = new ArrayList<>();
            List<String> simpleLsmNames = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(lsmNames)) {
                lsmNames.stream()
                        .filter(lsmName -> StringUtils.isNotBlank(lsmName))
                        .forEach(lsmName -> {
                            int pathSeparatorIndex = lsmName.indexOf('/');
                            if (pathSeparatorIndex > 0) {
                                fullLsmNames.add(lsmName);
                            } else if (pathSeparatorIndex == 0 && lsmName.length() > 1) {
                                simpleLsmNames.add(lsmName.substring(0));
                            } else if (pathSeparatorIndex == -1) {
                                simpleLsmNames.add(lsmName);
                            }
                        });
            }
            if (CollectionUtils.isNotEmpty(fullLsmNames)) {
                whereBuilder.add("im.name in (" + Joiner.on(',').join(Collections.nCopies(fullLsmNames.size(), '?')) + ")");
            }
            if (CollectionUtils.isNotEmpty(simpleLsmNames)) {
                String orSimpleLsmNameCond = simpleLsmNames.stream()
                        .map(lsmName -> "im.name like ?")
                        .reduce("", (c1, c2) -> StringUtils.isBlank(c1) ? c2 : c1 + " or " + c2);
                whereBuilder.add("(" + orSimpleLsmNameCond + ")");
            }
            queryBuilder
                    .append("where ")
                    .append(String.join(" and " , whereBuilder.build()))
                    .append(' ')
                    .append("group by ln.id, im.id ");
            if (offset > 0 && length > 0) {
                queryBuilder.append("limit ? offset ?");
            } else if (length > 0) {
                queryBuilder.append("limit ?");
            }

            String queryString = queryBuilder.toString();

            pstmt = conn.prepareStatement(queryString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            int fieldIndex = 1;
            for (Integer cvFieldValue : cvFieldValues) {
                pstmt.setInt(fieldIndex++, cvFieldValue);
            }
            if (StringUtils.isNotBlank(dataset)) {
                pstmt.setString(fieldIndex++, dataset);
            }
            if (StringUtils.isNotBlank(line)) {
                pstmt.setString(fieldIndex++, line);
            }
            if (CollectionUtils.isNotEmpty(slideCodes)) {
                for (String sc : slideCodes) pstmt.setString(fieldIndex++, sc);
            }
            if (CollectionUtils.isNotEmpty(fullLsmNames)) {
                for (String lsmName : fullLsmNames) pstmt.setString(fieldIndex++, lsmName);
            }
            if (CollectionUtils.isNotEmpty(simpleLsmNames)) {
                for (String lsmName : simpleLsmNames) pstmt.setString(fieldIndex++, "%" + lsmName);
            }

            if (offset > 0 && length > 0) {
                pstmt.setInt(fieldIndex++, length);
                pstmt.setInt(fieldIndex++, offset);
            } else if (length > 0) {
                pstmt.setInt(fieldIndex++, length);
            }

            logger.debug("Slide image query: {}, {}, {}", queryString, lineCvs, imageCvs);

            rs = pstmt.executeQuery();

            if (offset > 0 && length <= 0) {
                rs.absolute(offset);
            }

            while (rs.next()) {
                SlideImage si = extractSlideImage(rs);
                populateProperties(lineCvs, rs, lnTermFieldNameGenerator, si);
                populateProperties(imageCvs, rs, imTermFieldNameGenerator, si);
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
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception ignore) {
                }
            }
        }
        return slideImages;
    }

    private String createImageCVTermsQuery(List<String> cvNames) {
        return "select distinct "
                + "cv.id as cv_id,"
                + "cv.name as cv_name,"
                + "cv_term.id cv_term_id,"
                + "cv_term.name cv_term_name "
                + "from cv_term "
                + "join cv on cv.id = cv_term.cv_id "
                + "join image_property ip on ip.type_id = cv_term.id "
                + "where cv.name in "
                + "("
                + Joiner.on(',').join(Collections.nCopies(cvNames.size(), '?'))
                + ")";
    }

    private String imagePropertyJoin(String prefix, String term) {
        return String.format("join image_property %1$s_ip on %1$s_ip.image_id = im.id " +
                "join cv_term %1$s_cvterm on %1$s_cvterm.id = %1$s_ip.type_id and %1$s_cvterm.name = '%2$s' " +
                "join cv %1$s_cv on %1$s_cv.id = %1$s_cvterm.cv_id and %1$s_cv.name = 'light_imagery' ", prefix, term);
    }

    private String createLineCVTermsQuery(List<String> cvNames) {
        return "select distinct "
                + "cv.id as cv_id,"
                + "cv.name as cv_name,"
                + "cv_term.id cv_term_id,"
                + "cv_term.name cv_term_name "
                + "from cv_term "
                + "join cv on cv.id = cv_term.cv_id "
                + "join line_property lp on lp.type_id = cv_term.id "
                + "where cv.name in "
                + "("
                + Joiner.on(',').join(Collections.nCopies(cvNames.size(), '?'))
                + ")";
    }

    private SlideImage extractSlideImage(ResultSet rs) throws SQLException {
        SlideImage si = new SlideImage();
        si.setId(rs.getInt("im_id"));
        si.setName(rs.getString("im_name"));
        si.setPath(rs.getString("im_path"));
        si.setUrl(rs.getString("im_url"));
        si.setJfsPath(rs.getString("im_jfs_path"));
        si.setCaptureDate(rs.getTimestamp("im_capture_date"));
        si.setCreatedBy(rs.getString("im_created_by"));
        si.setCreateDate(rs.getTimestamp("im_create_date"));
        si.setLab(rs.getString("lab_value"));
        si.setDataset(rs.getString("ds_value"));
        si.setLineName(rs.getString("ln_name"));
        si.setSlideCode(rs.getString("sc_value"));
        si.setArea(rs.getString("area_value"));
        si.setObjectiveName(rs.getString("objective_value"));
        si.setTile(rs.getString("tile_value"));
        return si;
    }

    private void populateProperties(List<ControlledVocabulary> cvs, ResultSet rs, Function<ControlledVocabulary, String> lnTermFieldNameGenerator, SlideImage si)
            throws SQLException {
        cvs.forEach(cv -> {
            String fieldAlias = lnTermFieldNameGenerator.apply(cv);
            String fieldKey = cv.getVocabularyName() + "_" + cv.getVocabularyTerm();
            try {
                si.addProperty(fieldKey, rs.getString(fieldAlias));
            } catch (Exception e) {
                logger.error("Error reading field: {}", fieldAlias, e);
                throw new IllegalStateException("Failure reading " + fieldAlias, e);
            }
        });
    }

    private List<ControlledVocabulary> findAllUsedCVTerms(List<String> cvNames, Function<List<String>, String> queryBuilder) {
        List<ControlledVocabulary> cvs = new ArrayList<>();

        if (CollectionUtils.isEmpty(cvNames)) return cvs; // if no vocabulary name is specified don't do an exhaustive search - simply return empty

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();

            pstmt = conn.prepareStatement(queryBuilder.apply(cvNames));
            int fieldIndex = 1;
            for (String cvName : cvNames) pstmt.setString(fieldIndex++, cvName);
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
            if (conn != null) {
                try {
                    conn.close();
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

}
