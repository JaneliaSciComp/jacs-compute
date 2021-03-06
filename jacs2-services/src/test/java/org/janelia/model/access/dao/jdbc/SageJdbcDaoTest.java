package org.janelia.model.access.dao.jdbc;

import com.google.common.collect.ImmutableList;
import org.janelia.model.jacs2.sage.ControlledVocabulary;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SageJdbcDaoTest {

    private Connection testConnection;
    private PreparedStatement testPstmt;
    private ResultSet testRs;

    private SageJdbcDao testDao;

    @Before
    public void setUp() throws SQLException {
        DataSource testDs = mock(DataSource.class);
        Logger testLogger = mock(Logger.class);
        testConnection = mock(Connection.class);
        testPstmt = mock(PreparedStatement.class);
        testRs = mock(ResultSet.class);

        when(testDs.getConnection()).thenReturn(testConnection);
        when(testConnection.prepareStatement(anyString())).thenReturn(testPstmt);
        when(testPstmt.executeQuery()).thenReturn(testRs);
        testDao = new SageJdbcDao(testDs, testLogger);
    }

    @Test
    public void querySlideImagesWithAllFilters() throws SQLException {
        reset(testConnection);

        PreparedStatement lineCvPstmt = mock(PreparedStatement.class);
        PreparedStatement imCvPstmt = mock(PreparedStatement.class);
        ResultSet lineCvRs = mock(ResultSet.class);
        ResultSet imCvRs = mock(ResultSet.class);

        when(testConnection.prepareStatement(anyString()))
                .thenReturn(lineCvPstmt)
                .thenReturn(imCvPstmt);
        when(testConnection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(testPstmt);

        when(lineCvPstmt.executeQuery()).thenReturn(lineCvRs);
        when(imCvPstmt.executeQuery()).thenReturn(imCvRs);

        prepareCvRs(lineCvRs, ImmutableList.of(
                        createCV(2, "light_imagery", 6053, "vt_line"),
                        createCV(5, "line", 1728, "genotype"),
                        createCV(5, "line", 3676, "chromosome"),
                        createCV(5, "line", 4979, "flycore_permission")
                )
        );
        prepareCvRs(imCvRs, ImmutableList.of(
                        createCV(2, "light_imagery", 6045, "imaging_project"),
                        createCV(2, "light_imagery", 5766, "data_set"),
                        createCV(58, "fly", 3537, "cross_barcode"),
                        createCV(2, "light_imagery", 39, "renamed_by"),
                        createCV(2, "light_imagery", 5762, "microscope_filename"),
                        createCV(2, "light_imagery", 3526, "tissue_orientation"),
                        createCV(2, "light_imagery", 5012, "annotated_by"),
                        createCV(2, "light_imagery", 7, "area"),
                        createCV(2, "light_imagery", 1648, "mounting_protocol"),
                        createCV(2, "light_imagery", 6, "age"),
                        createCV(2, "light_imagery", 25, "gender"),
                        createCV(2, "light_imagery", 5868, "lsm_illumination_channel_1_power_bc"),
                        createCV(2, "light_imagery", 5866, "lsm_illumination_channel_2_name"),
                        createCV(2, "light_imagery", 5865, "lsm_illumination_channel_1_name"),
                        createCV(2, "light_imagery", 5871, "lsm_detection_channel_1_detector_ga"),
                        createCV(2, "light_imagery", 5051, "channel_spec"),
                        createCV(2, "light_imagery", 33, "objective"),
                        createCV(2, "light_imagery", 1627, "hostname"),
                        createCV(2, "light_imagery", 3668, "tile"),
                        createCV(2, "light_imagery", 6004, "screen_state"),
                        createCV(58, "fly", 1704, "effector"),
                        createCV(2, "light_imagery", 3654, "slide_code")
                )
        );
        testDao.findMatchingSlideImages("wangk11_kw_mcfo_images",
                "JRC_SS46543",
                ImmutableList.of("20170427_21_E6", "20170427_21_E7"),
                ImmutableList.of(
                        "20170501/FLFL_20170503155545197_286220.lsm",
                        "20170501/FLFL_20170503155558312_286224.lsm",
                        "20170501/FLFL_20170503155511097_286213.lsm",
                        "FLFL_20170503155511097_286214.lsm"), 1, 2);
        // verify line property CVs
        verify(testConnection).prepareStatement(
                "select distinct cv.id as cv_id,cv.name as cv_name,cv_term.id cv_term_id,cv_term.name cv_term_name " +
                        "from cv_term join cv on cv.id = cv_term.cv_id join line_property lp on lp.type_id = cv_term.id where cv.name in (?,?)"
        );
        verify(lineCvPstmt).setString(1, "line");
        verify(lineCvPstmt).setString(2, "light_imagery");
        verify(lineCvPstmt).close();
        verify(lineCvRs).close();
        verify(testConnection).prepareStatement(
                "select distinct cv.id as cv_id,cv.name as cv_name,cv_term.id cv_term_id,cv_term.name cv_term_name " +
                        "from cv_term join cv on cv.id = cv_term.cv_id join image_property ip on ip.type_id = cv_term.id where cv.name in (?,?)"
        );
        verify(imCvPstmt).setString(1, "fly");
        verify(imCvPstmt).setString(2, "light_imagery");
        verify(imCvPstmt).close();
        verify(imCvRs).close();

        verify(testConnection).prepareStatement("select " +
                        "im.id im_id,im.name im_name,im.url im_url,im.path im_path,im.jfs_path im_jfs_path,im.line_id im_line_id,im.family_id im_family_id," +
                        "im.capture_date im_capture_date,im.representative im_representative,im.created_by im_created_by,im.create_date im_create_date,ln.id ln_id," +
                        "ln.name ln_name,ln.lab_id ln_lab_id,ln.gene_id ln_gene_id,ln.organism_id ln_organism_id,ln.genotype ln_genotype," +
                        "lab_term.name as lab_value," +
                        "ds_ip.value as ds_value," +
                        "sc_ip.value as sc_value," +
                        "area_ip.value as area_value," +
                        "objective_ip.value as objective_value," +
                        "tile_ip.value as tile_value," +
                        "max(IF(lp.type_id = ?, lp.value, null)) lp_light_imagery_vt_line ," +
                        "max(IF(lp.type_id = ?, lp.value, null)) lp_line_genotype ," +
                        "max(IF(lp.type_id = ?, lp.value, null)) lp_line_chromosome ," +
                        "max(IF(lp.type_id = ?, lp.value, null)) lp_line_flycore_permission ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_imaging_project ,max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_data_set ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_fly_cross_barcode ,max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_renamed_by ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_microscope_filename ,max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_tissue_orientation ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_annotated_by ,max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_area ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_mounting_protocol ,max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_age ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_gender ,max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_lsm_illumination_channel_1_power_bc ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_lsm_illumination_channel_2_name ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_lsm_illumination_channel_1_name ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_lsm_detection_channel_1_detector_ga ,max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_channel_spec ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_objective ,max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_hostname ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_tile ,max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_screen_state ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_fly_effector ,max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_slide_code  " +
                        "from image im " +
                        "join line ln on ln.id = im.line_id " +
                        "join line_property lp on lp.line_id = im.line_id " +
                        "join image_property ip on ip.image_id = im.id " +
                        "join cv_term lab_term on ln.lab_id = lab_term.id " +
                        imagePropertyJoin("ds", "data_set") +
                        imagePropertyJoin("sc", "slide_code") +
                        imagePropertyJoin("area", "area") +
                        imagePropertyJoin("objective", "objective") +
                        imagePropertyJoin("tile", "tile") +
                        "where ds_ip.value = ? and ln.name = ? and sc_ip.value in (?,?) and im.name in (?,?,?) and (im.name like ?) " +
                        "group by ln.id, im.id " +
                        "limit ? offset ?",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        verify(testRs, never()).absolute(10);
    }

    @Test
    public void querySlideImagesByDatasetAndLine() throws SQLException {
        reset(testConnection);

        PreparedStatement lineCvPstmt = mock(PreparedStatement.class);
        PreparedStatement imCvPstmt = mock(PreparedStatement.class);
        ResultSet lineCvRs = mock(ResultSet.class);
        ResultSet imCvRs = mock(ResultSet.class);

        when(testConnection.prepareStatement(anyString()))
                .thenReturn(lineCvPstmt)
                .thenReturn(imCvPstmt);
        when(testConnection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(testPstmt);

        when(lineCvPstmt.executeQuery()).thenReturn(lineCvRs);
        when(imCvPstmt.executeQuery()).thenReturn(imCvRs);

        prepareCvRs(lineCvRs, ImmutableList.of(
                        createCV(2, "light_imagery", 6053, "vt_line")
                )
        );
        prepareCvRs(imCvRs, ImmutableList.of(
                        createCV(58, "fly", 3537, "cross_barcode"),
                        createCV(2, "light_imagery", 7, "area"),
                        createCV(2, "light_imagery", 33, "objective"),
                        createCV(2, "light_imagery", 3668, "tile"),
                        createCV(2, "light_imagery", 3654, "slide_code")
                )
        );
        testDao.findMatchingSlideImages("wangk11_kw_mcfo_images", "JRC_SS46543", null, null, 10, 0);
        verify(testConnection).prepareStatement("select " +
                        "im.id im_id,im.name im_name,im.url im_url,im.path im_path,im.jfs_path im_jfs_path,im.line_id im_line_id,im.family_id im_family_id," +
                        "im.capture_date im_capture_date,im.representative im_representative,im.created_by im_created_by,im.create_date im_create_date,ln.id ln_id," +
                        "ln.name ln_name,ln.lab_id ln_lab_id,ln.gene_id ln_gene_id,ln.organism_id ln_organism_id,ln.genotype ln_genotype," +
                        "lab_term.name as lab_value," +
                        "ds_ip.value as ds_value," +
                        "sc_ip.value as sc_value," +
                        "area_ip.value as area_value," +
                        "objective_ip.value as objective_value," +
                        "tile_ip.value as tile_value," +
                        "max(IF(lp.type_id = ?, lp.value, null)) lp_light_imagery_vt_line ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_fly_cross_barcode ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_area ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_objective ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_tile ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_slide_code  " +
                        "from image im " +
                        "join line ln on ln.id = im.line_id " +
                        "join line_property lp on lp.line_id = im.line_id " +
                        "join image_property ip on ip.image_id = im.id " +
                        "join cv_term lab_term on ln.lab_id = lab_term.id " +
                        imagePropertyJoin("ds", "data_set") +
                        imagePropertyJoin("sc", "slide_code") +
                        imagePropertyJoin("area", "area") +
                        imagePropertyJoin("objective", "objective") +
                        imagePropertyJoin("tile", "tile") +
                        "where ds_ip.value = ? and ln.name = ? " +
                        "group by ln.id, im.id ",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        verify(testRs).absolute(10);
    }

    private String imagePropertyJoin(String prefix, String term) {
        return String.format("join image_property %1$s_ip on %1$s_ip.image_id = im.id " +
                        "join cv_term %1$s_cvterm on %1$s_cvterm.id = %1$s_ip.type_id and %1$s_cvterm.name = '%2$s' " +
                        "join cv %1$s_cv on %1$s_cv.id = %1$s_cvterm.cv_id and %1$s_cv.name = 'light_imagery' ", prefix, term);
    }

    @Test
    public void querySlideImagesBySlideCodesAndLsmNames() throws SQLException {
        reset(testConnection);

        PreparedStatement lineCvPstmt = mock(PreparedStatement.class);
        PreparedStatement imCvPstmt = mock(PreparedStatement.class);
        ResultSet lineCvRs = mock(ResultSet.class);
        ResultSet imCvRs = mock(ResultSet.class);

        when(testConnection.prepareStatement(anyString()))
                .thenReturn(lineCvPstmt)
                .thenReturn(imCvPstmt);
        when(testConnection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(testPstmt);

        when(lineCvPstmt.executeQuery()).thenReturn(lineCvRs);
        when(imCvPstmt.executeQuery()).thenReturn(imCvRs);

        prepareCvRs(lineCvRs, ImmutableList.of(
                        createCV(2, "light_imagery", 6053, "vt_line")
                )
        );
        prepareCvRs(imCvRs, ImmutableList.of(
                        createCV(58, "fly", 3537, "cross_barcode"),
                        createCV(2, "light_imagery", 7, "area"),
                        createCV(2, "light_imagery", 33, "objective"),
                        createCV(2, "light_imagery", 3668, "tile"),
                        createCV(2, "light_imagery", 3654, "slide_code")
                )
        );
        testDao.findMatchingSlideImages(null,
                null,
                ImmutableList.of("20170427_21_E6", "20170427_21_E7"),
                ImmutableList.of(
                        "20170501/FLFL_20170503155545197_286220.lsm",
                        "20170501/FLFL_20170503155558312_286224.lsm",
                        "20170501/FLFL_20170503155511097_286213.lsm",
                        "FLFL_20170503155511097_286214.lsm",
                        "FLFL_20170503155511097_286215.lsm"), 0, 2);
        verify(testConnection).prepareStatement("select " +
                        "im.id im_id,im.name im_name,im.url im_url,im.path im_path,im.jfs_path im_jfs_path,im.line_id im_line_id,im.family_id im_family_id," +
                        "im.capture_date im_capture_date,im.representative im_representative,im.created_by im_created_by,im.create_date im_create_date,ln.id ln_id," +
                        "ln.name ln_name,ln.lab_id ln_lab_id,ln.gene_id ln_gene_id,ln.organism_id ln_organism_id,ln.genotype ln_genotype," +
                        "lab_term.name as lab_value," +
                        "ds_ip.value as ds_value," +
                        "sc_ip.value as sc_value," +
                        "area_ip.value as area_value," +
                        "objective_ip.value as objective_value," +
                        "tile_ip.value as tile_value," +
                        "max(IF(lp.type_id = ?, lp.value, null)) lp_light_imagery_vt_line ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_fly_cross_barcode ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_area ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_objective ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_tile ," +
                        "max(IF(ip.type_id = ?, ip.value, null)) ip_light_imagery_slide_code  " +
                        "from image im " +
                        "join line ln on ln.id = im.line_id " +
                        "join line_property lp on lp.line_id = im.line_id " +
                        "join image_property ip on ip.image_id = im.id " +
                        "join cv_term lab_term on ln.lab_id = lab_term.id " +
                        imagePropertyJoin("ds", "data_set") +
                        imagePropertyJoin("sc", "slide_code") +
                        imagePropertyJoin("area", "area") +
                        imagePropertyJoin("objective", "objective") +
                        imagePropertyJoin("tile", "tile") +
                        "where sc_ip.value in (?,?) and im.name in (?,?,?) and (im.name like ? or im.name like ?) " +
                        "group by ln.id, im.id " +
                        "limit ?",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        verify(testRs, never()).absolute(anyInt());
    }

    private void prepareCvRs(ResultSet rs, List<ControlledVocabulary> cvs) throws SQLException {
        boolean[] nextValues = new boolean[cvs.size() + 1];
        int[] cvIdValues = new int[cvs.size()];
        int[] termIdValues = new int[cvs.size()];
        String[] cvNameValues = new String[cvs.size()];
        String[] termValues = new String[cvs.size()];
        int cvIndex = 0;
        for (ControlledVocabulary cv : cvs) {
            nextValues[cvIndex] = true;
            cvIdValues[cvIndex] = cv.getVocabularyId();
            termIdValues[cvIndex] = cv.getTermId();
            cvNameValues[cvIndex] = cv.getVocabularyName();
            termValues[cvIndex] = cv.getVocabularyTerm();
            cvIndex++;
        }
        nextValues[cvIndex] = false;

        OngoingStubbing<Boolean> nextResults = when(rs.next()).thenReturn(nextValues[0]);
        for (int i = 1; i < nextValues.length; i++) nextResults = nextResults.thenReturn(nextValues[i]);

        OngoingStubbing<Integer> cvIdResults = when(rs.getInt("cv_id")).thenReturn(cvIdValues[0]);
        for (int i = 1; i < cvIdValues.length; i++) cvIdResults = cvIdResults.thenReturn(cvIdValues[i]);

        OngoingStubbing<Integer> termIdResults = when(rs.getInt("cv_term_id")).thenReturn(termIdValues[0]);
        for (int i = 1; i < termIdValues.length; i++) termIdResults = termIdResults.thenReturn(termIdValues[i]);

        OngoingStubbing<String> cvNameResults = when(rs.getString("cv_name")).thenReturn(cvNameValues[0]);
        for (int i = 1; i < cvNameValues.length; i++) cvNameResults = cvNameResults.thenReturn(cvNameValues[i]);

        OngoingStubbing<String> termResults = when(rs.getString("cv_term_name")).thenReturn(termValues[0]);
        for (int i = 1; i < termValues.length; i++) termResults = termResults.thenReturn(termValues[i]);
    }

    private ControlledVocabulary createCV(int cvId, String cvName, int termId, String term) {
        ControlledVocabulary cv = new ControlledVocabulary();
        cv.setVocabularyId(cvId);
        cv.setVocabularyName(cvName);
        cv.setTermId(termId);
        cv.setVocabularyTerm(term);
        return cv;
    }
}
