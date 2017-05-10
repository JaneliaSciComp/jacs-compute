package org.janelia.jacs2.asyncservice.dataimport;

import com.beust.jcommander.Parameter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.sample.DataSet;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.dataset.DatasetService;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SageDataService;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.SampleUtils;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;
import org.janelia.jacs2.model.sage.ImageLine;
import org.janelia.jacs2.model.sage.SlideImage;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Named("lsmImport")
public class LSMImportProcessor extends AbstractBasicLifeCycleServiceProcessor<List<LSMImportProcessor.SageLoaderResult>, Void> {

    static class LSMImportArgs extends ServiceArgs {
        @Parameter(names = "-dataset", description = "Data set name or identifier", required = false)
        String dataset;
        @Parameter(names = "-imageLine", description = "Image line name", required = false)
        String imageLine;
        @Parameter(names = "-slideCodes", description = "Slide codes", required = false)
        List<String> slideCodes;
        @Parameter(names = "-lsmNames", description = "LSM names", required = false)
        List<String> lsmNames;
    }

    static class SageLoaderResult {

        private final Number sageLoaderService;
        private final String lab;
        private final DataSet dataSet;
        private final String imageLine;
        private final List<SlideImage> slideImages;

        SageLoaderResult(Number sageLoaderService, String lab, DataSet dataSet, String imageLine, List<SlideImage> slideImages) {
            this.sageLoaderService = sageLoaderService;
            this.lab = lab;
            this.dataSet = dataSet;
            this.imageLine = imageLine;
            this.slideImages = slideImages;
        }
    }

    private final SageLoaderProcessor sageLoaderProcessor;
    private final SampleDataService sampleDataService;
    private final DatasetService datasetService;
    private final SageDataService sageDataService;

    @Inject
    LSMImportProcessor(ServiceComputationFactory computationFactory,
                       JacsServiceDataPersistence jacsServiceDataPersistence,
                       @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                       SageLoaderProcessor sageLoaderProcessor,
                       SampleDataService sampleDataService,
                       DatasetService datasetService,
                       SageDataService sageDataService,
                       Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sageLoaderProcessor = sageLoaderProcessor;
        this.sampleDataService = sampleDataService;
        this.datasetService = datasetService;
        this.sageDataService = sageDataService;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(this.getClass(), new LSMImportArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Void>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public Void collectResult(JacsServiceResult<?> depResults) {
                return null;
            }

            @Override
            public Void getServiceDataResult(JacsServiceData jacsServiceData) {
                return null;
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        LSMImportArgs args = getArgs(jacsServiceData);
        if (StringUtils.isBlank(args.dataset) &&
                StringUtils.isBlank(args.imageLine) &&
                CollectionUtils.isEmpty(args.slideCodes) &&
                CollectionUtils.isEmpty(args.lsmNames)) {
            throw new IllegalArgumentException("No filtering parameter has been specified for the LSM import from Sage.");
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected JacsServiceResult<List<SageLoaderResult>> submitServiceDependencies(JacsServiceData jacsServiceData) {
        LSMImportArgs args = getArgs(jacsServiceData);
        List<SlideImage> slideImages = retrieveSageImages(jacsServiceData.getOwner(), args);
        Map<ImageLine, List<SlideImage>> labImages =
                slideImages.stream().collect(Collectors.groupingBy(
                        si -> new ImageLine(si.getLab(), si.getDataset(), si.getLineName()),
                        Collectors.mapping(Function.identity(), Collectors.toList()))
                );
        List<SageLoaderResult> sageLoaderResults = labImages.entrySet().stream()
                .map(lineEntries -> {
                    ImageLine imageLine = lineEntries.getKey();
                    List<String> slideImageNames = lineEntries.getValue().stream().map(SlideImage::getName).collect(Collectors.toList());
                    DataSet ds = datasetService.getDatasetByNameOrIdentifier(jacsServiceData.getOwner(), imageLine.getDataset());
                    if (ds == null) {
                        logger.error("No dataset record found for {}", lineEntries.getKey());
                        return null;
                    }
                    JacsServiceData sageLoaderService = sageLoaderProcessor.createServiceData(new ServiceExecutionContext(jacsServiceData),
                            new ServiceArg("-lab", imageLine.getLab()),
                            new ServiceArg("-line", imageLine.getLab()),
                            new ServiceArg("-configFile", ds.getSageConfigPath()),
                            new ServiceArg("-grammarFile", ds.getSageGrammarPath()),
                            new ServiceArg("-sampleFiles", String.join(",", slideImageNames))
                    );
                    sageLoaderService = submitDependencyIfNotPresent(jacsServiceData, sageLoaderService);
                    return new SageLoaderResult(sageLoaderService.getId(), imageLine.getLab(), ds, imageLine.getName(), lineEntries.getValue());
                })
                .filter(r -> r != null)
                .collect(Collectors.toList());

        return new JacsServiceResult<>(jacsServiceData, sageLoaderResults);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<List<SageLoaderResult>>> processing(JacsServiceResult<List<SageLoaderResult>> depResults) {
        JacsServiceData jacsServiceData = depResults.getJacsServiceData();
        LSMImportArgs args = getArgs(jacsServiceData);
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    List<LSMImage> lsmImages = pd.getResult().stream()
                            .flatMap(sageLoaderResult -> sageLoaderResult.slideImages.stream())
                            .map(SampleUtils::createLSMFromSlideImage)
                            .map(lsm -> importLsm(jacsServiceData.getOwner(), lsm))
                            .collect(Collectors.toList())
                            ;
                    groupLsmsIntoSamples(lsmImages);
                    // find if the samples exist and create them or update them.
                    // TODO
                    return pd;
                });
    }

    private LSMImportArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new LSMImportArgs());
    }

    private List<SlideImage> retrieveSageImages(String subject, LSMImportArgs args) {
        String datasetIdentifer = null;
        if (StringUtils.isNotBlank(args.dataset)) {
            DataSet ds = datasetService.getDatasetByNameOrIdentifier(subject, args.dataset);
            if (ds == null) {
                throw new IllegalArgumentException ("Invalid dataset: " + args.dataset);
            }
            datasetIdentifer = ds.getIdentifier();
        }
        return sageDataService.getMatchingImages(datasetIdentifer, args.imageLine, args.slideCodes, args.lsmNames, new PageRequest());
    }

    private LSMImage importLsm(String owner, LSMImage lsmImage) {
        PageResult<LSMImage> matchingLsms = sampleDataService.searchLsms(owner, lsmImage, new PageRequest());
        if (matchingLsms.isEmpty()) {
            sampleDataService.createLSM(lsmImage);
            return lsmImage;
        } else if (matchingLsms.getResultList().size() == 1) {
            LSMImage existingLsm = matchingLsms.getResultList().get(0);
            SampleUtils.updateLsmAttributes(lsmImage, existingLsm);
            sampleDataService.updateLSM(existingLsm);
            return existingLsm;
        } else {
            // there is a potential clash or duplication here
            logger.warn("Multiple candidates found for {}", lsmImage);
            LSMImage existingLsm = matchingLsms.getResultList().get(0); // FIXME this probably needs to be changed
            SampleUtils.updateLsmAttributes(lsmImage, existingLsm);
            sampleDataService.updateLSM(existingLsm);
            return existingLsm;
        }
    }

    private List<Sample> groupLsmsIntoSamples(List<LSMImage> lsmImages) {
        List<Sample> samples = new ArrayList<>();
        // TODO
        return samples;
    }
}
