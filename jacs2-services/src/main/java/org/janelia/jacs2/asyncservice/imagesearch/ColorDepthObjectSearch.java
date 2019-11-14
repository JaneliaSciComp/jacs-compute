package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.gui.cdmip.ColorDepthMask;
import org.janelia.model.domain.gui.cdmip.ColorDepthMaskResult;
import org.janelia.model.domain.gui.cdmip.ColorDepthMatch;
import org.janelia.model.domain.gui.cdmip.ColorDepthParameters;
import org.janelia.model.domain.gui.cdmip.ColorDepthResult;
import org.janelia.model.domain.gui.cdmip.ColorDepthSearch;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Wraps the ColorDepthFileSearch service with integration with the Workstation via the domain model.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("colorDepthObjectSearch")
public class ColorDepthObjectSearch extends AbstractServiceProcessor<Boolean> {

    private final int minNodes;
    private final int maxNodes;
    private final int maxResultsPerMask;

    static class IntegratedColorDepthSearchArgs extends ServiceArgs {
        @Parameter(names = "-searchId", description = "GUID of the ColorDepthSearch object to use", required = true)
        Long searchId;
        @Parameter(names = "-maskId", description = "GUID of the ColorDepthMask object to use. If this is empty, all listed masks are searched.", required = false)
        Long maskId;
    }

    private final WrappedServiceProcessor<ColorDepthFileSearch, List<File>> colorDepthFileSearch;
    private final LegacyDomainDao dao;

    @Inject
    ColorDepthObjectSearch(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           LegacyDomainDao dao,
                           @IntPropertyValue(name = "service.colorDepthSearch.minNodes", defaultValue = 1) Integer minNodes,
                           @IntPropertyValue(name = "service.colorDepthSearch.maxNodes", defaultValue = 8) Integer maxNodes,
                           @IntPropertyValue(name = "service.colorDepthSearch.maxResultsPerMask", defaultValue = 100) Integer maxResultsPerMask,
                           ColorDepthFileSearch colorDepthFileSearch,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.dao = dao;
        this.minNodes = minNodes;
        this.maxNodes = maxNodes;
        this.maxResultsPerMask = maxResultsPerMask;
        this.colorDepthFileSearch = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, colorDepthFileSearch);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(ColorDepthObjectSearch.class, new IntegratedColorDepthSearchArgs());
    }

    @Override
    public ServiceResultHandler<Boolean> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Boolean>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public Boolean getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleProcessorResult>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<Boolean>> process(JacsServiceData jacsServiceData) {
        IntegratedColorDepthSearchArgs args = getArgs(jacsServiceData);

        Stopwatch sparkAppWatch = Stopwatch.createStarted();
        logger.info("Executing ColorDepthSearch#{} with ColorDepthMask#{}", args.searchId, args.maskId);

        ColorDepthSearch search = dao.getDomainObject(jacsServiceData.getOwnerKey(),
                ColorDepthSearch.class, args.searchId);

        if (search==null) {
            throw new ComputationException(jacsServiceData, "ColorDepthSearch#"+args.searchId+" not found");
        }

        if (search.getLibraries().isEmpty()) {
            throw new ComputationException(jacsServiceData, "ColorDepthSearch#"+args.searchId+" has no libraries defined");
        }

        List<ColorDepthMask> masks = dao.getDomainObjectsAs(search.getMasks(), ColorDepthMask.class);

        if (masks.isEmpty()) {
            throw new ComputationException(jacsServiceData, "ColorDepthSearch#"+args.searchId+" has no masks defined");
        }

        ColorDepthParameters searchParameters = search.getParameters();
        if (args.maskId != null) {
            // Filter down to just the selected mask
            masks = masks.stream().filter(m -> m.getId().equals(args.maskId)).collect(Collectors.toList());
            // Update search parameters which are saved into the result
            searchParameters.setMasks(masks.stream().map(Reference::createFor).collect(Collectors.toList()));
        }

        Map<String, ColorDepthMask> maskMap =
                masks.stream().collect(Collectors.toMap(ColorDepthMask::getFilepath,
                        Function.identity()));

        String inputFiles = masks.stream()
                .map(ColorDepthMask::getFilepath)
                .reduce((p1, p2) -> p1 + "," + p2)
                .orElse("");

        List<String> maskThresholds = masks.stream()
                .map(ColorDepthMask::getMaskThreshold)
                .map(Object::toString)
                .collect(Collectors.toList());

        List<String> pathsToSearch = new ArrayList<>();

        int totalFileCount = 0;
        for (String libraryIdentifier : search.getLibraries()) {
            List<String> colorDepthPaths = dao.getColorDepthPaths(jacsServiceData.getOwnerKey(), libraryIdentifier, search.getAlignmentSpace());
            pathsToSearch.addAll(colorDepthPaths);
            totalFileCount += colorDepthPaths.size();
        }
        logger.info("Searching {} libraries with {} total images", search.getLibraries().size(), totalFileCount);

        // Curve fitting using https://www.desmos.com/calculator
        // This equation was found using https://mycurvefit.com
        int desiredNodes = (int)Math.round(0.2 * Math.pow(totalFileCount, 0.32));

        int numNodes = Math.max(Math.min(desiredNodes, maxNodes), minNodes);
        int filesPerNode = (int)Math.round((double)totalFileCount/(double)numNodes);
        logger.info("Using {} worker nodes, with {} files per node", numNodes, filesPerNode);

        // Create temporary file with paths to search
        JacsServiceFolder workingDirectory = getWorkingDirectory(jacsServiceData);
        File colorDepthPaths = workingDirectory.getServiceFolder().resolve("colorDepthPaths.txt").toFile();
        try {
            FileUtils.writeLines(colorDepthPaths, pathsToSearch);
        }
        catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }

        List<ServiceArg> serviceArgList = new ArrayList<>();
        serviceArgList.add(new ServiceArg("-inputFiles", inputFiles));
        serviceArgList.add(new ServiceArg("-searchDirs", colorDepthPaths.getAbsolutePath()));
        serviceArgList.add(new ServiceArg("-maskThresholds", maskThresholds));
        serviceArgList.add(new ServiceArg("-numNodes", numNodes));

        if (search.getDataThreshold() != null) {
            serviceArgList.add(new ServiceArg("-dataThreshold", search.getDataThreshold()));
        }

        if (search.getPixColorFluctuation() != null) {
            serviceArgList.add(new ServiceArg("-pixColorFluctuation", search.getPixColorFluctuation()));
        }

        if (search.getXyShift() != null) {
            serviceArgList.add(new ServiceArg("-xyShift", search.getXyShift()));
        }

        if (search.getMirrorMask() != null && search.getMirrorMask()) {
            serviceArgList.add(new ServiceArg("-mirrorMask"));
        }

        // KR: ignoring this parameter, per Hideo's suggestion, and just returning top 200 results
//        if (search.getPctPositivePixels() != null) {
//            serviceArgList.add(new ServiceArg("-pctPositivePixels", search.getPctPositivePixels()));
//        }

        return colorDepthFileSearch.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                    .description("Color depth search")
                    .build(),
                serviceArgList.toArray(new ServiceArg[serviceArgList.size()]))
            .thenApply((JacsServiceResult<List<File>> result) -> {

                if (result.getResult().isEmpty()) {
                    throw new ComputationException(jacsServiceData, "Color depth search encountered an error");
                }

                try {
                    ColorDepthResult colorDepthResult = new ColorDepthResult();
                    colorDepthResult.setParameters(searchParameters);

                    for (File resultsFile : result.getResult()) {

                        logger.info("Processing result file: {}", resultsFile);

                        String maskFile;
                        try (Scanner scanner = new Scanner(resultsFile)) {
                            maskFile = scanner.nextLine();

                            ColorDepthMask colorDepthMask = maskMap.get(maskFile);
                            if (colorDepthMask==null) {
                                throw new IllegalStateException("Unrecognized mask path: "+maskFile);
                            }

                            ColorDepthMaskResult maskResult = new ColorDepthMaskResult();
                            maskResult.setMaskRef(Reference.createFor(colorDepthMask));

                            int i = 0;
                            while (scanner.hasNext()) {
                                String line = scanner.nextLine();
                                String[] s = line.split("\t");
                                int c = 0;
                                int score = Integer.parseInt(s[c++].trim());
                                double scorePct = Double.parseDouble(s[c++].trim());
                                String filepath = s[c].trim();

                                ColorDepthImage colorDepthImageByPath = dao.getColorDepthImageByPath(jacsServiceData.getOwnerKey(), filepath);
                                if (colorDepthImageByPath==null) {
                                    throw new IllegalStateException("Could not find result file in database:"+ filepath);
                                }
                                else {
                                    ColorDepthMatch match = new ColorDepthMatch();
                                    match.setImageRef(Reference.createFor(colorDepthImageByPath));
                                    match.setScore(score);
                                    match.setScorePercent(scorePct);
                                    maskResult.addMatch(match);
                                }

                                if (++i>=maxResultsPerMask) {
                                    logger.warn("Too many results returned, truncating at {}", maxResultsPerMask);
                                    break;
                                }
                            }

                            colorDepthResult.getMaskResults().add(maskResult);
                        }
                        catch (IOException e) {
                            throw new ComputationException(jacsServiceData, e);
                        }
                    }

                    colorDepthResult = dao.save(jacsServiceData.getOwnerKey(), colorDepthResult);
                    logger.info("Saved {}", colorDepthResult);
                    dao.addColorDepthSearchResult(jacsServiceData.getOwnerKey(), search.getId(), colorDepthResult);
                    logger.info("Updated ColorDepthSearch#{} with new result", search.getId());

                    long seconds = sparkAppWatch.stop().elapsed().getSeconds();
                    logger.info("ColorDepthSearch#{} completed after {} seconds", search.getId(), seconds);
                }
                catch (Exception e) {
                    throw new ComputationException(jacsServiceData, e);
                }

                return new JacsServiceResult<>(jacsServiceData, Boolean.TRUE);
        });
    }

    private IntegratedColorDepthSearchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new IntegratedColorDepthSearchArgs());
    }
}
