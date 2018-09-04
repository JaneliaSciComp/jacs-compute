package org.janelia.jacs2.asyncservice.sample;

import org.janelia.jacs2.asyncservice.sample.helpers.QuotaValidator;
import org.janelia.jacs2.asyncservice.utils.ContainerizedServiceUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.domain.DomainDAO;
import org.janelia.model.domain.compute.ContainerizedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

/**
 * A reference to a versioned Singularity container.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SingularityApp {

    private static final Logger log = LoggerFactory.getLogger(SingularityApp.class);

    @Inject
    @PropertyValue(name = "Singularity.Bin.Path")
    private String singularityBinPath;

    @Inject
    @PropertyValue(name = "Singularity.LocalImages.Path")
    private String containersPath;

    @Inject
    private DomainDAO domainDao;

    private String containerName;
    private String containerVersion;
    private String appName;
    private ContainerizedService container;

    public SingularityApp() {
    }

    public String getContainerName() {
        return containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public String getContainerVersion() {
        return containerVersion;
    }

    public void setContainerVersion(String containerVersion) {
        this.containerVersion = containerVersion;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public final void resolve() {

        List<ContainerizedService> containers = domainDao.getDomainObjectsByName(null, ContainerizedService.class, containerName);
        if (containers.isEmpty()) {
            throw new IllegalArgumentException("Cannot find container with name "+containerName);
        }

        if (containerVersion==null) {
            ContainerizedServiceUtils.sortByVersion(containers);
            this.container = containers.get(containers.size()-1); // latest version
            log.trace("Found latest version ({}) of {}", container.getVersion(), containerName);
        }
        else {
            this.container = ContainerizedServiceUtils.getContainer(containers, containerName, containerVersion);
            log.trace("Found version {} of {}", container.getVersion(), containerName);
            if (container==null) {
                throw new IllegalArgumentException("Cannot find container with name "+containerName+" and version "+containerVersion);
            }
        }
    }

    /**
     * Finds the container on disk and return the absolute path to it.
     * @return absolute path to the container
     * @throws FileNotFoundException
     */
    protected String getContainerFilepath() throws FileNotFoundException {
        String prefix = String.format("%s/%s-%s",
                containersPath, container.getName(), container.getVersion());
        File file1 = new File(prefix+".img");
        if (file1.exists()) {
            return file1.getAbsolutePath();
        }
        File file2 = new File(prefix+".simg");
        if (file2.exists()) {
            return file2.getAbsolutePath();
        }
        throw new FileNotFoundException("Could not find container at "+file1+" or "+file2.getName());
    }

    /**
     * Return the command line for invoking the app with the given arguments. Access to any external directories must
     * be enumerated in the externalDirs parameter.
     * @param externalDirs all external directories which need to be mounted in the container
     * @param appArgs arguments to the app
     * @return the complete singularity command
     */
    protected String getSingularityCommand(List<String> externalDirs, List<String> appArgs) throws FileNotFoundException {

        StringBuilder cmd = new StringBuilder();

        int i = 0;
        for(String dir : externalDirs) {
            cmd.append("B").append(++i).append("=").append(dir).append("\n");
        }

        cmd.append(singularityBinPath);
        cmd.append(" run");
        for(int j=1; j<=i; j++) {
            cmd.append(" -B $B").append(j);
        }
        cmd.append(" --app ").append(appName);
        cmd.append(" ").append(getContainerFilepath());
        for(String arg : appArgs) {
            cmd.append(" ");
            cmd.append(arg);
        }

        return cmd.toString();
    }

}
