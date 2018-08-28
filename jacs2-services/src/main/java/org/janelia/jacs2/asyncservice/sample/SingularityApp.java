package org.janelia.jacs2.asyncservice.sample;

import org.janelia.jacs2.asyncservice.utils.ContainerizedServiceUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.domain.DomainDAO;
import org.janelia.model.domain.compute.ContainerizedService;

import javax.inject.Inject;
import java.util.List;

/**
 * A versioned Singularity container.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SingularityApp {

    @Inject
    @PropertyValue(name = "Singularity.LocalImages.Path")
    private String containersPath;

    @Inject
    private DomainDAO domainDao;

    private String containerName;
    private String containerVersion;
    private String appName;
    private ContainerizedService container;

    /**
     * Use the latest version of the given container.
     * @param containerName
     * @param appName
     */
    public SingularityApp(String containerName, String appName) {
        this.containerName = containerName;
        this.appName = appName;
        resolve();
    }

    /**
     * Pick a specific version of the container.
     * @param containerName
     * @param containerVersion
     * @param appName
     */
    public SingularityApp(String containerName, String containerVersion, String appName) {
        this.containerName = containerName;
        this.containerVersion = containerVersion;
        this.appName = appName;
        resolve();
    }

    private final void resolve() {

        List<ContainerizedService> containers = domainDao.getDomainObjectsByName(null, ContainerizedService.class, containerName);
        if (containers.isEmpty()) {
            throw new IllegalArgumentException("Cannot find container with name "+containerName);
        }

        if (containerVersion==null) {
            ContainerizedServiceUtils.sortByVersion(containers);
            this.container = containers.get(containers.size()-1); // latest version
        }
        else {
            this.container = ContainerizedServiceUtils.getContainer(containers, containerName, containerVersion);
            if (container==null) {
                throw new IllegalArgumentException("Cannot find container with name "+containerName+" and version "+containerVersion);
            }
        }
    }

    protected String getContainerFilepath() {
        String prefix = containersPath+"/"+containerName+"-"+containerVersion;
        String filepath = prefix+".simg";
        return filepath;
    }

    /**
     * Return the command line for invoking the app with the given arguments. Access to any external directories must
     * be enumerated in the externalDirs parameter.
     * @param externalDirs all external directories which need to be mounted in the container
     * @param appArgs arguments to the app
     * @return the complete singularity command
     */
    protected String getSingularityCommand(List<String> externalDirs, List<String> appArgs) {

        StringBuilder cmd = new StringBuilder();

        int i = 0;
        for(String dir : externalDirs) {
            cmd.append("B").append(++i).append("=").append(dir).append("\n");
        }

        cmd.append(" singularity run");
        for(int j=1; j<=i; j++) {
            cmd.append(" -B $B").append(j);
        }
        cmd.append(" --app ").append(appName);
        cmd.append(" ").append(getContainerFilepath());
        for(String arg : appArgs) {
            cmd.append(arg);
        }

        return cmd.toString();
    }

}
