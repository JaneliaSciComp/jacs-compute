package org.janelia.jacs2.asyncservice.containerizedservices;

import java.nio.file.Path;

class ContainerImage {
    String protocol;
    Path localPath;
    String imageName;

    ContainerImage(String protocol, Path localPath, String imageName) {
        this.protocol = protocol;
        this.localPath = localPath;
        this.imageName = imageName;
    }

    boolean isLocalImage() {
        return "file".equals(protocol);
    }

    boolean requiresPull() {
        return "shub".equals(protocol) || "docker".equals(protocol);
    }

    Path getLocalImagePath() {
        return localPath.resolve(imageName);
    }

    boolean localImageExists() {
        return getLocalImagePath().toFile().exists();
    }
}
