package org.janelia.jacs2.app;

import jakarta.servlet.ServletException;
import jakarta.ws.rs.core.Application;

public interface AppContainer {
    void initialize(Application application, AppArgs appArgs) throws ServletException;
    void start();
}
