package org.janelia.jacs2.app;

import javax.servlet.ServletException;
import javax.ws.rs.core.Application;

public interface AppContainer {
    void initialize(Application application, AppArgs appArgs) throws ServletException;
    void start();
}
