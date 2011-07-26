package org.janelia.it.FlyWorkstation.gui.framework.session_mgr;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

/**
* The SessionModel manages BrowserModels, as well as providing the API
* for event registeration/deregistration for the BrowserModels.  This
* class follows the Singleton pattern (Gamma, et al.).  As such, you
* must get the instance using SessionModel.getSessionModel().
*
* Initially written by: Peter Davies
*/

public class SessionModel extends GenericModel {
  private static SessionModel sessionModel=new SessionModel();
  private Vector browserModels=new Vector(10);

  private SessionModel() {
    super();
    browserModels=new Vector(10);
  }  //Singleton pattern enforcement --PED 5/13

  static SessionModel getSessionModel() {return sessionModel;} //Only the SessionManager should have direct access.

  BrowserModel addBrowserModel() {
     BrowserModel browserModel=new BrowserModel();
     browserModels.addElement(browserModel);
     fireBrowserAdded(browserModel);
     return browserModel;
  }

  void addBrowserModel(BrowserModel browserModel) {
     browserModels.addElement(browserModel);
     fireBrowserAdded(browserModel);
  }

  /**
  * Exit the application if the last browserModel is removed
  */

  public void removeBrowserModel(BrowserModel browserModel) {
    browserModels.removeElement(browserModel);
    browserModel.dispose();
    fireBrowserRemoved(browserModel);
    if (browserModels.isEmpty()) SessionMgr.getSessionMgr().systemExit();
  }

  /**
  * Exit the application with full notification
  */
  public void removeAllBrowserModels() {
    BrowserModel browserModel;
    for (Enumeration e=browserModels.elements();e.hasMoreElements(); ) {
      browserModel=(BrowserModel)e.nextElement();
      browserModel.dispose();
      fireBrowserRemoved(browserModel);
    }
  }

  public void addSessionListener(SessionModelListener sessionModelListener) {
    for (Enumeration e=browserModels.elements();e.hasMoreElements();)
       sessionModelListener.browserAdded((BrowserModel)e.nextElement());
    if (!modelListeners.contains(sessionModelListener)) modelListeners.add(sessionModelListener);
  }

  public void removeSessionListener(SessionModelListener sessionModelListener) {
    modelListeners.remove(sessionModelListener);
  }

  public int getNumberOfBrowserModels() {
    return this.browserModels.size();
  }

  public void systemWillExit() {
     removeAllBrowserModels();
     fireSystemExit();
  }

//  void loadProgressMeterStateChange(boolean on) {
//     fireLoadProgressStateChange(on);
//  }

  private void fireBrowserRemoved(BrowserModel browserModel) {
    for (Iterator e=modelListeners.iterator();e.hasNext();)
       ((SessionModelListener)e.next()).browserRemoved(browserModel);
  }

  private void fireBrowserAdded(BrowserModel browserModel) {
    for (Iterator e=modelListeners.iterator();e.hasNext();)
       ((SessionModelListener)e.next()).browserAdded(browserModel);
  }

  private void fireSystemExit() {
    for (Iterator e=modelListeners.iterator();e.hasNext();)
       ((SessionModelListener)e.next()).sessionWillExit();
  }

}
