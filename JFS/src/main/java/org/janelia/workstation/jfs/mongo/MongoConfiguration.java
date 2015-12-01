package org.janelia.workstation.jfs.mongo;

import com.mongodb.MongoClient;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

/**
 * Created by schauderd on 10/28/15.
 */
public class MongoConfiguration {
    private Jongo jongo;
    private MongoClient client;
    private String collection;

    public String getCollection() {
        if (collection==null) {
            collection = "object";
        }
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public Jongo getJongo() {
        return jongo;
    }

    public void setJongo(Jongo jongo) {
        this.jongo = jongo;
    }

    public MongoClient getClient() {
        return client;
    }

    public void setClient(MongoClient client) {
        this.client = client;
    }

    public MongoCollection getObjectCollection() {
        return jongo.getCollection(collection);
    }

    public void cleanUp() {
        client.close();
    }

}
