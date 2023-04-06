package org.example;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;

import java.time.Duration;

public class CouchbaseCapellaCRUD {

    private static final String CONNECTION_STRING = "couchbases://cb.r8gvciwkzk6mlbml.cloud.couchbase.com";
    private static final String USERNAME = "abhijeet@example.com";
    private static final String PASSWORD = "Password@P1";
    private static final String BUCKET_NAME = "person";

    private Cluster cluster;
    private Bucket bucket;
    private Collection collection;

    public CouchbaseCapellaCRUD() {
        cluster = Cluster.connect(CONNECTION_STRING, USERNAME, PASSWORD);
        bucket = cluster.bucket(BUCKET_NAME);
        bucket.waitUntilReady(Duration.ofSeconds(10));
        collection = bucket.defaultCollection();
    }

    public JsonObject create(JsonObject content) {
        String documentId = "user::" + "james@gmail.com";
        Collection collection = bucket.defaultCollection();
        collection.insert(documentId, content);
        return read(documentId);
    }

    public JsonObject read(String id) {
        GetResult getResult = collection.get(id);
        return getResult.contentAsObject();
    }

    public QueryResult readBySQL(String statement) {

        QueryResult result = cluster.query(statement);
        return result;
    }

    public JsonObject update(String id, JsonObject content) {
        collection.upsert(id, content);
        return read(id);
    }

    public void delete(String id) {
        collection.remove(id);
    }

    public static void main(String[] args) {
        CouchbaseCapellaCRUD crud = new CouchbaseCapellaCRUD();

        // Create a new JsonObject
        JsonObject jsonObject = JsonObject.create()
                .put("id", "user::james@gmail.com")
                .put("name", "James Doe")
                .put("age", 30)
                .put("email", "james@gmail.com");
        JsonObject created = crud.create(jsonObject);
        String id = created.getString("id");
        System.out.println("Created: " + created);

        // Read
        JsonObject read = crud.read(id);
        System.out.println("Read: " + read);

        // TODO: CREATE PRIMARY INDEX ON `default`:`person`
        // Create a N1QL query to get all the beer documents
        String statement = "SELECT * FROM `person` where name=\"James Doe\"";
        QueryResult result = crud.readBySQL(statement);

        // Loop through the result set and print each document
        for (JsonObject row : result.rowsAsObject()) {
            System.out.println("SQL++ QUERY Result: "+row);
        }

        // Update
        JsonObject updatedContent = read.put("name", "James Doe");
        JsonObject updated = crud.update(id, updatedContent);
        System.out.println("Updated: " + updated);

        // Delete
//        crud.delete(id);
        try {
            JsonObject deleted = crud.read(id);
            System.out.println("Deleted: " + deleted);
        } catch (DocumentNotFoundException e) {
            System.out.println("Document not found (expected after deletion)");
        }
    }
}