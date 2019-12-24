package com.qihoo.qsql.exec;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;

public class MongoQueryHandler {
    public static void main( String args[] ) {

        // Creating a Mongo client
        MongoCredential credentialOne =MongoCredential.createScramSha1Credential("xxxx", "test","xxxx"
                .toCharArray());
        MongoClient mongoClient = new MongoClient(new ServerAddress("1.1.1.1" , 7774 ),
            Arrays.asList(credentialOne));
        MongoDatabase  database = mongoClient.getDatabase("test");
        // Accessing the database
        MongoCollection<Document> collection = database.getCollection("products");

        BasicDBObject andQuery = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        //obj.add(new BasicDBObject("number", 2));
        obj.add(new BasicDBObject("name", "KoBi"));
        andQuery.put("$and", obj);

        //List<Document> documents =  collection.find(andQuery);

        for (Document doc : collection.find(andQuery)) {
            System.out.println(doc.toJson());
        }

    }
}
