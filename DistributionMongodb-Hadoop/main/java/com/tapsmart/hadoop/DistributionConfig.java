package com.tapsmart.hadoop;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
import com.mongodb.hadoop.util.MongoTool;

public class DistributionConfig extends MongoTool {

    public static void main(String[] args) throws Exception {
    	
    	// First of all drop the old distributions collection
    	Properties prop = new Properties();
    	prop.load(DistributionConfig.class.getClassLoader().getResourceAsStream("distribution.properties"));
    	ArrayList<ServerAddress> addr = new ArrayList<ServerAddress>();
    	for (String s: prop.getProperty("database.host").split(",")) {
    	    addr.add(new ServerAddress(s));
    	}
    	Mongo mongoClient = new Mongo(addr );
        DB db = mongoClient.getDB( prop.getProperty("database.name") );
        db.getCollection( prop.getProperty("database.collection.distributions") ).drop();

    	DistributionConfig conf = new DistributionConfig();
    	Configuration cfg = new Configuration();
    	cfg.addResource("mongo-defaults.xml");
    	cfg.addResource("tapsmart-distconsolidation.xml");
    	conf.setConf(cfg);

	    ToolRunner.run(conf, args);
	}
}
