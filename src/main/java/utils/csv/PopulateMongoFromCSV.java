package utils.csv;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bson.Document;

import com.mongodb.BasicDBList;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class PopulateMongoFromCSV {
    public static final String LATITUDE = "lat";
    public static final String LONGITUDE = "lon";
    public static void main(String[] args){
        if(args.length < 2){
            System.out.println("Usage : utils.csv.PopulateMongoFromCSV <csv path> <userid>");
            System.exit(1);
        }
        String csvPath = args[0];
        String userId = args[1];
        File csvFile = new File(csvPath);
        MongoClient mongoClient = new MongoClient( "localhost" , 3001 );
        MongoDatabase db = mongoClient.getDatabase("meteor");
        MongoCollection routes = db.getCollection("routes");
        try (CSVParser csvParser = CSVParser.parse(csvFile, Charset.defaultCharset(),
                CSVFormat.DEFAULT.withHeader())){
            Map<String, Integer> headerMap = csvParser.getHeaderMap(); 
            BasicDBList coordinatePairsList = new BasicDBList();
            for(CSVRecord record : csvParser.getRecords()){
                BasicDBList coordinatePair = new BasicDBList();
                coordinatePair.add(Double.parseDouble(record.get(LATITUDE)));
                coordinatePair.add(Double.parseDouble(record.get(LONGITUDE)));
                coordinatePairsList.add(coordinatePair);
            }
            BasicDBList coordinates = new BasicDBList();
            coordinates.add(coordinatePairsList);
            Document obj =  new Document("userid", userId)
                    .append("route", new Document("type", "MultiLineString")
                    .append("coordinates", coordinates));
            routes.insertOne(obj);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            mongoClient.close();
        }
    }
    
}
