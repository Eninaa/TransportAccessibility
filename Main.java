import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.simple.JSONObject;
import java.util.*;


public class Main {
    public static void main(String[] args) {

        if(args[0].equals("region63_samarskaya_obl")) {

            String ConnectionString = "mongodb://192.168.57.102:27017";

            MongoClient mongo = MongoClients.create(ConnectionString);
            MongoDatabase db = mongo.getDatabase(args[0]);

            MongoCollection<Document> stops = db.getCollection("ta_stops");
            MongoCollection<Document> houses = db.getCollection("mar_houses");

            BasicDBObject updateObject = new BasicDBObject();
            JSONObject parameters = new JSONObject();
            parameters.put("transportAccessibility", 0);
            updateObject.put("$set", parameters);
            MongoCursor<Document> jo = houses.find().iterator();
            while (jo.hasNext()) {
                Document d = jo.next();
                ObjectId id = (ObjectId) d.get("_id");
                BasicDBObject obj = new BasicDBObject();
                obj.put("_id", id);
                houses.updateOne(obj, updateObject);
            }

            AggregateIterable stage1 = stops.aggregate(Arrays.asList(new Document("$project",
                            new Document("KS_ID",
                                    new Document("$toInt", "$KS_ID"))
                                    .append("title", 1L)
                                    .append("adjacentStreet", 1L)
                                    .append("direction", 1L)
                                    .append("point",
                                            new Document("type", "Point")
                                                    .append("coordinates", Arrays.asList(new Document("$toDouble", "$longitude"),
                                                            new Document("$toDouble", "$latitude"))))),
                    new Document("$lookup",
                            new Document("from", "ta_routes_structure")
                                    .append("localField", "KS_ID")
                                    .append("foreignField", "stops.KS_ID")
                                    .append("as", "routes")),
                    new Document("$lookup",
                            new Document("from", "ta_trips_count")
                                    .append("localField", "routes.KR_ID")
                                    .append("foreignField", "KR_ID")
                                    .append("as", "races")),
                    new Document("$unwind",
                            new Document("path", "$races")),
                    new Document("$group",
                            new Document("_id",
                                    new Document("KS_ID", "$KS_ID")
                                            .append("title", "$title")
                                            .append("adjacentStreet", "$adjacentStreet")
                                            .append("direction", "$direction")
                                            .append("routes", "$routes")
                                            .append("point", "$point"))
                                    .append("countOfRoutes",
                                            new Document("$sum", 1L))
                                    .append("sumOfRaces",
                                            new Document("$sum", "$races.races number"))),
                    new Document("$project",
                            new Document("KS_ID", "$_id.KS_ID")
                                    .append("title", "$_id.title")
                                    .append("adjacentStreet", "$_id.adjacentStreet")
                                    .append("direction", "$_id.direction")
                                    .append("point", "$_id.point")
                                    .append("sumOfRaces", "$sumOfRaces")
                                    .append("countOfRoutes",
                                            new Document("$size", "$_id.routes"))),
                    new Document("$project",
                            new Document("KS_ID", "$_id.KS_ID")
                                    .append("title", "$_id.title")
                                    .append("adjacentStreet", "$_id.adjacentStreet")
                                    .append("direction", "$_id.direction")
                                    .append("point", "$_id.point")
                                    .append("sumOfRaces", "$sumOfRaces")
                                    .append("countOfRoutes", "$countOfRoutes")
                                    .append("sumCoeff",
                                            new Document("$divide", Arrays.asList("$sumOfRaces", 985L)))
                                    .append("countCoeff",
                                            new Document("$divide", Arrays.asList("$countOfRoutes", 38L)))),
                    new Document("$project",
                            new Document("KS_ID", "$_id.KS_ID")
                                    .append("title", "$_id.title")
                                    .append("adjacentStreet", "$_id.adjacentStreet")
                                    .append("direction", "$_id.direction")
                                    .append("point", "$_id.point")
                                    .append("sumOfRaces", "$sumOfRaces")
                                    .append("countOfRoutes", "$countOfRoutes")
                                    .append("coeff",
                                            new Document("$sum", Arrays.asList("$sumCoeff", "$countCoeff")))),
                    new Document("$project",
                            new Document("KS_ID", "$_id.KS_ID")
                                    .append("title", "$_id.title")
                                    .append("adjacentStreet", "$_id.adjacentStreet")
                                    .append("direction", "$_id.direction")
                                    .append("point", "$_id.point")
                                    .append("sumOfRaces", "$sumOfRaces")
                                    .append("countOfRoutes", "$countOfRoutes")
                                    .append("coeff",
                                            new Document("$divide", Arrays.asList("$coeff", 1.869810312583489d)))))).allowDiskUse(true);
            MongoCursor<Document> dd = stage1.iterator();
            AggregateIterable res = null;

            while (dd.hasNext()) {
                Document d = dd.next();
                double c;
                c = (double) d.get("coeff");
                Document p = (Document) d.get("point");
                ArrayList coor = (ArrayList) p.get("coordinates");

                res = houses.aggregate(Arrays.asList(new Document("$geoNear",
                                new Document("near",
                                        new Document("type", "Point")
                                                .append("coordinates", Arrays.asList(coor.get(0), coor.get(1))))
                                        .append("distanceField", "dist")
                                        .append("maxDistance", 500L)),
                        new Document("$project",
                                new Document("_id", "$_id")
                                        .append("Street", "$Street")
                                        .append("HouseNumber", "$HouseNumber")
                                        .append("transportAccessibility", "$transportAccessibility")),
                        new Document("$set",
                                new Document("transportAccessibility",
                                        new Document("$add", Arrays.asList("$transportAccessibility", c))))));

                MongoCursor<Document> hh = res.iterator();

                while (hh.hasNext()) {
                    Document g = hh.next();
                    ObjectId id = (ObjectId) g.get("_id");
                    double c1 = (double) g.get("transportAccessibility");

                    BasicDBObject obj = new BasicDBObject();
                    obj.put("_id", id);
                    JSONObject params = new JSONObject();
                    params.put("transportAccessibility", c1);
                    BasicDBObject updateObj = new BasicDBObject();
                    updateObj.put("$set", params);
                    houses.updateOne(obj, updateObj);
                }
            }
            mongo.close();
        }
    }
}