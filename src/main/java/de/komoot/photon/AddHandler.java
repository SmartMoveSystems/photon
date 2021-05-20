package de.komoot.photon;

import static spark.Spark.halt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.elasticsearch.client.Client;
import org.json.JSONObject;

import com.neovisionaries.i18n.CountryCode;
import com.vividsolutions.jts.geom.*;

import de.komoot.photon.query.BadRequestException;
import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.query.PhotonRequestFactory;
import de.komoot.photon.searcher.BaseElasticsearchSearcher;
import de.komoot.photon.searcher.PhotonRequestHandler;
import de.komoot.photon.utils.ConvertToGeoJson;
import lombok.extern.slf4j.Slf4j;
import spark.Request;
import spark.Response;
import spark.RouteImpl;

@Slf4j
public class AddHandler<R extends PhotonRequest> extends RouteImpl
{
   private static final String DEBUG_PARAMETER = "debug";

   private final PhotonRequestFactory photonRequestFactory;
   private final PhotonRequestHandler requestHandler;
   private final ConvertToGeoJson geoJsonConverter;
   private String languages = "en";
   private Client esNodeClient;

   AddHandler(String path, Client esNodeClient, String thisLanguages, String defaultLanguage)
   {
      super(path);
      this.esNodeClient = esNodeClient;
      List<String> supportedLanguages = Arrays.asList(languages.split(","));
      this.photonRequestFactory = new PhotonRequestFactory(supportedLanguages, defaultLanguage);
      this.geoJsonConverter = new ConvertToGeoJson();
      this.requestHandler = new PhotonRequestHandler(new BaseElasticsearchSearcher(esNodeClient), supportedLanguages);
   }

   @Override
   public String handle(Request request, Response response)
   {
      PhotonRequest photonRequest = null;
      try {
         photonRequest = photonRequestFactory.create(request);
      } catch (BadRequestException e) {
         JSONObject json = new JSONObject();
         json.put("message", e.getMessage());
         halt(e.getHttpStatus(), json.toString());
      }
      List<JSONObject> results = requestHandler.handle(photonRequest);
      JSONObject geoJsonResults = geoJsonConverter.convert(results);

      importOpenAddress();
      if (photonRequest.getDebug()) {
         JSONObject debug = new JSONObject();
         debug.put("query", new JSONObject(requestHandler.dumpQuery(photonRequest)));
         geoJsonResults.put("debug", debug);
         return geoJsonResults.toString(4);
      }

      return geoJsonResults.toString();
   }

   private PhotonDoc getDocument(String prefix, String streetStr, String cityStr, String stateStr, 
      String postcode, double latitude, double longitude, long id, CountryCode countryCode)
   {
      GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
      Point point = geometryFactory.createPoint(new Coordinate(longitude, latitude));

//          public PhotonDoc(long placeId, String osmType, long osmId, String tagKey, String tagValue) {

      PhotonDoc doc = new PhotonDoc(id, // long placeId
         "N", // String osmType
         id, // long osmId
         "place", // String tagKey
         "house"); // String tagValue
      doc.houseNumber(prefix);
      doc.centroid(point);
      doc.countryCode(countryCode.getAlpha2());
      doc.postcode(postcode);

      HashMap<String, String> address = new HashMap<>();
      address.put("street", streetStr);
      address.put("city", cityStr);
//      address.put("suburb", streetStr);
//      address.put("neighbourhood", streetStr);
//      address.put("county", streetStr);
      address.put("state", stateStr);
      doc.address(address);

      return doc;
   }
   
   private static List<String> getCSVFiles(String directory)
   {
      List<String> results = new ArrayList<String>();
      try
      {
         Files.walk(Paths.get(directory)).filter(Files::isRegularFile).forEach((f) -> {
            String path = f.toString(); 
            if (path.endsWith(".csv"))
            {
               log.info(path);
               results.add(path);
            }
         });
      }
      catch (Exception e)
      {
         log.error("Error", e);
      }
      return results;
   }

   enum ImportType 
   {
      GNAF, OA, NZ
   }
   
   private String changeAbbreviations(String text)
   {
      boolean found = false;
      String result = text;
      if (text.isEmpty()) return result;
      
      String[][] map = {
         {"ST", "STREET"},
         {"RD", "ROAD"},
         {"DR", "DRIVE"},
         {"AVE", "AVENUE"},
         {"CIR", "CIRCUIT"},
         {"CR", "CIRCLE"},
         {"CT", "COURT"},
         {"PL", "PLACE"},
         {"LN", "LANE"},
         {"MTN", "MOUNTAIN"},
         {"HL", "HILL"},
         {"HWY", "HIGHWAY"},
         {"WAY", "WAY"},
         {"E", "EAST"},
         {"W", "WEST"},
         {"S", "SOUTH"},
         {"N", "NORTH"},
         {"HBR", "HARBOR"},
         {"CRES", "CRESCENT"},
         {"CLS", "CLOSE"},
         {"LOOP", "LOOP"},
         {"BLVD", "BOULEVARD"},
         {"TRL", "TRAIL"},
         {"ALY", "ALLEY"},
         {"PKWY", "PARKWAY"},
         {"PKY", "PARKWAY"},
         {"BND", "BEND"},
         {"TER", "TERRACE"},
         {"TRCE", "TERRACE"},
         {"RUN", "RUN"},
         {"EXT", "EXTENSION"},
         {"PATH", "PATH"},
         {"VIS", "VISTA"},
         {"HTS", "HEIGHTS"},
         {"Rd", "Road"},
         {"Dr", "Drive"},
         {"St", "Street"},
         {"Ln", "Lane"},
         {"Ave", "Avenue"},
         {"Pl", "Place"},
         {"Blvd", "Boulevard"},
         {"Hwy", "Highway"},
         {"Ct", "Court"},
         {"Cir", "Circuit"},
      };
      for (String[] item: map)
      {
         if (result.endsWith(" " + item[0]))
         {
            result = result.substring(0, result.length() - item[0].length()) + item[1];
            found = true;
            break; // Assume only one street type
         }
      }
      if (!found)
      {
         //log.info("no change: " + text);
      }
      
      return result;
   }
   
   public void importOpenAddress()
   {
      boolean test = false;
//      importOpenAddress("G:\\OSM\\2021-02-25\\nz-street-address.csv", ImportType.NZ, CountryCode.NZ, "", test);
//      importOpenAddress("G:\\OSM\\2021-02-25\\countrywide.csv", ImportType.GNAF, CountryCode.AU, "", test);
      importOpenAddress("/mnt/g/OSM/2021-02-25/countrywide.csv", ImportType.GNAF, CountryCode.AU, "", test);

      // Loop through all the USA csv files that can be found
//      int fileCount = 1;
//      total = 0;
//      List<String> files = getCSVFiles("G:\\OSM\\2021-02-25\\us\\");
//      for (String file: files)
//      {
//         int index1 = file.lastIndexOf("\\");
//         int index2 = file.substring(0, index1).lastIndexOf("\\");
//         String state = file.substring(index2 + 1, index1).toUpperCase();
//         log.warn("Processing file " + fileCount++ + " of " + files.size() + ": " + file);
//         importOpenAddress(file, ImportType.OA, CountryCode.US, state, test);
//      }
   }
   
   private int total = 0;
   
   public void importOpenAddress(String file, ImportType type, CountryCode countryCode, String state, boolean test)
   {

      try
      {
         int totalAddress = 0;

         BufferedReader reader = new BufferedReader(new FileReader(file));
         log.info("Starting Import");

         de.komoot.photon.elasticsearch.Importer importer = new de.komoot.photon.elasticsearch.Importer(esNodeClient, languages, "");
         log.info("Starting Importer");

         String line = reader.readLine();
         // Skip the first line as it is a header
         line = reader.readLine();
         int count = 0;
         if (line != null) line.replaceAll("\"", "");
         while (line != null)
         {
            try
            {
               total ++;
               //String[] parts = line.split(",");
               String[] parts = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
               if (parts.length < 5)
               {
                  line = reader.readLine();
                  continue;
               }
               
               boolean isNZ = type == ImportType.NZ;
               double longitude = Double.parseDouble(isNZ ? parts[24] : parts[0]);
               double latitude = Double.parseDouble(isNZ ? parts[25] : parts[1]);
               if (isNZ && longitude > 180)
               {
                  longitude = -360 + longitude;
               }
               
               // Ensure boundaries are appropriate
               if (longitude > 180 || longitude < -180 || latitude > 85 || latitude < -85)
               {
                  line = reader.readLine();
                  continue;
               }
               
               String text = isNZ ? parts[12] : parts[2];
               String text2 = isNZ ? "" : parts[4];
               if (!text2.isEmpty())
               {
                  text = text2 + " " + text;
               }
               text = text.replace("'", "''");
               text = text.replace("\\", "\\\\");

               String streetStr = isNZ ? parts[13] : parts[3];
               if (type == ImportType.OA)
               {
                  streetStr = changeAbbreviations(streetStr);
               }
               String cityStr = isNZ ? parts[10] : parts[5];
               String postcode = isNZ ? "" : parts[8];
               String stateString = "";
               long id = 0;

               if (isNZ)
               {
                  stateString = parts[10].equals(parts[11]) ? "" : parts[11];
                  id = Long.parseLong(parts[1]);
               }
               else 
               {
                  stateString = type == ImportType.GNAF ? parts[7] : state;
                  String idString = "";
                  if (parts.length >= 11)
                  {
                     idString = parts[10];
                  }
                  else if (type == ImportType.OA)
                  {
                     idString = parts[9];
                     idString = idString.replace("-", "");
                  }
                  BigInteger bi = new BigInteger(idString, 16);
                  id = bi.longValue();
               }

               PhotonDoc doc = getDocument(text, streetStr, cityStr, stateString, postcode, 
                  latitude, longitude, id, countryCode);
               
               count ++;               
               if (!test) importer.add(doc);
               
               if (count < 10000 && count % 1000 == 0)
               {
                  log.info("Progress: " + total);
               }
               if (count % 10000 == 0)
               {
                  log.info("Progress: " + total);
               }
               if (count % 300000 == 0)
               {
                  if (!test) importer.finish();
               }
            }
            catch(NumberFormatException nfe)
            {
               log.warn("Number format error: " + nfe.getMessage());
            }
            catch(Exception e)
            {
               log.error("Error on line " + count, e);
            }
            
            line = reader.readLine();
         }

         totalAddress += total;
         log.info("Complete: " + total + " (" + totalAddress + ")");
         if (!test) importer.finish();
         reader.close();
      }
      catch (Exception e)
      {
         log.error("Error reading URL data", e);
      }
   }

}