package de.komoot.photon;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import de.komoot.photon.elasticsearch.Server;
import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.query.PhotonRequestFactory;
import de.komoot.photon.searcher.SearchHandler;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.Client;
import org.json.JSONArray;
import org.json.JSONObject;
import spark.Request;
import spark.Response;
import spark.RouteImpl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
public class AddHandler<R extends PhotonRequest> extends RouteImpl
{
   private static final String DEBUG_PARAMETER = "debug";

   private final PhotonRequestFactory photonRequestFactory;
   private final SearchHandler requestHandler;
   private String[] languages = {"en"};
   private Client esNodeClient;

   public static void main(String[] args)
   {
      AddHandler handler = new AddHandler();
      handler.importOpenAddress();
   }

   AddHandler()
   {
      super("");
      photonRequestFactory = null;
      requestHandler = null;
   }

   AddHandler(String path, SearchHandler dbHandler, Server server, String[] languages, String defaultLanguage)
   {
      super(path);
      List<String> supportedLanguages = Arrays.asList(languages);
      this.photonRequestFactory = new PhotonRequestFactory(supportedLanguages, defaultLanguage);
      this.requestHandler = dbHandler;
      this.esNodeClient = server.getClient();
   }

   @Override
   public String handle(Request request, Response response)
   {
      importOpenAddress();
      return "";
   }

   private PhotonDoc getDocument(String prefix, String streetStr, String cityStr, String stateStr,
      String postcode, double latitude, double longitude, long id, String countryCode)
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
      doc.countryCode(countryCode);
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

   private static List<String> getGEOFiles(String directory)
   {
      List<String> results = new ArrayList<String>();
      try
      {
         Files.walk(Paths.get(directory)).filter(Files::isRegularFile).forEach((f) -> {
            String path = f.toString();
            if (path.endsWith(".geojson"))
            {
               // Only want to process files with addresses (exclude buildings and parcels)
               if (path.contains("addresses"))
               {
                  log.info(path);
                  results.add(path);
               }
               else
               {
                  log.info("EXCLUDED: " + path);
               }
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

      /////////////////////////////////////////
      // OLD
      /////////////////////////////////////////

      //      importOpenAddress("G:\\OSM\\2021-02-25\\nz-street-address.csv", ImportType.NZ, CountryCode.NZ, "", test);
      //      importOpenAddress("G:\\OSM\\2021-02-25\\countrywide.csv", ImportType.GNAF, CountryCode.AU, "", test);
      //      importOpenAddress("/mnt/g/OSM/2021-05-20/nz-street-address.csv", ImportType.NZ, CountryCode.NZ, "", test);
      //      importOpenAddress("/mnt/g/OSM/2021-05-20/countrywide.csv", ImportType.GNAF, CountryCode.AU, "", test);

      //      // Loop through all the USA csv files that can be found
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

      /////////////////////////////////////////
      // AUSTRALIA
      /////////////////////////////////////////

      importOpenAddressGEO("C:\\aa\\OSM\\source.geojson", ImportType.OA, "AU", null, test);

      /////////////////////////////////////////
      // USA
      /////////////////////////////////////////

      //      importZip("F:\\2022-03\\US zip codes.txt");
      //      // Loop through all the USA geo files that can be found
      //      int fileCount = 1;
      //      total = 0;
      ////      List<String> files = getGEOFiles("F:\\2022-03\\test\\");
      //      List<String> files = getGEOFiles("F:\\2022-03\\us\\");
      ////      List<String> files = getGEOFiles("C:\\aa\\test\\");
      //      for (String file: files)
      //      {
      //         int index1 = file.lastIndexOf("\\");
      //         int index2 = file.substring(0, index1).lastIndexOf("\\");
      //         String state = file.substring(index2 + 1, index1).toUpperCase();
      //         log.warn("Processing file " + fileCount++ + " of " + files.size() + ": " + file);
      //         importOpenAddressGEO(file, ImportType.OA, CountryCode.US, state, test);
      //      }
   }

   private int total = 0;
   private int totalInvalid = 0;
   private int totalNom = 0;
   private int totalCache = 0;

   private Hashtable<String, ZipData> zipMap = new Hashtable<>();

   private static class ZipData
   {
      String zip;
      String city;
      String county;
      String state;
   }

   private static class StreetData
   {
      String street;
      String state;
      String postcode;
      String city;
      double latitude;
      double longitude;
      boolean cache = false;
   }

   public static double calcDist(double lat1, double lon1, double lat2, double lon2)
   {
      // Calculate using Great Circle
      // ACOS(SIN(RADIANS(C2))*SIN(RADIANS(C3)) +
      //      COS(RADIANS(C2))*COS(RADIANS(C3))*COS(RADIANS(D2-D3)))*6378*1000
      double lonDist = Math.abs(lon1 - lon2);
      double la1 = Math.toRadians(lat1);
      double la2 = Math.toRadians(lat2);
      double lod = Math.toRadians(lonDist);
      double sin = Math.sin(la1) * Math.sin(la2);
      double cos = Math.cos(la1) * Math.cos(la2) * Math.cos(lod);
      double acos = Math.acos(sin+cos);
      double distGC = acos * 6378.0;

      return distGC;
   }

   private static double MAXDIST = 500; // max distance for a street to be considered the same (in metres)
   private HashMap<String, ArrayList<StreetData>> streetMap = new HashMap<>();

   /**
    * Returns information on a street if the file did not already contain the city, state and postcode. It does
    * this by looking it up in a nominatim call. It will use a cache of streets already looked up for speed purposes
    * but will only consider it a match if it is within MAXDIST metres;
    */
   public StreetData getStreetData(String street, double latitude, double longitude)
   {
      StreetData result = null;
      String key = street;
      // Get the list of existing streets that match the name
      ArrayList<StreetData> list = streetMap.get(key);
      if (list != null)
      {
         for (StreetData data: list)
         {
            // Check to see if this street is within MAXDIST of this data
            double dist = calcDist(latitude, longitude, data.latitude, data.longitude);
            if (dist < MAXDIST)
            {
               // Use this one as the street data for this address
               result = data;
               result.cache = true;
               return result;
            }
         }
      }

      // Try a reverse lookup first to find a location
      String url = "http://localhost:2323/reverse?lat=" + latitude + "&lon=" + longitude;
      result = getData(url, street, latitude, longitude);

      // Next try searching for the address
      //      if (result == null)
      //      {
      //         url = "http://localhost:2323/api?limit=1&location_bias_scale=10&lat=" + latitude + "&lon=" + longitude +
      //            "&q=" + street.replace(" ", "%20");
      //         result = getData(url, street, latitude, longitude);
      //      }

      // Store the result in the cache if one was found
      if (result != null)
      {
         if (list == null)
         {
            list = new ArrayList<>();
            streetMap.put(key, list);
         }
         list.add(result);
      }
      else
      {
         //log.error("BLANK DATA: " + street + "\n" + url);
      }
      return result;
   }

   private StreetData getData(String url, String street, double latitude, double longitude)
   {
      StreetData result = null;
      try
      {
         HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
         InputStream inputStream = connection.getInputStream();
         BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
         String json = reader.readLine();
         JSONObject obj = new JSONObject(json);
         JSONArray array = obj.getJSONArray("features");
         if (array.length() == 0) return null;
         JSONObject properties = array.getJSONObject(0).getJSONObject("properties");

         result = new StreetData();
         result.city = properties.optString("city", "");
         result.state = properties.optString("state", "");
         result.postcode = properties.optString("postcode", "");
         result.street = street;
         result.latitude = latitude;
         result.longitude = longitude;

         // Only use the initial digits if more is given
         if (result.postcode.contains("-"))
         {
            result.postcode = result.postcode.substring(0, result.postcode.indexOf("-"));
         }

         if (result.city.isEmpty() || result.state.isEmpty())
         {
            // If the city or state are missing try to get it from the zip code
            if (!result.postcode.isEmpty())
            {
               ZipData zipData = zipMap.get(result.postcode);
               if (zipData != null)
               {
                  if (result.city.isEmpty()) result.city = zipData.city;
                  if (result.state.isEmpty()) result.state = zipData.state;
               }
            }
         }

         // If it is still empty
         if (result.city.isEmpty() || result.state.isEmpty())
         {
            // Try using the county instead
            String county = properties.optString("county", "");
            if (!county.isEmpty())
            {
               result.city = county;
            }
            else
            {
               //               log.error("BLANK DATA: " + street + " - \n" + url + "\n" + json);
               //               result = null;
            }
         }

      }
      catch(Exception e)
      {
         log.error("Error reading HTTP call data", e);
      }
      return result;
   }

   // CLear the street cache between regions as there will be no overlap
   public void clearStreetCache()
   {
      streetMap = new HashMap<>();
   }

   public void importOpenAddressGEO(String file, ImportType type, String countryCode, String state, boolean test)
   {

      try
      {
         int invalid = 0;
         int nom = 0;
         int cache = 0;
         int uid = 1900000000;

         BufferedReader reader = new BufferedReader(new FileReader(file));
         log.info("Starting Import");
         clearStreetCache();

         de.komoot.photon.elasticsearch.Importer importer = null;
         if (!test)
         {
            importer = new de.komoot.photon.elasticsearch.Importer(esNodeClient, languages, new String[]{""});
         }
         log.info("Starting Importer");

         String line = reader.readLine();
         int count = 0;
         if (line != null) line.replaceAll("\"", "");
         while (line != null)
         {
            try
            {
               JSONObject lineObj = new JSONObject(line);
               JSONObject properties = lineObj.getJSONObject("properties");
               JSONObject geometry = lineObj.getJSONObject("geometry");
               JSONArray coords = null;
               if (geometry != null)
               {
                  coords = geometry.getJSONArray("coordinates");
               }

               if (properties == null || geometry == null || coords == null || coords.length() != 2)
               {
                  line = reader.readLine();
                  continue;
               }

               double longitude = coords.getDouble(0);
               double latitude = coords.getDouble(1);;
               if (longitude > 180)
               {
                  longitude = -360 + longitude;
               }

               // Ensure boundaries are appropriate
               if (longitude > 180 || longitude < -180 || latitude > 85 || latitude < -85)
               {
                  line = reader.readLine();
                  continue;
               }

               String text = properties.getString("number");
               text = text == null ? "" : text;
               String text2 = properties.getString("unit");
               text2 = text2 == null ? "" : text2;
               if (!text2.isEmpty())
               {
                  text = text2 + " " + text;
               }
               text = text.replace("'", "''");
               text = text.replace("\\", "\\\\");

               String streetStr = properties.getString("street");;
               if (type == ImportType.OA)
               {
                  streetStr = changeAbbreviations(streetStr);
               }
               String cityStr = properties.getString("city");
               String postcode = properties.getString("postcode");
               String stateString = properties.getString("region");

               long id = 0;
               String idString = properties.getString("id");
               if (countryCode.equals("AU")) {
                  idString = idString.replace("GAQLD", "1").replace("GAVIC", "2")
                     .replace("GANSW", "3").replace("GASA_", "4")
                     .replace("GAWA_", "5").replace("GATAS", "6")
                     .replace("GANT_", "7").replace("GAACT", "8")
                     .replace("GAOT_", "9");
               }
               try
               {
                  BigInteger bi = new BigInteger(idString, 16);
               }
               catch(NumberFormatException nfe)
               {
                  idString = "";
               }
               if (idString == null || idString.trim().isEmpty())
               {
                  // Use the hash instead
                  idString = properties.getString("hash");
               }
               if (idString != null && !idString.trim().isEmpty())
               {
                  BigInteger bi = new BigInteger(idString, 16);
                  id = bi.longValue();
               }

               if (stateString.isEmpty() && countryCode.equals("US"))
               {
                  int index1 = file.indexOf("\\us\\");
                  int index2 = file.indexOf("\\", index1 + 4);
                  stateString = file.substring(index1 + 4, index2).toUpperCase();
               }

               if (streetStr == null || streetStr.isEmpty())
               {
                  // Skip these as they have no use
                  line = reader.readLine();
                  continue;
               }
               else if (cityStr.isEmpty() || stateString.isEmpty())
               {
                  // Perform a lookup on the street for this address in nominatim
                  StreetData data = getStreetData(streetStr, latitude, longitude);
                  if (data != null && !data.city.isEmpty())
                  {
                     if (data.cache)
                     {
                        cache++;
                        totalCache++;
                     }
                     else
                     {
                        nom++;
                        totalNom++;
                     }
                     cityStr = data.city;
                     postcode = data.postcode;
                     stateString = data.state;
                  }
                  else
                  {
                     invalid ++;
                     totalInvalid ++;
                     line = reader.readLine();
                     continue;
                  }
               }

               PhotonDoc doc = getDocument(text, streetStr, cityStr, stateString, postcode,
                  latitude, longitude, id, countryCode);

               total ++;
               count ++;
               if (!test) importer.add(doc, uid++);

               if (count < 10000 && count % 1000 == 0)
               {
                  //                  log.info("Progress: " + total + " (" + count + ") - invalid " + invalid);
               }
               if (count % 100000 == 0)
               {
                  log.info("Progress: " + total + " (" + count + ") - invalid " + invalid);
               }
//               if (count % 300000 == 0)
//               {
//                  if (!test) importer.finish();
//               }
            }
            catch(Exception e)
            {
               log.warn("Error on line " + count + ": " + line, e);
            }

            line = reader.readLine();
         }

         log.info("Complete: TOTAL " + count + " (" + total + ") | INVALID " +
            invalid + " (" + totalInvalid + ") | PERCENT " +
            (count == 0 ? 100 : (Math.round(100.0 * (double)invalid/(double)count))) +
            " (" + (Math.round(100.0 * (double)totalInvalid/(double)total)) + ") | NOM " +
            nom + " (" + totalNom + ") | CACHE " +
            cache + " (" + totalCache + ") - " + file);
         if (!test) importer.finish();
         reader.close();
      }
      catch (Exception e)
      {
         log.error("Error reading URL data", e);
      }
   }

   public void importOpenAddress(String file, ImportType type, String countryCode, String state, boolean test)
   {
      try
      {
         int totalAddress = 0;
         int invalid = 0;
         int uid = 1900000000;

         BufferedReader reader = new BufferedReader(new FileReader(file));
         log.info("Starting Import");

         de.komoot.photon.elasticsearch.Importer importer = new de.komoot.photon.elasticsearch.Importer(esNodeClient, languages, new String[]{""});
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

               if (streetStr == null || cityStr == null || id == 0 || streetStr.isEmpty() || cityStr.isEmpty())
               {
                  if (streetStr == null || streetStr.isEmpty())
                  {
                     // Don't report on these
                  }
                  else
                  {
                     //log.warn("Error on line " + count + ": " + line);
                     invalid ++;
                     totalInvalid ++;
                  }
                  line = reader.readLine();
                  continue;
               }

               PhotonDoc doc = getDocument(text, streetStr, cityStr, stateString, postcode,
                  latitude, longitude, id, countryCode);

               count ++;
               if (!test) importer.add(doc, uid++);

               if (count < 10000 && count % 1000 == 0)
               {
                  //                  log.info("Progress: " + total);
               }
               if (count % 10000 == 0)
               {
                  //                  log.info("Progress: " + total);
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
         log.info("Complete: TOTAL " + count + " (" + total + ") | INVALID " +
            invalid + " (" + totalInvalid + ") | PERCENT " +
            (count == 0 ? 100 : (Math.round(100.0 * (double)invalid/(double)count))) +
            " (" + (Math.round(100.0 * (double)totalInvalid/(double)total)) + ") - " + file);
         if (!test) importer.finish();
         reader.close();
      }
      catch (Exception e)
      {
         log.error("Error reading URL data", e);
      }
   }

   public void importZip(String file)
   {
      try
      {
         BufferedReader reader = new BufferedReader(new FileReader(file));
         log.info("Starting ZIP Import");
         String line = reader.readLine();
         int count = 0;
         while (line != null)
         {
            try
            {
               total ++;
               String[] parts = line.split("\t");
               //               String[] parts = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
               ZipData data = new ZipData();
               data.zip = parts[1];
               data.city = parts[2];
               data.state = parts[4];
               data.county = parts[5];
               zipMap.put(data.zip, data);
            }
            catch(Exception e)
            {
               log.error("Error on line " + count, e);
            }

            line = reader.readLine();
         }

         reader.close();
      }
      catch (Exception e)
      {
         log.error("Error reading URL data", e);
      }
   }

}