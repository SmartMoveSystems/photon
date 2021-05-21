Example for running the process:
    
    java -jar photon-0.3.4-sm.jar -listen-port 2323 -languages en -default-language en -cluster photonNoNominatim

- Note that the languages need to be set otherwise calls from different sources and browsers may use a default language which doesn't work well and returns unexpected results
- The current SM photon has code in BoundingBoxParamConverter that uses a bounding box of AU and NZ if no bounding box is specified
- The cluster parameter is needed if there is more than one instance of Photon running otherwise it tries to cluster

To update to the latest data:

1. Get latest photon code and jar from https://github.com/komoot/photon or better the SmartMove github version at https://github.com/SmartMoveSystems/photon
1. Get latest photon DB (world) from https://download1.graphhopper.com/public/ (very large 72G+)
1. Get countrywide.csv from http://results.openaddresses.io/sources/au/countrywide (click on the latest "Output")
1. Get latest NZ street data from https://data.linz.govt.nz/layer/53353-nz-street-address/history/
1. If US addresses needed get the 4 US regions from https://results.openaddresses.io/
1. Ensure the files are extracted and paths are set appropriately in the SmartMove github version in AddHandler.java
1. Run the SmartMove photon instance and do to http://localhost:2322/add?q=1 to start the processing (around 1 hour for AU+NZ, 1 day for US)
1. Once finished move a whole bunch of large files around
1. When starting on the new server it may need 10 mins or so to index stuff? 
