## RRAMEN | An Interactive Tool for Evaluating Choices and Changes in Transportation Networks
<kbd><img src="/images/rramen.png" /></kbd>

---

RRAMEN is an interactive tool that supports different city-scale mobility-related queries by different types of users with different needs. RRAMEN is based on the concept of Relative Reachability, and its main goal is to aid individual users and urban planners in making informed choices and evaluating changes w.r.t. a city's transportation network. 

### 1. Examples & Usage

#### a. Individual Users
Individual users can use RRAMEN to find easily reachable facilities within a city, or to make decisions related to their commute. Next, we show some examples of queries that can be answered by RRAMEN and are of interest to individual users.
##### Single source-multiple destinations
<kbd><img src="/images/ind-one-to-many-path.png" /></kbd>

##### Multiple sources-single destination
<kbd><img src="/images/ind-many-to-one.png" /></kbd>

##### Single source-region destination
<kbd><img src="/images/ind-one-to-region.png" /></kbd>
<kbd><img src="/images/ind-one-to-neig.png" /></kbd>

#### b. Urban Planners
Urban Planners can use RRAMEN to study transportation systems and the impact of changes on them.
##### Multiple sources-region destination
<kbd><img src="/images/plan-many-to-region.png" /></kbd>

##### Removing/adding lines/stops
<kbd><img src="/images/plan-many-to-region-line-removed.png" /></kbd>
<kbd><img src="/images/plan-many-to-region-remove-stop.png" /></kbd>


##### Removing/adding road segments
<kbd><img src="plan-many-to-region-remove-seg-ex.png" /></kbd>
<kbd><img src="/images/plan-many-to-region-remove-seg.png" /></kbd>

### 2. Installation & Setup
#### a. Database
RRAMEN uses PostGIS to store road and public transportation network data. 
It requires PostgreSQL 10.10 or higher and PostGIS 2.5 or higher.

RRAMEN reads the database connection values (username, password, database name, etc.) from a config file `config.ini`. `config-sample.ini` (located in the root directory) is a sample config file. You must set the database connection values under the section `[postgresql]` and copy the contents of the sample file to a file named `config.ini`, which should be placed in the root directory.

#### b. Mapbox
RRAMEN employs Mapbox to display the map and visualize results. Mapbox requires an access token which can be obtained after creating an account on [https://account.mapbox.com/auth/signup/](https://account.mapbox.com/auth/signup/). Once your account is set up, you can find your API key on [https://www.mapbox.com/studio/](https://www.mapbox.com/studio/) under the “Access Token” section.

The `MAPBOX_ACCESS_KEY` must be set in the `config.ini` file under the `[mapbox]` section.

#### c. Datasets
In order to solve RR queries, RRAMEN requires the following datasets for a given region of interest.
##### OSM Map
RRAMEN uses OSM maps ([https://www.openstreetmap.org/](https://www.openstreetmap.org/)) in order to model a city's road network.
After downloading an OSM map for a given region, the `.osm` file must be placed in `data/osm/<region-name>/<osm-file>` under RRAMEN's root directory.
##### OSM Boundaries
In order to allow users to select neighborhoods from a city, RRAMEN requires `.json` files containing a hierarchy of boundaries that separates the given region into smaller areas. Such data can be manually downloaded from [https://wambachers-osm.website/boundaries/](https://wambachers-osm.website/boundaries/) or using the following command:
```
curl -f -o <region-name>.zip --url 'https://wambachers-osm.website/boundaries/exportBoundaries?cliVersion=1.0&cliKey=<client-key>&exportFormat=json&exportLayout=levels&exportAreas=land&from_al=2&to_al=12&union=false&selected=<region-code>'
```
Where `<client-key>` is a key obtained from [https://wambachers-osm.website/boundaries/](https://wambachers-osm.website/boundaries/) and `<region-code>` is the number of the relation representing the region you want to export, which can be obtained from [https://wambachers-osm.website/boundaries/](https://wambachers-osm.website/boundaries/) or [https://www.openstreetmap.org/](https://www.openstreetmap.org/). For instance, `<region-code>` for Berlin is 62422.

More details about how to export the boundaries for a given region can be found [here](https://wambachers-osm.website/index.php/projekte/internationale-administrative-grenzen/boundaries-map-4-3-english).

The exported `.json` files must be placed in `data/osm_boundaries/<region-name>/` under RRAMEN's root directory.

##### General Transit Feed Specification (GTFS) data
Finally, RRAMEN uses GTFS feeds to build public transportation networks with the corresponding timetables. Such feeds can be obtained, for instance, from [https://transitfeeds.com/](https://transitfeeds.com/). 
RRAMEN requires the following text files:
* agency.txt
* stops.txt
* routes.txt
* trips.txt
* stop_times.txt
* calendar.txt (if not provided, calendar_dates.txt must be provided)
* transfers.txt (optional)

The `.txt` files from a GTFS feed must be placed in `data/gtfs/<region-name>/` under RRAMEN's root directory.

#### d. Data Importer
Once the database connection values are set in `config.ini` and all required datasets are in the `data/` folder, the networks can be imported into the database. In order to do so, simply run:
```
./data_importer.sh <region-name>
```

#### e. Starting the server
You can start the server by running:
```
./run.sh <region-name>
```
Next, browse to [http://127.0.0.1:5000/rramen](http://127.0.0.1:5000/rramen) to use RRAMEN.

### 3. Background and Research
