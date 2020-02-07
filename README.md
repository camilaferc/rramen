## RRAMEN | An Interactive Tool for Evaluating Choices and Changes in Transportation Networks
<kbd><img src="/images/rramen.png" /></kbd>

---

RRAMEN is an interactive tool that supports different city-scale mobility-related queries by different types of users with different needs. RRAMEN is based on the concept of Relative Reachability, and its main goal is to aid individual users and urban planners in making informed choices and evaluating changes w.r.t. a city's transportation network. 

### 1. Examples & Usage

#### a. Individual Users

#### b. Urban Planners

### 2. Installation & Setup
#### a. Database
RRAMEN uses PostGIS to store road and public transportation network data. 
It requires PostgreSQL 10.10 or higher and PostGIS 2.5 or higher.
#### b. Datasets
In order to solve RR queries, RRAMEN requires the following datasets for a given region of interest.
##### OSM Map
RRAMEN uses OSM maps ([https://www.openstreetmap.org/](https://www.openstreetmap.org/)) in order to model a city's road network.
After downloading an OSM map for a given region, the `.osm` file must be placed in `data/osm/<region-name>/<osm-file>` under RRAMEN's root directory.
##### OSM Boundaries
In order to allow users to select neighborhoods from a city, RRAMEN requires `.json` files containing a hierarchy of boundaries that separates the given region into smaller areas. Such data can be manually downloaded from [https://wambachers-osm.website/boundaries/](https://wambachers-osm.website/boundaries/) or using the following command:
```
curl -f -o <region-name>.zip --url 'https://wambachers-osm.website/boundaries/exportBoundaries?cliVersion=1.0&cliKey=<client-key>&exportFormat=json&exportLayout=levels&exportAreas=land&from_al=2&to_al=12&union=false&selected=<region-code>'
```
##### General Transit Feed Specification (GTFS) data
[https://transitfeeds.com/](https://transitfeeds.com/)

### 3. Background and Research
