
### Customer Loyalty Profile (CLS)

##### How it works

The *prepare* subsystem evaluates the purchase history of every individual user and determines
how far the latest purchase transaction differs from the previous history with respect to the
amount spent and the recency of the event.

These data are used to map every customer into one of 4 predefined loyalty segments, from *churner* 
and *vulnerable* up to *neutral* and *loyal*. Then, the customer's loyalty segment is combined with
the overall customer type (see RFM segmentation). 

The preparer stores the loyalty data as a Parquet file on the file system and enables other 
applications to leverage the data.

The *loader* subsystem extracts the Parquet file, transforms the content into JSON documents and
indexes these documents in an Elasticsearch index.

The result is a time series of loyalty data for every single customer.

##### Usage

The loader results can directly be used to visualize loyalty trajectories, or, customers can be 
aggregated by the customer type and loyalty state to support e.g. customer targeting.

---

### Customer Location Profile (LOC)

##### How it works

The *prepare* subsystem evaluates the IP addresses provided with each purchase a customer made.

From this address, geospatial data such as country, region, city and the LATLON coordinates are
determined. The **GeoLite** database is used for this data preparation step.

The preparer stores the geospatial data as a Parquet file on the file system and enables other 
applications to leverage the data.

The *loader* subsystem extracts the Parquet file, transforms the content into JSON documents and
indexes these documents in an Elasticsearch index, using the geospatial support of Elasticsearch.

The result is a time series of geospatial data for every single customer.

##### Usage

The loader results can directly be used to visualize the geospatial purchase behavior of every individual
customer on a geospatial map, or, customers can be aggregated by their countries, regions or cities.

---

### Customer Purchase Forecast (CPF)

##### How it works

The *enrich* subsystem evaluates the customer purchase process model for every single customer 
and determines the most likely next purchase amounts and times, n steps ahead. It also specifies
the most probable next recency & monetary states segments the customer can be assigned to. 

The enricher stores the purchase forecast data as a Parquet file on the file system and enables other 
applications to leverage the data.

The *loader* subsystem extracts the Parquet file, transforms the content into JSON documents and
indexes these documents in an Elasticsearch index.

The result is a time series of purchase forecast data for every single customer.

##### Usage

The loader results can directly be used to visualize the purchase forecast data of every individual
customer, or, customers can be aggregated e.g. by their next most probable purchase times to feed 
targeting campaigns.

---

### Customer Segment Profile (RFM)

---

### Purchase Overview Metric (POM)

---

### Purchase Frequency Profile (PPF)

---

### Product Relation Model (PRM)

