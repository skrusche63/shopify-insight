
## Elasticsearch Predictive Plugin

The final results of the multiple data processing phases are indexed in appropriate Elasticsearch 
indexes, and extend the base indexes that hold synchronized store data for the customer & product 
base, and the registered purchase orders.

Due to this approach, all insights discovered from the customers' purchase behavior are searchable 
and can be directly transformed into valuable actions by suitable search-driven applications.

And, as all data derived from applying analytics and predictive analysis, are equipped with a timestamp, 
it is easy to build and visualize time series from these data, or compare insights that have different 
timestamps.

In addition to this functionality that is empowered by Elasticsearch, this platform also provides a plugin 
for Elasticsearch (Predictive Plugin) to support product recommendations, and the retrieval of time series 
data.

### Product Recommendations

The predictive plugin supports the following recommendations:

---

#### Recommended for you

Recommended products are computed from the customer's preference profile by taking most similar users
and their preferences into account.  

---

#### More like this

This recommendation provides products that are similar to a certain product.

---

#### Bought also bought

This kind of recommendation provides products that are related to a certain product or a set of 
products. 

---

#### Top selling products

Topic selling products do not refer to a certain customer, and are retrieved from product purchase frequencies.

These frequencies exist for two different contexts: one is independent of a certain customer segment 
and retrieves the top sellers from the **purchase metrics**, based on the purchase data of a certain time 
window, and derived in the data preparation phase (see Prepare Subsystem).

The other context takes a certain customer segment (e.g. frequent buyer) into account and retrieves the 
top sellers from the **product purchase frequency** of the respective segment. By using product frequencies
in this context, the recommendation request must specify a certain customer type (or segment).

This information can be retrieved from multiple mappings within the *customer* index. An appropriate one, that 
is recommended here, is the *segments* mapping, i.e ```customers/segments```. 

---

### Time Series Data



