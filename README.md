![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/shopify-insight/master/images/dr_kruscheundpartner_640.png)

## Shopifyinsight. 

Shopifyinsight. provides a bridge between Shopify and Predictiveworks. and makes it easy for Shopify customers to apply 
MarketBasket Analysis to their orders, compute Product Recommendations and more.

![Shopifyinsight.](https://raw.github.com/skrusche63/shopify-insight/master/images/shopify_access_640.png)

### REST API

The REST API of Shopifyinsights. actually supports the following requests:

#### POST /feed/order

Collect orders from a specific Shopify store and index into an Elasticsearch index. Orders of a 
certain time period form the base of market basket analysis.

#### POST /feed/product

Collect product data from a specific Shopify store.

#### POST /train/{engine}

Start a certain predictive engine from Predictiveworks. and perform a data mining or predictive 
analytics task.

#### POST /status/{engine}

Retrieve the current status of a data mining or predictive analytics task from a certain predictive engine.

#### POST /get/{engine}/{subject}

Retrieve a specific mining result or prediction, specified by *subject* from  a certain predictive engine.

