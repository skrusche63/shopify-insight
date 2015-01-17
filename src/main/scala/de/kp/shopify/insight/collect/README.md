
## Collect Subsystem

The *collect* subsystem connects to a store platforms (Shopify) leveraging the respective 
REST API and collects customer, product and purchase data on a weekly or other predefined
time base.

These data are used to create or update Elasticsearch indexes for customers, products and 
purchases.

The *collect* phase defines the starting point of a set of data processing phases that aim 
to evaluate the customers' purchase history to extract valuable business insights, provide 
product recommendations, or, forecast customer lifetime value and next purchase, and more.

Having stored customer, product and purchase data in an Elasticsearch index makes these data 
directly searchable, and accessible by other search-based applications leveraging the REST API
of Elasticsearch. 

 