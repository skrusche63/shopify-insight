![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/shopify-insight/master/images/dr_kruscheundpartner_640.png)

## Shopifyinsight. 

Shopifyinsight is a customer science platform that leverages predictive analytics to achieve a deep customer understanding,
learn their preferences and forecast future behavior. It is based on the [Predictiveworks](http://predictiveworks.eu) and 
uses approved machine learning algorithms to recommend preferred products to customers, predict the next probable purchases, 
identify customers that look alike others, and more.

Shopifyinsight utilizes the approved RFM concept (see below) to segment customers by their business value, and combines this 
approach with modern data mining and predictive analytics.

 
> Shopifyinsight empowers e-commerce solution partners to add the *missing* customer perspective to modern e-commerce 
platforms, such as Magento, Demandware, Hybris, Shopify and others, and enables those platforms to target the right 
customer with the right product at the right time.

---

### Customer Science Process & Elasticsearch

Modern search engines are one of the most commonly used tools to interact with data volumes at any scale, and, Elasticsearch 
is one of the most powerful distributed and scalable member of these engines.

Shopifyinsight connects to a store platform, collects customer, product and order data on a weekly (or monthly basis), 
and loads these data into an Elasticsearch cluster.

Shopifyinsight then applies a well-defined customer science process to these e-commerce data, leveraging multiple processing 
phases. The results of this process, such as customer profiles, forecasts, product recommendations, or purchase frequencies 
are added to the Elasticsearch cluster, too, and extend the existing existing customer, product and purchase data.

Shopifyinsight uses Elasticsearch as the common serving layer for all kinds of data, makes them directly searchable and 
accessible by other search-driven applications through Elasticsearch's REST API.

And, with [Kibana](http://www.elasticsearch.org/overview/kibana/), the results of the customer science process can be 
directly visualized or time-based comparisons can be made with prior results.  
 

---

### Features

#### RFM Analysis

The concept of RFM has proven very effective when applied to marketing databases and describes a method 
used for analyzing the customer value.

RFM analysis depends on R(ecency), F(requency) and M(onetary) measures which are 3 important purchase 
related variables that influence the future purchase possibilities of customers.

*Recency* refers to the interval between the time, the last consuming behavior happens, and present. The importance 
of this parameters results from the experience that most recent purchasers are more likely to purchase again than 
less recent purchasers. 

*Frequency* is the number of transactions that a customer has made within a certain time window. This measure is used 
based on the experience that customers with more purchases are more likely to buy products than customers with fewer 
purchases. 

*Monetary* refers to the cumulative total of money spent by a particular customer. 

RFM provides a simple yet powerful framework for quantifying customer purchase behavior and is an excellent means to 
segment the customer base. Example: A customer has made a high number of purchases with high monetary value but not for 
a long time. At this situation something might have gone wrong, and marketers can contact with this customer to get feedback, 
or start a reactivation program.