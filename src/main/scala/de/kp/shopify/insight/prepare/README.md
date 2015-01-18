
## Prepare Subsystem

### Customer Analysis

#### Customer Segmentation

##### RFM Analysis (RFM)

Customer RFM segmentation is a customer data preparation job that is based on the RFM model. 
RFM is short for R(ecency), F(requency) and M(onetary) and is an approved approach to build 
customer types from their purchase behavior.

##### Loyalty Analysis (CLS)

Customer loyalty segmentation is a customer data preparation job that is based on the 
results of the customer RFM segmentation. 

Customer loyalty segmentation evaluates the (repeat) purchase behavior, assigns a loyalty 
descriptor to every individual customer and joins the result with the RFM segmentation result.

Loyalty Analysis e.g. shows when specific customers fade off their usual purchasing pattern.


*How it works*

Customer loyalty segments are built from the customers' repeat purchase behavior. The amount
spent in the last purchase transaction and the recency of this transaction is compared to the
individual repeat purchase behavior of every customer.

The results for the last amount and recency are rated with scores from 1 to 4 with respect to 
the "normal" repeat purchase behavior of the customer. These ratings are then mapped onto a 
customer loyalty descriptor (1 to 4) and finally assigned to the customer type specification 
derived from the RFM segmentation.

---

#### Customer Affinity

#### Purchase Time Affinity

This data preparation job evaluates the purchase history of every single customer and computes 
a purchase frequency distribution with respect to the day of the week and the hour of the day,
purchases are performed.

These time-based distribution are leveraged to create persona that specify customers with 
similar purchase time preferences.

##### Customer Day Affinity (CDA)

##### Customer Hour Affinity (CHA)

#### Customer Recency / Repeat Affinity (CSA)

This data preparation job evaluates the purchase history of every single customer and computes
the time-to-repeat purchase distribution.


#### Product Affinity (CPA)

A data preparation job that the purchase frequency distribution for every single customer and
applies the TF-IDF mechanism (from text analysis) to build a customer product preference profile.

These product preference profiles are leveraged to create personas that specify customers with 
similar preference profiles.

The customer product preference profiles are also used to apply collaborating filtering and compute
a predefined number of products that can be recommended to every single customer. 

---

### Customer Movement (LOC)

---

### Customer Purchase States (CPS)

A data preparation job to build a purchase process model. This model is then used to forecast
the most probable next purchases for every single customer.

---

### Product Analysis

#### Product Relations (ASR)

This data preparation job evaluates the purchase history for a certain period of time and 
prepares to discover those products that are often bought together.


#### Product Segmentation (PPF)

This data preparation job leverages a quantile-based mechanism to segment products with respect 
to their customer purchase frequency. This job measures both, the customer frequency of a certain
product and also the purchase frequency.

This enables to also to products into account that are frequently bought only by few customers.

---

### Purchase Overview Metric (POM)



