
## Data Preparation Subsystem

### Customer RFM Segmentation (RFM)

Customer RFM segmentation is a customer data preparation job that is based on the RFM model. 
RFM is short for R(ecency), F(requency) and M(onetary) and is an approved approach to build 
customer types from the purchase behavior of a certain period of time.

### Customer Loyalty Segmentation (CLS)

Customer loyalty segmentation is a customer data preparation job that is based on the 
results of the customer RFM segmentation. 

Customer loyalty segmentation evaluates the (repeat) purchase behavior, assigns a loyalty 
descriptor to every individual customer and joins the result with the RFM segmentation result.

*How it works*

Customer loyalty segments are built from the customers' repeat purchase behavior. The amount
spent in the last purchase transaction and the recency of this transaction is compared to the
individual repeat purchase behavior of every customer.

The results for the last amount and recency are rated with scores from 1 to 4 with respect to 
the "normal" repeat purchase behavior of the customer. These ratings are then mapped onto a 
customer loyalty descriptor (1 to 4) and finally assigned to the customer type specification 
derived from the RFM segmentation.

### Customer Day Affinity (CDA)

### Customer Hour Affinity (CHA)

### Customer Movement (LOC)

### Customer Product Affinity (CPA)

### Customer Timespan Affinity (CSA)

### Product Affinity (ASR)

### Product Purchase Frequency (PPF)

### Purchase Overview Metric (POM)

### State Transition Matrix (STM)



