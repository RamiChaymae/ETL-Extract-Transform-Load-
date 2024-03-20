The central objective is to deepen our understanding of advanced concepts relating to the extraction and loading of data in parallel. The focus is on exploring the mechanisms for extracting data from various sources (Excel, Csv and SQLServer), using a threaded approach. These threads are distributed strategically, dividing the sources into segments determined by startIndex and endIndex values. These values are calculated as a function of a parameter called threadTrigger, which represents the quantity of records processed by each thread.
![image](https://github.com/RamiChaymae/ETL-Extract-Transform-Load-/assets/136628810/bf338cd1-8b85-49d9-9ab2-b0d8ade075df)

The threads are then used to perform transformations such as Column Transformation, Data Join, Sum of Amounts per user.

![image](https://github.com/RamiChaymae/ETL-Extract-Transform-Load-/assets/136628810/61ebb337-f553-4c90-9167-866977a26de7)

