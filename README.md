# weather_data_analysis
                       
NOAA generates tens of terabytes of data a day from satellites, radars, ships, weather models, and other sources. While these data are publicly available, it is difficult to download and work with such high volumes. NOAA’s vast wealth of data therefore represents a substantial untapped economic opportunity. Our Big Data Project utilizes 30GBS of data set.  The main purpose of this project is to become familiar withHadoop system and data analytics with MapReduce and Spark programming.The main purpose of this projectis to become familiar withHadoop system and data analytics with MapReduce and Spark programming and also gain experience with research on Big data and data Analytics. 

# Data Set 
   Our dataset consists of weather event details, Storm event details, Weather structure and Weather warnings. They all contained different important information like Event ID, Start and End time of different events, type of events etc. Our focus was on Injuries and Death rate per state whether direct or indirect.

# MapReduce 
   We implemented both the mapper and reducer in one big class. Data mapper runs the map functions but extends from the Hadoop package. We extended the mapper class with our own instruction for handling various input in specific manner. During map master computer instructs worker computers to process local input data and Hadoop performs shuffle process. Thus, master computer collects the results from all reducers and compliers to answer overall query. 

# Hive 
   Our hive analysis was implemented through Google Cloud Platform. We uploaded our data set to Google Cloud storage buckets. Created our tables and queries through Google Cloud Secure Socket Shell. Hive Bridges the gap between low-level java programming for Hadoop and SQL also, leverages Hadoop supports partitioning for scalability and performance.

# Spark 
     We also did our spark analytics with python programming language. We wrote some python scripts to call the storage clusters from google cloud to our local machine. We used pandas in python, a software library written for the Python programming language for data manipulation and analysis. In particular, it offers data structures and operations for manipulating numerical tables and time series. 
