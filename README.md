# citibike-dashboard
Capstone for CUNY SPS Data Science Masters

## Mission Statement

In this capstone project for my masters in Data Science, I will be processing data regarding New York City's Citi Bike bike share program. I will utilize historic data to make predictions on the number of future riders as well as make a number of business intelligence decisions based on my discoveries. Ultimately, I will create a live dashboard displaying all of this information

### Phase 1 - Model Creation

#### Step 1 - Data Ingestion and Cleaning

I pulled all the publicly available Citi Bike data containing over 50 million rows on each ride taken since 2013. I combined this data with information on historic weather data. The data was cleaned, uploaded to GCP where aggregate queries could be performed using Big Query. 

Ultimately, I created a ~40k line csv file containing information on every hour from the program's inception until the end of 2017.

Challenges included cleaning the data (data was missing or inconsistently formatted), ingesting it into GCP in a timely fashion and combining the ride data with the wather data.

#### Step 2 - EDA

Using jupyter notebook to run both Python and R, I began to explore the predictors I had collected in the previous step. My visualization tool of choice for creating presentation ready plots is ggplot. Initially however, I will plot in seaborn and matplotlib for initial exploration. Following this exploration, I create more robust visualizations with ggplot. 

These visualizations may find their way into my final report, but also serve to aid me in the next steps of the project. The notes before each plot also help for me to summarize my thoughts and findings, although they are hardly exhaustive.

#### Step 3 - Machine Learning

I created a variety of ML models, ultimately narrowing the work down to the strongest model of each type. The XGBoost model was able to create highly accurate results that are certainly usable. However, I want to do some additional exploration with neural networks to see if they can provide further predictive ability.

#### Step 4 - Deep Learning

After iterating through a number of models, I settled on a predictive model that utilizes a GRU series to analyze the previous weeks actual values and then combines it with a dense network representing the given hours meta information to make a prediction. This model ends up being only slightly better than the XGBoost model.

#### Step 5 - Ensemble Model

As a final step in the creation of my model, I tested an ensemble that works by averaging the predictions of the XGBoost model and the nueral net. The results are incredibly promising as there is a roughly 25% improvement in predictions using the ensemble than either model individually. 

I created a variety of plots to demonstrate the predictions on the validation data.

### Phase 2 - Dashboard

#### Step 1 - Simulated Data Streaming

I will be using Google Cloud Platform to simulate the streaming Citi Bike data. I created a small web app with cron jobs that will read "current" bike and weather data and send it into a Pub/Sub queue. This data uses 2018 historical data as if it were 2019 data and streams it in in near real time. 

#### Step 2 - Dataflow with Apache Beam

Next, I will write a Java program using Apach Beam to ingest and process the Pub/Sub data and perform ETL to prepare it for use in the dashboard. TDC.





