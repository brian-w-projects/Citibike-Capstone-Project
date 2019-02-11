# citibike-dashboard
Capstone for CUNY SPS Data Science Masters

## Mission Statement

In this capstone project for my masters in Data Science, I will be processing data regarding New York City's Citi Bike bike share program. I will utilize historic data to make predictions on the number of future riders as well as make a number of business intelligence decisions based on my discoveries. Ultimately, I will create a live dashboard displaying all of this information

### Step 1 - Data Ingestion and Cleaning

I pulled all the publicly available Citi Bike data containing over 50 million rows on each ride taken since 2013. I combined this data with information on historic weather data. The data was cleaned, uploaded to GCP where aggregate queries could be performed using Big Query. 

Ultimately, I created a ~40k line csv file containing information on every hour from the program's inception until the end of 2017.

Challenges included cleaning the data (data was missing or inconsistently formatted), ingesting it into GCP in a timely fashion and combining the ride data with the wather data.

### Step 2 - EDA

Using jupyter notebook to run both Python and R, I began to explore the predictors I had collected in the previous step. My visualization tool of choice for creating presentation ready plots is ggplot. Initially however, I will plot in seaborn and matplotlib for initial exploration. Following this exploration, I create more robust visualizations with ggplot. 

These visualizations may find their way into my final report, but also serve to aid me in the next steps of the project. The notes before each plot also help for me to summarize my thoughts and findings, although they are hardly exhaustive.

### Step 3 - Machine Learning