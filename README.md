**# Youtube-Data-Harvesting**
 This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.
1. Tools Installed
  Visual Studio code
  Jupyter notebook
  Python 3.11.3 or higher
  MySQL
  MongoDB
  Youtube API key
2. ETL Process
a) Extracting the data
Extract the particular youtube channel data by using the youtube channel id, with the help of the youtube API developer console.
b) Transforming the data
After the extraction process, take the required data from the extraction and transform it into JSON format.
c) Loading the data
After the transformation process, the JSON format data is stored in the MongoDB database, also it has the option to migrate the data to MySQL database from the MongoDB database.
d) EDA Process and Framework
EDA, or Exploratory Data Analysis, is a crucial step in the data analysis process that involves summarizing, visualizing, and understanding the main characteristics of a dataset.
  a) Access MySQL DB
  Create a connection to the MySQL server and access the specified MySQL DataBase by using pymysql library and access tables.
  b) Filter the data
  Filter and process the collected data from the tables depending on the given requirements by using SQL queries and transform the processed data into a DataFrame format.
  c) Data Visualization
  Finally, create a Dashboard by using Streamlit and give dropdown options on the Dashboard to the user and select a question from that menu to analyse the data and show the output in Dataframe Table.
