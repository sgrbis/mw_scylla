#imports
import dash
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext
import dash_core_components as dcc
import dash_html_components as html
from pyspark.sql.functions import col
import pyspark
import pyspark.sql.functions as f
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
sc = pyspark.SparkContext()
sql = SQLContext(sc)
dataFrame = sql.read.format("org.apache.spark.sql.cassandra").options(table="similarity_table", keyspace="tt").load() #reading from scylla
df1 = dataFrame
df1 = df1.groupBy('app_name').count().select('app_name', f.col('count').alias('count')) #grouping by apps and counting
df1 = df1.sort(col("count").desc()) #sorting in descending order
df1 = df1.limit(5) #get only 5
df = df1.toPandas() #conversion to pandas
df.head(3)
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

list1 = df.app_name.unique()
list2 = []
sum = 0
for i in list1:
        if (i == df['app_name']).any():
                sum = sum + 1
                list2.append(df['count'])
print(list2)
print(list1)

sc.stop()

app.layout = html.Div(children=[
    html.H1(children='Scylla Visualization'),

    html.Div(children='''
         From similarity_table.
    '''),
    dcc.Graph(
	id='example-graph',
        figure={
            'data': [
                {'x': list1, 'y': list2, 'type': 'bar'},
            ],
            'layout': {
                'title': 'Number of users for top 5 apps'
            }
	}
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)


#runs on port 8050, ssh and bind localhost
