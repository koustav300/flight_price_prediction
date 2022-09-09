import pymysql
from sqlalchemy import create_engine
import pymongo

# creating mysql connection
# Credentials to database connection
aws_endpoint = 'yt-scrapper.co2ntnc4f72p.us-east-1.rds.amazonaws.com'
hostname = aws_endpoint
dbname = 'yt_scrape'
uname = "admin"
pwd = "mds268ds"


def create_sql_engine():
    # Create SQLAlchemy engine to connect to MySQL Database
    try:
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                               .format(host=hostname, db=dbname, user=uname, pw=pwd))
    except Exception as e:
        print('Err --  ' + e)
    return engine

def create_mongodb_conn():
    # creating connection with mongodb
    client = pymongo.MongoClient(
                "mongodb+srv://koustav300:kdeysonai@cluster0.o9pog9p.mongodb.net/?retryWrites=true&w=majority")

    return client

def create_pysql_connction():
    # Connect to the database
    connection = pymysql.connect(host=aws_endpoint,
                                 user=uname,
                                 password=pwd,
                                 db=dbname)
    return connection