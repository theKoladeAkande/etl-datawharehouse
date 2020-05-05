import os

#AWS
KEY = os.environ.get('KEY')
SECRET = os.environ.get('SECRET')

#DWH
DWH_DB = os.environ.get('DWH_DB')
DWH_DB_USER = os.environ.get('DWH_DB_USER')
DWH_DB_PASSWORD = os.environ.get('DWH_DB_PASSWORD')
DWH_PORT = os.environ.get('DWH_PORT')

DWH_ENDPOINT = os.environ.get('DWH_ENDPOINT')
DWH_CLUSTER_TYPE = os.environ.get('DWH_CLUSTER_TYPE')
DWH_NUM_NODES = os.environ.get('DWH_NUM_NODES')
DWH_NODE_TYPE = os.environ.get('DWH_NODE_TYPE')
DWH_IAM_ROLE_NAME = os.environ.get('DWH_IAM_ROLE_NAME')
DWH_CLUSTER_IDENTIFIER = os.environ.get('DWH_CLUSTER_IDENTIFIER')
DWH_ROLE_ARN = os.environ.get("DWH_ROLE_ARN")

#S3
SPARKIFY_BUCKET = 'udacity-dend'
SPARKIFY_SONG_DATA = 's3://udacity-dend/song_data'
SPARKIFY_LOG_JSONPATH = 's3://udacity-dend/log_json_path.json'
SPARKIFY_LOG_DATA = 's3://udacity-dend/log_data'