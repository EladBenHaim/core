import boto3
import os, sys, json
import socket
from py2neo import Graph
import requests

def get_aws_secret_value(secret_name,secret_region_name):
    try:
        session = boto3.session.Session(aws_access_key_id=os.environ['ACCESS_KEY'],aws_secret_access_key=os.environ['SECRET_ACCESS_KEY'])
        client = session.client(service_name='secretsmanager',region_name=secret_region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret_value = json.loads(get_secret_value_response['SecretString'])
        return secret_value['username'], secret_value['password']
    except Exception as e:
        print('Error in get_aws_secret_value fucntion: ' + str(e))
        sys.exit(1)

# this function gets hosts list and return the not busy host
def get_neo4j_leader(neo4j_lb,db_name,user,password,port=7474):
    try:
        neo4j_hosts_list = socket.gethostbyname_ex(neo4j_lb)[2]
        for host in neo4j_hosts_list:
            r = requests.get(f'http://{host}:7474/db/{db_name}/cluster/writable', auth=(user, password))
            if r.status_code == 200:
                return host
        print("Error: can't find a leader host")
        return None
    except Exception as e:
        print('Error in get_neo4j_leader fucntion: ' + str(e))
        sys.exit(1)

# Connect to neo4j platform
# param neo4j_name: dbms name (dev, prod, neo4j etc)
# param secret_name: get user and password feom aws secret, default neo4j secret
# param ip_address: ip address, default false
def get_graph(neo4j_name,secret_name='neo4j',ip_address=False):
    secret_region_name = "eu-central-1"
    neo4j_port = "7687"
    if 'NEO4J_LB' in os.environ:
        neo4j_lb = os.environ['NEO4J_LB']
    else:
        neo4j_lb = "neo4j-prod.infra.local"
    neo4j_user, neo4j_password = get_aws_secret_value(secret_name, secret_region_name)
    if not ip_address:
        neo4j_hostname =get_neo4j_leader(neo4j_lb,neo4j_name,neo4j_user,neo4j_password)
    elif ip_address:
        neo4j_hostname = ip_address
    neo4j_connection_url = "bolt://{server}:{port}".format(server=neo4j_hostname, port=neo4j_port)
    return Graph(neo4j_connection_url, auth=(neo4j_user, neo4j_password), name=neo4j_name)