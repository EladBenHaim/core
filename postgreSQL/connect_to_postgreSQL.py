import boto3
import os, sys, json
import psycopg2
from core.connect_to_neo4j import get_aws_secret_value


def get_connection():
    secret_name = "drugdb-postgres-user"
    secret_region_name = "eu-central-1"
    postgres_hostname = "drug.c7mtkwk59wbs.eu-central-1.rds.amazonaws.com"
    postgres_port = "5432"
    postgres_db = 'probes-drugs'
    postgres_user, postgres_password = get_aws_secret_value(secret_name, secret_region_name)
    return psycopg2.connect(user=postgres_user, password=postgres_password, host=postgres_hostname,
                            port=postgres_port, database=postgres_db)

