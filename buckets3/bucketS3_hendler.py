import boto3
import glob
import os
import io 
import s3fs
import pandas as pd

access_key = os.environ["ACCESS_KEY"] 
secret_access_key = os.environ["SECRET_ACCESS_KEY"]
bucket_name = os.environ["BUCKET_NAME"]
client = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
resource = boto3.resource("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

# get file object directly from s3
def get_file_object_from_s3():
    fs = s3fs.S3FileSystem(key=access_key, secret=secret_access_key)
    return fs


def upload_folder(path_local_folder, path_folder_in_bucket, bucket_name = bucket_name):
    for file in glob.glob(path_local_folder + "/*"):
        upload_key = path_folder_in_bucket + str(file.split("\\")[-1])
        client.upload_file(file, bucket_name, upload_key)


def extract_name_from_path(path_to_file):
    path_list = path_to_file.split("/")
    if len(path_list) > 1:
        return str(path_list[-1])
    return path_to_file.split("\\")[-1]


def upload_file(path_to_file, key, bucket_name = bucket_name):
    file_name = extract_name_from_path(path_to_file)
    upload_key = key + file_name
    client.upload_file(path_to_file, bucket_name, upload_key)


# def list_bucket(bucket_name):
#     response = client.list_objects(Bucket=bucket_name)
#     return json.dumps(response, indent=4, sort_keys=True, default=json_util.default)


def list_files_in_dir(remote_directory_name, bucket_name = bucket_name):
    bucke = resource.Bucket(bucket_name)
    list_files = []
    for obj in bucke.objects.filter(Prefix=remote_directory_name):
        list_files.append(obj.key)
    return list_files


# delete all files in bucket with the prefix, if there are non, do nothing
def delete_dir_from_bucket(dir_path, bucket_name = bucket_name):
    bucket = resource.Bucket(bucket_name)
    bucket.objects.filter(Prefix=dir_path).delete()


def delete_file_from_bucket(key,bucket_name = bucket_name):
    resource.Object(bucket_name, key).delete()


def download_from_s3(model_path, local_model_path, bucket_name=bucket_name):
    bucket = resource.Bucket(bucket_name)
    bucket.download_file(model_path, local_model_path) # save to local path
    return local_model_path


# read file content directly from s3
def get_file_content_from_s3(file_key, bucket_name = bucket_name):
    obj = client.get_object(Bucket= bucket_name, Key= file_key) 
    return obj['Body']
  

# read file content directly from s3 to string value
def get_string_obo_file_from_s3(file_key, bucket_name = bucket_name):
    bytes_buffer = io.BytesIO()
    client.download_fileobj(Bucket=bucket_name, Key=file_key, Fileobj=bytes_buffer)
    byte_value = bytes_buffer.getvalue()
    str_value = byte_value.decode() #python3, default decoding is utf-8
    return str_value

  
def read_xlsx_from_s3(file_key, bucket = bucket_name, usecols = None):
    body = get_file_content_from_s3(file_key ,bucket_name)
    xlsx_string = body.read()
    df_from_s3 = pd.read_excel(io.BytesIO(xlsx_string), usecols= usecols)
    return df_from_s3
