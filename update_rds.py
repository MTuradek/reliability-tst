#!/usr/bin/env python

import sys
import os
import logging
import psycopg2
import boto3

DB_CONN_STRING = os.getenv('DB_CONN_STRING')

# S3 bucket name to use. It should exist and be accessible to your AWS credentials
S3_LEGACY_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'legacy-s3-mt1971')
S3_PRODUCTION_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'production-s3-mt1971')

# S3 connection details
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', 'https://s3.amazonaws.com/')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

OLD_PATH = 'image/'
NEW_PATH = 'avatar/'


def select_legacy_images_from_db(connection, path):
    cursor = None
    rows = []
    query = f"SELECT id,path FROM avatars WHERE path LIKE '{path}%' ORDER BY id ASC"

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error inserting to the database: {error}")
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        return rows


def copy_objects_between_buckets(source_bucket, destination_bucket, source_list, new_path):
    try:
        s3 = boto3.resource('s3', region_name=AWS_DEFAULT_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        for obj in source_list:
            dest_object = obj[1].split("/")[-1]
            copy_source = {'Bucket': source_bucket, 'Key': obj[1]}
            s3.meta.client.copy(copy_source, destination_bucket, new_path + dest_object)

    except Exception as error:
        logging.error(f"Error copying between S3 buckets: {error}")
        sys.exit(1)


def update_image_path_in_database(connection, source_list, new_path):
    cursor = None
    query = "UPDATE avatars SET path = %s WHERE id = %s"

    try:
        for obj in source_list:
            path = new_path + obj[1].split("/")[-1]
            path_id = obj[0]
            fields = (path, path_id)

            cursor = connection.cursor()
            cursor.execute(query, fields)
            connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error inserting to the database: {error}")
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


def delete_images_from_legacy_bucket(source_list):
    check = select_legacy_images_from_db(conn, OLD_PATH)

    if check:
        print("There are still images with old path in DB")
        sys.exit(2)
    else:
        print("Remove files from legacy-s3")
        try:
            s3 = boto3.resource('s3', region_name=AWS_DEFAULT_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

            for obj in source_list:
                s3.Object(S3_LEGACY_BUCKET_NAME, obj[1]).delete()

        except Exception as error:
            logging.error(f"Error copying between S3 buckets: {error}")
            sys.exit(1)


if __name__ == '__main__':

    # Connect to db
    try:
        conn = psycopg2.connect(DB_CONN_STRING)
    except Exception as e:
        logging.error(f"Error while connecting to the database: {e}")
        sys.exit(1)

    data = select_legacy_images_from_db(conn, OLD_PATH)
    copy_objects_between_buckets(S3_LEGACY_BUCKET_NAME, S3_PRODUCTION_BUCKET_NAME, data, NEW_PATH)
    update_image_path_in_database(conn, data, NEW_PATH)
    delete_images_from_legacy_bucket(data)

    conn.close()
