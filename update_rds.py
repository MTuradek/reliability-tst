#!/usr/bin/env python

import sys
import os
import logging
import psycopg2
import boto3

DB_CONN_STRING = os.getenv('DB_CONN_STRING')

# S3 bucket name to use. It should exist and be accessible to your AWS credentials
S3_LEGACY_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'legacy-s3-test-2021')
S3_PRODUCTION_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'production-s3-test-2021')

# S3 connection details
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', 'https://s3.amazonaws.com/')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# s3 object path
OLD_PATH = 'image/'
NEW_PATH = 'avatar/'


def select_legacy_images_from_db(db_conn, path):
    """ Data gathering: Creates list of avatars with legacy path which have to be updated """
    cursor = None
    rows = []
    query = f"SELECT id,path FROM avatars WHERE path LIKE '{path}%' ORDER BY id ASC"

    try:
        cursor = db_conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error fetching images from the database: {error}")
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        return rows


def copy_objects_between_buckets(s3_conn, source_bucket, destination_bucket, source_list, new_path):
    """ Avatars move: Copies images from legacy s3 bucket to production s3 bucket with modified path """
    try:
        for obj in source_list:
            dest_object = obj[1].split("/")[-1]  # extract avatar filename
            copy_source = {'Bucket': source_bucket, 'Key': obj[1]}
            s3_conn.meta.client.copy(copy_source, destination_bucket, new_path + dest_object)

    except Exception as error:
        logging.error(f"Error copying between S3 buckets: {error}")
        sys.exit(1)


def update_image_path_in_database(db_conn, source_list, new_path):
    """ DB path update: Updates old path to the new one. All images have been copied to the production s3 bucket,
        so there is not issue with invalid avatar path """
    cursor = None
    query = "UPDATE avatars SET path = %s WHERE id = %s"

    try:
        for obj in source_list:
            path = new_path + obj[1].split("/")[-1]  # extracts avatar name and add the new path
            path_id = obj[0]
            fields = (path, path_id)

            cursor = db_conn.cursor()
            cursor.execute(query, fields)
            db_conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error updating the database: {error}")
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


def delete_images_from_legacy_bucket(s3_conn, source_list):
    """ Delete avatars from legacy s3 bucket: Checks if there are and legacy paths in database and if not,
        deletes all avatars within particular path from legacy s3 bucket """
    check = select_legacy_images_from_db(conn, OLD_PATH)

    if check:
        print(f"There are still images with {OLD_PATH} in database")
        sys.exit(2)
    else:
        print(f"Removing files from s3 bucket: {OLD_PATH}")
        try:
            for obj in source_list:
                s3_conn.Object(S3_LEGACY_BUCKET_NAME, obj[1]).delete()

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

    # Initialize s3 resource
    try:
        s3 = boto3.resource('s3',
                            endpoint_url=S3_ENDPOINT_URL,
                            aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            region_name=AWS_DEFAULT_REGION
                            )
    except Exception as e:
        logging.error(f"Error while connecting to S3: {e}")
        sys.exit(1)

    # path updating
    data = select_legacy_images_from_db(db_conn=conn, path=OLD_PATH)
    copy_objects_between_buckets(s3_conn=s3, source_bucket=S3_LEGACY_BUCKET_NAME,
                                 destination_bucket=S3_PRODUCTION_BUCKET_NAME, source_list=data, new_path=NEW_PATH)
    update_image_path_in_database(db_conn=conn, source_list=data, new_path=NEW_PATH)
    delete_images_from_legacy_bucket(s3_conn=s3, source_list=data)

    conn.close()
