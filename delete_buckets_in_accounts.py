# Get list of accounts in Organization
#   Assume given role in each account
#   Get collection of buckets starting with PARAMETER
#     Delete bucket

import sys
import json
# from urllib.parse import urlparse
import boto3
from botocore.exceptions import ClientError

# Globals
global_sts_client = boto3.client('sts')
ENV_ASSUMED_ROLE_NAME = 'AWSCloudFormationStackSetExecutionRole'


class BucketCleanUp(object):

  #===============================================================================
  # Constructor
  #===============================================================================
  def __init__(self, bucket_prefix):

    self._bucket_prefix = bucket_prefix

    self.accounts = self.get_accounts()


  #===============================================================================
  # function: get_accounts
  #===============================================================================
  def get_accounts(self):

    accounts = []

    for account in boto3.client('organizations').list_accounts()['Accounts']:

      #Just in case...
      if account['Status'] != 'ACTIVE':
        print('Ignoring account {}, which is {}'.format(account['Id'], account['Status']))

      else:

        accounts.append(account['Id'])

    return accounts


  #===============================================================================
  # function: get_credentials_for_assumed_role
  #===============================================================================
  def _get_credentials_for_assumed_role(self, account_id):

    role_arn = 'arn:aws:iam::' + account_id + ':role/' + ENV_ASSUMED_ROLE_NAME

    # Get the temporary STS credentials from the consolidated account.
    response = global_sts_client.assume_role(
      RoleArn = role_arn,
      RoleSessionName = 'delete-buckets'
    )

    credentials = response['Credentials']

    return credentials


  #===============================================================================
  # function: delete_buckets
  #===============================================================================
  def delete_buckets(self):

    for account_id in self.accounts:

      print('\nIn account: {0}'.format(account_id))

      credentials = self._get_credentials_for_assumed_role(account_id)

      # Get an S3 client using the assumed role.
      s3_client = boto3.client('s3',
          aws_access_key_id = credentials['AccessKeyId'],
          aws_secret_access_key = credentials['SecretAccessKey'],
          aws_session_token = credentials['SessionToken'],
      )

      for bucket in s3_client.list_buckets()['Buckets']:
        bucket_name = bucket['Name']
        if bucket_name.startswith(self._bucket_prefix):
          # print('\t{0}'.format(bucket_name))

          try:
            # Delete the bucket objects.
            self._empty_versioned_bucket(bucket_name, s3_client)

            try:
              # Delete the bucket.
              s3_client.delete_bucket(Bucket=bucket_name)
              print('\tDeleted bucket: {0}'.format(bucket_name))

            except ClientError as e:
              print('\tCOULD NOT delete bucket {0}: {1}'.format(bucket_name, e))

          except ClientError as e:
            print('\tCOULD NOT delete objects in bucket {0}: {1}'.format(bucket_name,e))




  def _empty_versioned_bucket(self, bucket, s3_client):
      ''' Deletes all objects in a versioned bucket

      :param bucket: Bucket name
      :type bucket: string
      '''
      # logger.debug('calling empty_versioned_bucket')

      paginator = s3_client.get_paginator('list_object_versions')

      delete = {'Objects': []}

      try:
        for result in paginator.paginate(Bucket=bucket):
            for version_type in ('Versions', 'DeleteMarkers'):
                if not result.get(version_type):
                    continue

                # logger.debug(
                #     'version type: "%s" contains a total of "%s" objects',
                #     version_type,
                #     len(result.get(version_type))
                # )

                for version in result.get(version_type):
                    delete['Objects'].append(
                        dict(
                            Key=version.get('Key'),
                            VersionId=version.get('VersionId')
                        )
                    )

            delete_len = len(delete['Objects'])

            if not delete_len:
                continue

            # logger.info('attempting to delete a total of %s objects', delete_len)

            try:
                response = s3_client.delete_objects(
                    Bucket=bucket,
                    Delete=delete
                )
            except:
                raise

            # if response.get('Deleted'):
            #     logger.info(
            #         'deleted a total of %s objects',
            #         len(response['Deleted'])
            #     )
            # else:
            #     logger.warn('response did not include "Deleted" items')

      except:
        pass



#===============================================================================
# Main function
#===============================================================================
if __name__ == '__main__':

  # Read the bucket prefix parameter.
  bucket_prefix = sys.argv[1]

  # Get a reference to the cleaner class.
  cleaner = BucketCleanUp(bucket_prefix)

  cleaner.delete_buckets()

