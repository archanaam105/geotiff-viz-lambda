import json
import boto3
from osgeo import gdal

region = "eu-west-2"
client = boto3.client('cognito-identity',region)   


def lambda_handler(event, context):

    bucketName = event['bucket']
    prefix = 'upload/'
    vrt_path = '/tmp/merged.vrt'
    compressed_path = '/tmp/compressed.tif'
    cog_path = '/tmp/final_geotiff.tif'
    fin_object_key= 'merged/final_geotiff.tif'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketName)
    uploaded_objs = bucket.objects.filter(Prefix=prefix)
    gdal_path = f"/vsis3/{bucketName}/"
    dtm_list = []
    for obj in uploaded_objs:
        if(obj.key not in prefix):
            dtm = gdal.Open(gdal_path+obj.key)
            dtm_list.append(dtm)
            dtm = None
    vrt = gdal.BuildVRT(vrt_path, dtm_list)
    gdal.Translate(compressed_path,vrt, xRes=6.0,yRes=6.0)
    gdal.Warp(cog_path,compressed_path,dstSRS='EPSG:4326')
    response = s3.meta.client.upload_file(cog_path, bucketName, fin_object_key)
    object_url = f'https://{bucketName}.s3.{region}.amazonaws.com/{fin_object_key}'
    return object_url

    
def authenticate():
    identity_pool_id = "eu-west-2:ecff2e4f-735b-4113-8e91-7884c77fccc5"
    response = client.get_id(IdentityPoolId=identity_pool_id)
    identity_id = response['IdentityId']
    res = client.get_credentials_for_identity(IdentityId=identity_id)
    return res