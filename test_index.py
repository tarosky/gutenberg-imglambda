import datetime
import json
import os
import secrets
from typing import Any, Dict, List, Optional

from aiobotocore import session

from index import ImgServer

PERM_RESP_MAX_AGE = 365 * 24 * 60 * 60
TEMP_RESP_MAX_AGE = 20 * 60
S3_PREFIX = 'prefix'
REGION = 'ap-northeast-1'

CSS_MIME = 'text/css'
GIF_MIME = "image/gif"
JPEG_MIME = 'image/jpeg'
JS_MIME = "text/javascript"
PNG_MIME = 'image/png'
SOURCE_MAP_MIME = 'application/octet-stream'
WEBP_MIME = "image/webp"


def read_test_config(name: str) -> str:
  path = f'{os.getcwd()}/config/test/{name}'
  with open(path, 'r') as f:
    return f.read().strip()


def generate_safe_random_string() -> str:
  return secrets.token_urlsafe(256 // 8)


async def create_img_server(name: str) -> ImgServer:
  account_id = read_test_config('aws-account-id')
  sqs_name = f'test-{name}-{generate_safe_random_string()}'
  access_key_id = read_test_config('access-key-id')
  secret_access_key = read_test_config('secret-access-key')

  sess = session.get_session()
  sqs = await sess.create_client(
      'sqs',
      region_name=REGION,
      aws_access_key_id=access_key_id,
      aws_secret_access_key=secret_access_key).__aenter__()
  s3 = await sess.create_client(
      's3',
      region_name=REGION,
      aws_access_key_id=access_key_id,
      aws_secret_access_key=secret_access_key).__aenter__()

  return ImgServer(
      region=REGION,
      sqs=sqs,
      s3=s3,
      generated_domain=f"{read_test_config('s3-bucket')}.s3.example.com",
      original_bucket=read_test_config('public-content-bucket'),
      generated_key_prefix=S3_PREFIX,
      sqs_queue_url=(
          f'https://sqs.{REGION}.amazonaws.com/{account_id}/{sqs_name}'),
      perm_resp_max_age=PERM_RESP_MAX_AGE,
      temp_resp_max_age=TEMP_RESP_MAX_AGE)


def get_test_sqs_queue_name_from_url(sqs_queue_url: str) -> str:
  return sqs_queue_url.split('/')[-1]


async def create_test_environment(name: str) -> ImgServer:
  img_server = await create_img_server(name)
  await img_server.sqs.create_queue(
      QueueName=get_test_sqs_queue_name_from_url(img_server.sqs_queue_url),
      tags={
          'tarosky:type': 'test',
          'tarosky:repo': 'https://github.com/tarosky/gutenberg-imglambda',
      })
  return img_server


async def clean_test_environment(img_server: ImgServer) -> None:
  await img_server.sqs.delete_queue(QueueUrl=img_server.sqs_queue_url)


async def put_original(
    img_server: ImgServer,
    key: str,
    name: str,
    mime: str,
) -> None:
  path = f'{os.getcwd()}/samplefile/original/{name}'
  with open(path, 'r') as f:
    await img_server.s3.put_object(
        Body=f,
        Bucket=img_server.original_bucket,
        ContentType=mime,
        Key=key,
    )


async def put_generated(
    img_server: ImgServer,
    key: str,
    name: str,
    mime: str,
) -> None:
  path = f'{os.getcwd()}/samplefile/generated/{name}'
  with open(path, 'r') as f:
    await img_server.s3.put_object(
        Body=f,
        Bucket=img_server.generated_bucket,
        ContentType=mime,
        Key=key,
    )


async def get_original_object_time(
    img_server: ImgServer,
    key: str,
) -> datetime.datetime:
  res: Dict[str, Any] = await img_server.s3.head_object(
      Bucket=img_server.original_bucket, Key=key)
  return res['LastModified']


async def receive_sqs_message(
    img_server: ImgServer) -> Optional[Dict[str, str]]:
  res: Dict[str, List[Dict[str, Any]]] = await img_server.sqs.receive_messages(
      QueueUrl=img_server.sqs_queue_url,
      MaxNumberOfMessages=1,
      VisibilityTimeout=1,
      WaitTimeSeconds=1)
  msgs = res['Messages']
  if len(msgs) == 0:
    return None
  body: str = msgs[0]['Body']
  obj: Dict[str, str] = json.loads(body)
  return obj
