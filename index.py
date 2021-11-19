import dataclasses
import datetime
import json
import logging
import os
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError
from dateutil import parser
from mypy_boto3_s3.client import S3Client
from mypy_boto3_sqs.client import SQSClient

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

TIMESTAMP_METADATA = 'original-timestamp'
HEADERS = 'headers'

image_exts = set([
    '.jpg',
    '.jpeg',
    '.png',
    '.gif',
])

long_exts = [
    '.js.map',
    '.min.js',
    '.min.css',
]

minifiable_exts = set([
    '.css',
    '.js',
])


@dataclasses.dataclass
class FieldUpdate:
  res_cache_control: Optional[str] = None
  origin_domain: Optional[str] = None
  origin_path: Optional[str] = None


@dataclasses.dataclass
class XParams:
  region: str
  generated_domain: str
  original_bucket: str
  generated_key_prefix: str
  sqs_queue_url: str
  perm_resp_max_age: int
  temp_resp_max_age: int


def json_dump(obj: Any) -> str:
  return json.dumps(obj, separators=(',', ':'), sort_keys=True)


def is_not_found_client_error(exception: ClientError) -> bool:
  return exception.response['Error']['Code'] == '404'


def get_normalized_extension(path: str) -> str:
  n = path.lower()
  for le in long_exts:
    if n.endswith(le):
      return le
  _, ext = os.path.splitext(path)
  return ext


class ImgServer:
  instances: Dict[XParams, 'ImgServer'] = {}

  def __init__(
      self,
      region: str,
      sqs: SQSClient,
      s3: S3Client,
      generated_domain: str,
      original_bucket: str,
      generated_key_prefix: str,
      sqs_queue_url: str,
      perm_resp_max_age: int,
      temp_resp_max_age: int,
  ):
    self.region = region
    self.sqs = sqs
    self.s3 = s3
    self.generated_domain = generated_domain
    self.generated_bucket = generated_domain.split('.', 1)[0]
    self.original_bucket = original_bucket
    self.generated_key_prefix = generated_key_prefix
    self.sqs_queue_url = sqs_queue_url
    self.perm_resp_max_age = perm_resp_max_age
    self.temp_resp_max_age = temp_resp_max_age

  @classmethod
  def from_lambda(cls, req: Dict[str, Any]) -> 'ImgServer':
    region: str = req[HEADERS]['x-env-region']
    generated_domain: str = req[HEADERS]['x-env-generated-domain']
    original_bucket: str = req['origin']['s3']['domainName'].split('.', 1)[0]
    generated_key_prefix: str = req[HEADERS]['x-env-generated-key-prefix']
    sqs_queue_url: str = req[HEADERS]['x-env-sqs-queue-url']
    perm_resp_max_age: int = int(req[HEADERS]['x-env-perm-resp-max-age'])
    temp_resp_max_age: int = int(req[HEADERS]['x-env-temp-resp-max-age'])

    server_key = XParams(
        region=region,
        generated_domain=generated_domain,
        original_bucket=original_bucket,
        generated_key_prefix=generated_key_prefix,
        sqs_queue_url=sqs_queue_url,
        perm_resp_max_age=perm_resp_max_age,
        temp_resp_max_age=temp_resp_max_age)

    if server_key not in cls.instances:
      sqs = boto3.client('sqs', region_name=region)
      s3 = boto3.client('s3', region_name=region)
      cls.instances[server_key] = cls(
          region=region,
          sqs=sqs,
          s3=s3,
          generated_domain=generated_domain,
          original_bucket=original_bucket,
          generated_key_prefix=generated_key_prefix,
          sqs_queue_url=sqs_queue_url,
          perm_resp_max_age=perm_resp_max_age,
          temp_resp_max_age=temp_resp_max_age)

    return cls.instances[server_key]

  def as_permanent(self, update: FieldUpdate) -> FieldUpdate:
    update.res_cache_control = f'public, max-age={self.perm_resp_max_age}'
    return update

  def as_temporary(self, update: FieldUpdate) -> FieldUpdate:
    update.res_cache_control = f'public, max-age={self.temp_resp_max_age}'
    return update

  def origin_as_generated(
      self, gen_key: str, update: FieldUpdate) -> FieldUpdate:
    update.origin_domain = self.generated_domain
    update.origin_path = gen_key
    return update

  def supports_webp(self, accept_header: str) -> bool:
    return 'image/webp' in accept_header

  def get_time_original(self, path: str) -> Optional[datetime.datetime]:
    try:
      res = self.s3.head_object(Bucket=self.original_bucket, Key=path)
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e

    return res['LastModified']

  def get_time_generated(self, path: str) -> Optional[datetime.datetime]:
    try:
      res = self.s3.head_object(
          Bucket=self.generated_bucket,
          Key=self.generated_key_prefix + '/' + path)
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e

    t_str: Optional[str] = res['Metadata'].get(TIMESTAMP_METADATA, None)
    if t_str is None:
      return None
    return parser.parse(t_str)

  def needs_generation(
      self, t_orig: Optional[datetime.datetime],
      t_gen: Optional[datetime.datetime]) -> bool:
    return t_gen is None or t_gen != t_orig

  def keep_uptodate(
      self,
      path: str,
      t_orig: Optional[datetime.datetime],
      t_gen: Optional[datetime.datetime],
  ) -> None:
    if not self.needs_generation(t_orig, t_gen):
      return

    self.sqs.send_message(
        QueueUrl=self.sqs_queue_url,
        MessageBody=json_dump({
            'bucket': self.generated_bucket,
            'path': path,
        }))
    log.debug(
        json_dump(
            {
                'message': 'enqueued',
                'bucket': self.generated_bucket,
                'path': path,
            }))

  def generate_and_respond_with_original(
      self, path: str, gen_key: str) -> FieldUpdate:
    try:
      t_gen = self.get_time_generated(gen_key)
      t_orig = self.get_time_original(path)

      self.keep_uptodate(path, t_orig, t_gen)

      return self.as_permanent(FieldUpdate())
    except Exception as e:
      log.warning(
          json_dump({
              'path': path,
              'gen_key': gen_key,
              'error': str(e),
          }))
      return self.as_permanent(FieldUpdate())

  def res_with_generated_or_generate_and_res_with_original(
      self, path: str, gen_key: str) -> FieldUpdate:
    try:
      t_gen = self.get_time_generated(gen_key)
      t_orig = self.get_time_original(path)

      if not self.needs_generation(t_orig, t_gen):
        return self.as_permanent(
            self.origin_as_generated(gen_key, FieldUpdate()))

      self.keep_uptodate(path, t_orig, t_gen)

      return self.as_temporary(FieldUpdate())
    except Exception as e:
      log.warning(
          json_dump({
              'path': path,
              'gen_key': gen_key,
              'error': str(e),
          }))
      return self.as_temporary(FieldUpdate())

  def res_with_original_or_generated(
      self, path: str, gen_key: str) -> FieldUpdate:
    t_orig = self.get_time_original(path)
    if t_orig is not None:
      return self.as_permanent(FieldUpdate())
    return self.as_permanent(self.origin_as_generated(gen_key, FieldUpdate()))

  def respond_with_original(self) -> FieldUpdate:
    return self.as_permanent(FieldUpdate())

  def process(self, path: str, accept_header: str) -> FieldUpdate:
    ext = get_normalized_extension(path)
    gen_key = self.generated_key_prefix + '/' + path

    if ext in image_exts:
      if self.supports_webp(accept_header):
        return self.res_with_generated_or_generate_and_res_with_original(
            path, gen_key + '.webp')
      else:
        return self.generate_and_respond_with_original(path, gen_key)
    elif ext in minifiable_exts:
      return self.res_with_generated_or_generate_and_res_with_original(
          path, gen_key)
    elif ext == '.js.map':
      return self.res_with_original_or_generated(path, gen_key)
    else:
      # This condition includes:
      #   '.min.js' and '.min.css'
      return self.respond_with_original()


def lambda_main(event: Dict[str, Any]) -> Dict[str, Any]:
  req: Dict[str, Any] = event['Records'][0]['cf']['request']
  accept_header: str = req[HEADERS].get('accept', {'value': ''})['value']

  server = ImgServer.from_lambda(req)
  # Remove leading '/'
  field_update = server.process(req['uri'][1:], accept_header)

  if field_update.origin_domain is not None:
    req['origin']['s3']['domainName'] = field_update.origin_domain

  if field_update.origin_path is not None:
    req['origin']['s3']['path'] = field_update.origin_path

  if field_update.res_cache_control is not None:
    req[HEADERS]['x-res-cache-control'] = {
        'key': 'X-Res-Cache-Control',
        'value': field_update.res_cache_control,
    }

  return req


def lambda_handler(event: Dict[str, Any], _: Any) -> Any:
  return lambda_main(event)
