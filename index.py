import dataclasses
import datetime
import json
import logging
import os
import re
import sys
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError
from dateutil import parser, tz
from mypy_boto3_s3.client import S3Client
from mypy_boto3_sqs.client import SQSClient
from pythonjsonlogger.jsonlogger import JsonFormatter

TIMESTAMP_METADATA = 'original-timestamp'
HEADERS = 'headers'
EXPIRATION = 'Expiration'

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

# https://stackoverflow.com/a/11548754/1160341
logging.getLogger().setLevel(logging.DEBUG)


def parse_expiration(s: str) -> Dict[str, str]:
  pattern = re.compile(r'\s*([\w-]+)="([^"]*)"(:?,|$)')
  return {m.group(1): m.group(2) for m in pattern.finditer(s)}


def get_now() -> datetime.datetime:
  # Return timezone-aware datetime
  return datetime.datetime.now(tz=tz.tzutc())


class MyJsonFormatter(JsonFormatter):

  def add_fields(self, log_record: Any, record: Any, message_dict: Any) -> None:
    log_record['_ts'] = datetime.datetime.utcnow().strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ')

    if log_record.get('level'):
      log_record['level'] = log_record['level'].upper()
    else:
      log_record['level'] = record.levelname

    super().add_fields(log_record, record, message_dict)


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
  expiration_margin: int


def json_dump(obj: Any) -> str:
  return json.dumps(obj, separators=(',', ':'), sort_keys=True)


def is_not_found_client_error(exception: ClientError) -> bool:
  return exception.response['Error']['Code'] == '404'


def get_normalized_extension(path: str) -> str:
  n = path.lower()
  for le in long_exts:
    if n.endswith(le):
      return le
  _, ext = os.path.splitext(n)
  return ext


class ImgServer:
  instances: Dict[XParams, 'ImgServer'] = {}

  def __init__(
      self,
      log: logging.Logger,
      region: str,
      sqs: SQSClient,
      s3: S3Client,
      generated_domain: str,
      original_bucket: str,
      generated_key_prefix: str,
      sqs_queue_url: str,
      perm_resp_max_age: int,
      temp_resp_max_age: int,
      expiration_margin: int,
  ):
    self.log = log
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
    self.expiration_margin = datetime.timedelta(seconds=expiration_margin)

  @classmethod
  def from_lambda(cls, req: Dict[str, Any]) -> 'ImgServer':
    log = logging.getLogger(__name__)
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(MyJsonFormatter())
    log_handler.setLevel(logging.DEBUG)
    log_handler.setStream(sys.stderr)
    log.addHandler(log_handler)

    region: str = req[HEADERS]['x-env-region']
    generated_domain: str = req[HEADERS]['x-env-generated-domain']
    original_bucket: str = req['origin']['s3']['domainName'].split('.', 1)[0]
    generated_key_prefix: str = req[HEADERS]['x-env-generated-key-prefix']
    sqs_queue_url: str = req[HEADERS]['x-env-sqs-queue-url']
    perm_resp_max_age: int = int(req[HEADERS]['x-env-perm-resp-max-age'])
    temp_resp_max_age: int = int(req[HEADERS]['x-env-temp-resp-max-age'])
    expiration_margin: int = int(req[HEADERS]['x-env-expiration-margin'])

    server_key = XParams(
        region=region,
        generated_domain=generated_domain,
        original_bucket=original_bucket,
        generated_key_prefix=generated_key_prefix,
        sqs_queue_url=sqs_queue_url,
        perm_resp_max_age=perm_resp_max_age,
        temp_resp_max_age=temp_resp_max_age,
        expiration_margin=expiration_margin)

    if server_key not in cls.instances:
      sqs = boto3.client('sqs', region_name=region)
      s3 = boto3.client('s3', region_name=region)
      cls.instances[server_key] = cls(
          log=log,
          region=region,
          sqs=sqs,
          s3=s3,
          generated_domain=generated_domain,
          original_bucket=original_bucket,
          generated_key_prefix=generated_key_prefix,
          sqs_queue_url=sqs_queue_url,
          perm_resp_max_age=perm_resp_max_age,
          temp_resp_max_age=temp_resp_max_age,
          expiration_margin=expiration_margin)

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

  def get_time_original(self, key: str) -> Optional[datetime.datetime]:
    try:
      res = self.s3.head_object(Bucket=self.original_bucket, Key=key)
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e

    # Return timezone-aware datetime
    return res['LastModified']

  def object_expired(
      self,
      now: datetime.datetime,
      key: str,
      expiration: str,
  ) -> bool:
    d = parse_expiration(expiration)
    exp_str = d.get('expiry-date', None)
    if exp_str is None:
      self.log.warning(
          {
              'message': 'expiry-date not found',
              'key': key,
              'expiration': expiration,
          })
      return False

    exp = parser.parse(exp_str)
    return exp < now + self.expiration_margin

  def get_time_generated(
      self,
      now: datetime.datetime,
      key: str,
  ) -> Optional[datetime.datetime]:
    try:
      res = self.s3.head_object(Bucket=self.generated_bucket, Key=key)
      if 'Expiration' in res and self.object_expired(now, key,
                                                     res['Expiration']):
        self.log.debug(
            {
                'message': 'expired object found',
                'key': key,
                'expiration': res['Expiration'],
            })
        return None
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e

    t_str: Optional[str] = res['Metadata'].get(TIMESTAMP_METADATA, None)
    if t_str is None:
      return None
    return parser.parse(t_str)

  def keep_uptodate(
      self,
      path: str,
      t_orig: Optional[datetime.datetime],
      t_gen: Optional[datetime.datetime],
  ) -> None:
    if t_gen == t_orig:
      return

    self.sqs.send_message(
        QueueUrl=self.sqs_queue_url,
        MessageBody=json_dump({
            'bucket': self.original_bucket,
            'path': path,
        }))
    self.log.debug(
        {
            'message': 'enqueued',
            'bucket': self.original_bucket,
            'path': path,
        })

  def generate_and_respond_with_original(
      self, path: str, gen_key: str) -> FieldUpdate:
    try:
      now = get_now()
      t_gen = self.get_time_generated(now, gen_key)
      t_orig = self.get_time_original(path)

      self.keep_uptodate(path, t_orig, t_gen)

      if t_orig is not None:
        return self.as_permanent(FieldUpdate())
      return self.as_temporary(FieldUpdate())
    except Exception as e:
      self.log.error(
          {
              'message': 'error during generate_and_respond_with_original()',
              'path': path,
              'gen_key': gen_key,
              'error': str(e),
          })
      return self.as_permanent(FieldUpdate())

  def res_with_generated_or_generate_and_res_with_original(
      self, path: str, gen_key: str) -> FieldUpdate:
    try:
      now = get_now()
      t_gen = self.get_time_generated(now, gen_key)
      t_orig = self.get_time_original(path)

      self.keep_uptodate(path, t_orig, t_gen)

      if t_orig is not None and t_orig == t_gen:
        return self.as_permanent(
            self.origin_as_generated(gen_key, FieldUpdate()))
      return self.as_temporary(FieldUpdate())
    except Exception as e:
      self.log.error(
          {
              'message': (
                  'error during '
                  'res_with_generated_or_generate_and_res_with_original()'),
              'path': path,
              'gen_key': gen_key,
              'error': str(e),
          })
      return self.as_temporary(FieldUpdate())

  def res_with_original_or_generated(
      self, path: str, gen_key: str) -> FieldUpdate:
    try:
      t_orig = self.get_time_original(path)
      if t_orig is not None:
        return self.as_permanent(FieldUpdate())
      return self.as_permanent(self.origin_as_generated(gen_key, FieldUpdate()))
    except Exception as e:
      self.log.error(
          {
              'message': 'error during res_with_original_or_generated()',
              'path': path,
              'gen_key': gen_key,
              'error': str(e),
          })
      return self.as_temporary(FieldUpdate())

  def respond_with_original(self) -> FieldUpdate:
    return self.as_permanent(FieldUpdate())

  def process(self, path: str, accept_header: str) -> FieldUpdate:
    ext = get_normalized_extension(path)
    gen_key = self.generated_key_prefix + path

    if ext in image_exts:
      img_gen_key = f'{gen_key}.webp'
      if self.supports_webp(accept_header):
        return self.res_with_generated_or_generate_and_res_with_original(
            path, img_gen_key)
      else:
        return self.generate_and_respond_with_original(path, img_gen_key)
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
