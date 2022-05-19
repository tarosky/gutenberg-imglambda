import dataclasses
import datetime
import json
import logging
import os
import re
import sys
from logging import Logger
from pathlib import Path
from typing import Any, Dict, Optional
from urllib import parse

import boto3
from botocore.exceptions import ClientError
from dateutil import parser, tz
from mypy_boto3_s3.client import S3Client
from mypy_boto3_sqs.client import SQSClient
from pythonjsonlogger.jsonlogger import JsonFormatter

TIMESTAMP_METADATA = 'original-timestamp'
HEADERS = 'headers'
EXPIRATION = 'Expiration'
MESSAGE = 'message'
VALUE = 'value'

LAMBDA_EXPIRATION_MARGIN = 60

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

API_VERSION = 2


def get_version() -> str:
  return Path(__file__).resolve().with_name('VERSION').read_text().strip()


version = get_version()


def get_now() -> datetime.datetime:
  # Return timezone-aware datetime
  return datetime.datetime.now(tz=tz.tzutc())


class MyJsonFormatter(JsonFormatter):

  def __init__(self) -> None:
    super().__init__(json_ensure_ascii=False)

  def add_fields(self, log_record: Any, record: Any, message_dict: Any) -> None:
    log_record['_ts'] = datetime.datetime.utcnow().strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ')

    if log_record.get('level'):
      log_record['level'] = log_record['level'].upper()
    else:
      log_record['level'] = record.levelname

    log_record['version'] = version

    super().add_fields(log_record, record, message_dict)


def init_logging() -> Logger:
  # https://stackoverflow.com/a/11548754/1160341
  logger = logging.getLogger()
  logger.setLevel(logging.DEBUG)
  for h in logger.handlers:
    logger.removeHandler(h)

  logging.getLogger('botocore').setLevel(logging.WARNING)
  logging.getLogger('urllib3').setLevel(logging.INFO)

  log = logging.getLogger(__name__)
  log_handler = logging.StreamHandler()
  log_handler.setFormatter(MyJsonFormatter())
  log_handler.setLevel(logging.DEBUG)
  log_handler.setStream(sys.stderr)
  log.addHandler(log_handler)
  log.propagate = False

  return log


logger = init_logging()


def parse_expiration(s: str) -> Dict[str, str]:
  pattern = re.compile(r'\s*([\w-]+)="([^"]*)"(:?,|$)')
  return {m.group(1): m.group(2) for m in pattern.finditer(s)}


@dataclasses.dataclass
class FieldUpdate:
  res_cache_control: Optional[str] = None
  res_cache_control_overridable: Optional[str] = None
  origin_domain: Optional[str] = None
  uri: Optional[str] = None


@dataclasses.dataclass(eq=True, frozen=True)
class XParams:
  region: str
  generated_domain: str
  original_bucket: str
  generated_key_prefix: str
  sqs_queue_url: str
  perm_resp_max_age: int
  temp_resp_max_age: int
  expiration_margin: int


@dataclasses.dataclass
class Location:
  path: str
  key: str
  gen_path: str
  gen_key: str

  @classmethod
  def from_path(
      cls, path: str, gen_prefix: str, gen_postfix: str) -> 'Location':
    key = parse.unquote(path[1:])
    return cls(
        path=path,
        key=key,
        gen_path=f'/{gen_prefix}{path[1:]}{gen_postfix}',
        gen_key=f'{gen_prefix}{key}{gen_postfix}')

  def as_dict(self) -> Dict[str, str]:
    return {
        'path': self.path,
        'key': self.key,
        'gen_path': self.gen_path,
        'gen_key': self.gen_key,
    }


def json_dump(obj: Any) -> str:
  return json.dumps(obj, separators=(',', ':'), sort_keys=True)


def is_not_found_client_error(exception: ClientError) -> bool:
  return exception.response['Error']['Code'] == '404'


def get_header(req: Dict[str, Any], name: str) -> str:
  return req['origin']['s3']['customHeaders'][name][0][VALUE]


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
  def from_lambda(
      cls,
      log: Logger,
      req: Dict[str, Any],
  ) -> Optional['ImgServer']:
    expiration_margin: int = LAMBDA_EXPIRATION_MARGIN

    try:
      region: str = get_header(req, 'x-env-region')
      generated_domain: str = get_header(req, 'x-env-generated-domain')
      original_bucket: str = req['origin']['s3']['domainName'].split('.', 1)[0]
      generated_key_prefix: str = get_header(req, 'x-env-generated-key-prefix')
      sqs_queue_url: str = get_header(req, 'x-env-sqs-queue-url')
      perm_resp_max_age: int = int(get_header(req, 'x-env-perm-resp-max-age'))
      temp_resp_max_age: int = int(get_header(req, 'x-env-temp-resp-max-age'))
    except KeyError as e:
      log.warning({
          MESSAGE: 'environment variable not found',
          'key': str(e),
      })
      return None

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

  def as_overridable(self, update: FieldUpdate) -> FieldUpdate:
    update.res_cache_control_overridable = 'true'
    return update

  def origin_as_generated(
      self, loc: Location, update: FieldUpdate) -> FieldUpdate:
    update.origin_domain = self.generated_domain
    update.uri = loc.gen_path
    return update

  def supports_webp(self, accept_header: str) -> bool:
    return 'image/webp' in accept_header

  def get_time_original(self, loc: Location) -> Optional[datetime.datetime]:
    try:
      res = self.s3.head_object(Bucket=self.original_bucket, Key=loc.key)
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e

    # Return timezone-aware datetime
    return res['LastModified']

  def object_expired(
      self,
      now: datetime.datetime,
      loc: Location,
      expiration: str,
  ) -> bool:
    d = parse_expiration(expiration)
    exp_str = d.get('expiry-date', None)
    if exp_str is None:
      self.log.warning(
          {
              MESSAGE: 'expiry-date not found',
              **loc.as_dict(),
              'expiration': expiration,
          })
      return False

    exp = parser.parse(exp_str)
    return exp < now + self.expiration_margin

  def get_time_generated(
      self,
      now: datetime.datetime,
      loc: Location,
  ) -> Optional[datetime.datetime]:
    try:
      res = self.s3.head_object(Bucket=self.generated_bucket, Key=loc.gen_key)
      if 'Expiration' in res and self.object_expired(now, loc,
                                                     res['Expiration']):
        self.log.debug(
            {
                MESSAGE: 'expired object found',
                **loc.as_dict(),
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
      loc: Location,
      t_orig: Optional[datetime.datetime],
      t_gen: Optional[datetime.datetime],
  ) -> None:
    if t_gen == t_orig:
      return

    body = {
        'version': API_VERSION,
        'path': loc.key,
        'src': {
            'bucket': self.original_bucket,
            'prefix': '',
        },
        'dest': {
            'bucket': self.generated_bucket,
            'prefix': self.generated_key_prefix,
        },
    }

    self.sqs.send_message(
        QueueUrl=self.sqs_queue_url, MessageBody=json_dump(body))
    self.log.debug({
        MESSAGE: 'enqueued',
        **loc.as_dict(),
        'body': body,
    })

  def generate_and_respond_with_original(self, loc: Location) -> FieldUpdate:
    try:
      now = get_now()
      t_gen = self.get_time_generated(now, loc)
      t_orig = self.get_time_original(loc)

      self.keep_uptodate(loc, t_orig, t_gen)

      if t_orig is not None:
        return self.as_permanent(FieldUpdate())
      return self.as_temporary(FieldUpdate())
    except Exception as e:
      self.log.error(
          {
              MESSAGE: 'error during generate_and_respond_with_original()',
              **loc.as_dict(),
              'error': str(e),
          })
      return self.as_permanent(FieldUpdate())

  def res_with_generated_or_generate_and_res_with_original(
      self, loc: Location) -> FieldUpdate:
    try:
      now = get_now()
      t_gen = self.get_time_generated(now, loc)
      t_orig = self.get_time_original(loc)

      self.keep_uptodate(loc, t_orig, t_gen)

      if t_orig is not None and t_orig == t_gen:
        return self.as_permanent(self.origin_as_generated(loc, FieldUpdate()))
      return self.as_temporary(FieldUpdate())
    except Exception as e:
      self.log.error(
          {
              MESSAGE: (
                  'error during '
                  'res_with_generated_or_generate_and_res_with_original()'),
              **loc.as_dict(),
              'error': str(e),
          })
      return self.as_temporary(FieldUpdate())

  def res_with_original_or_generated(self, loc: Location) -> FieldUpdate:
    try:
      t_orig = self.get_time_original(loc)
      if t_orig is not None:
        return self.as_permanent(FieldUpdate())
      return self.as_permanent(self.origin_as_generated(loc, FieldUpdate()))
    except Exception as e:
      self.log.error(
          {
              MESSAGE: 'error during res_with_original_or_generated()',
              **loc.as_dict(),
              'error': str(e),
          })
      return self.as_temporary(FieldUpdate())

  def respond_with_original(self) -> FieldUpdate:
    return self.as_overridable(self.as_permanent(FieldUpdate()))

  def process(self, path: str, accept_header: str) -> FieldUpdate:
    ext = get_normalized_extension(path)

    if ext in image_exts:
      if self.supports_webp(accept_header):
        return self.res_with_generated_or_generate_and_res_with_original(
            Location.from_path(path, self.generated_key_prefix, '.webp'))
      else:
        return self.generate_and_respond_with_original(
            Location.from_path(path, self.generated_key_prefix, '.webp'))
    elif ext in minifiable_exts:
      return self.res_with_generated_or_generate_and_res_with_original(
          Location.from_path(path, self.generated_key_prefix, ''))
    elif ext == '.js.map':
      return self.res_with_original_or_generated(
          Location.from_path(path, self.generated_key_prefix, ''))
    else:
      # This condition includes:
      #   '.min.js' and '.min.css'
      return self.respond_with_original()


def lambda_main(event: Dict[str, Any]) -> Dict[str, Any]:
  req: Dict[str, Any] = event['Records'][0]['cf']['request']
  accept_header: str = (
      req[HEADERS]['accept'][0][VALUE] if 'accept' in req[HEADERS] else '')

  # Set default value
  req[HEADERS]['x-res-cache-control'] = [{VALUE: ''}]
  req[HEADERS]['x-res-cache-control-overridable'] = [{VALUE: ''}]

  server = ImgServer.from_lambda(logger, req)
  if server is None:
    return req

  path = req['uri']
  field_update = server.process(path, accept_header)

  if field_update.origin_domain is not None:
    req['origin']['s3']['domainName'] = field_update.origin_domain
    req[HEADERS]['host'][0][VALUE] = field_update.origin_domain

  if field_update.uri is not None:
    req['uri'] = field_update.uri

  if field_update.res_cache_control is not None:
    req[HEADERS]['x-res-cache-control'] = [
        {
            VALUE: field_update.res_cache_control,
        },
    ]

  if field_update.res_cache_control_overridable is not None:
    req[HEADERS]['x-res-cache-control-overridable'] = [
        {
            VALUE: field_update.res_cache_control_overridable,
        },
    ]

  server.log.debug(
      {
          MESSAGE: 'done',
          'path': path,
          'uri': req['uri'],
          'accept_header': accept_header,
          'origin_domain': req['origin']['s3']['domainName'],
          'res_cache_control': req[HEADERS]['x-res-cache-control'][0][VALUE],
          'res_cache_control_overridable': (
              req[HEADERS]['x-res-cache-control-overridable'][0][VALUE]),
      })

  return req


def lambda_handler(event: Dict[str, Any], _: Any) -> Any:
  # # For debugging
  # print('event:')
  # print(json.dumps(event))

  ret = lambda_main(event)

  # # For debugging
  # print('return:')
  # print(json.dumps(ret))

  return ret
