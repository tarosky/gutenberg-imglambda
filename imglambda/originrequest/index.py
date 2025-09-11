import dataclasses
import datetime
import json
import logging
import os
import re
import sys
from enum import Enum
from logging import Logger
from typing import Any, Optional, Self
from urllib import parse

import boto3
from botocore.exceptions import ClientError
from dateutil import parser, tz
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.type_defs import HeadObjectOutputTypeDef
from mypy_boto3_sqs.client import SQSClient
from pathspec import PathSpec
from pathspec.patterns.gitwildmatch import GitWildMatchPattern
from pythonjsonlogger.jsonlogger import JsonFormatter

import imglambda
from imglambda.typing import HttpPath, OriginRequestEvent, Request, S3Key

TIMESTAMP_METADATA = 'original-timestamp'
OPTIMIZE_TYPE_METADATA = 'optimize-type'
OPTIMIZE_QUALITY_METADATA = 'optimize-quality'
OVERRIDABLE = 'x-res-cache-control-overridable'
CACHE_CONTROL = 'x-res-cache-control'

LAMBDA_EXPIRATION_MARGIN = 60

long_exts = [
    '.min.css',
]

expiration_re = re.compile(r'\s*([\w-]+)="([^"]*)"(:?,|$)')

API_VERSION = 2


def get_now() -> datetime.datetime:
  # Return timezone-aware datetime
  return datetime.datetime.now(tz=tz.tzutc())


class MyJsonFormatter(JsonFormatter):

  def __init__(self) -> None:
    super().__init__(json_ensure_ascii=False)

  def add_fields(self, log_record: Any, record: Any, message_dict: Any) -> None:
    log_record['_ts'] = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    if log_record.get('level'):
      log_record['level'] = log_record['level'].upper()
    else:
      log_record['level'] = record.levelname

    log_record['version'] = imglambda.version

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


def parse_expiration(s: str) -> dict[str, str]:
  return {m.group(1): m.group(2) for m in expiration_re.finditer(s)}


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
  bypass_minifier_patterns: str
  expiration_margin: int
  basedir: str


class OptimImageType(Enum):
  WEBP = 1
  AVIF = 2

  @classmethod
  def create_from_s3_object(cls, obj: HeadObjectOutputTypeDef) -> Optional['OptimImageType']:
    image_type = obj['Metadata'].get(OPTIMIZE_TYPE_METADATA)
    if image_type is None:
      return cls.WEBP
    if image_type == 'none':
      return None
    if image_type == 'avif':
      return cls.AVIF
    return cls.WEBP

  def extension(self) -> str:
    if self == OptimImageType.WEBP:
      return '.webp'
    if self == OptimImageType.AVIF:
      return '.avif'
    raise Exception('system error')


class ImagePriority:
  types: set[OptimImageType]

  def __init__(self, types: set[OptimImageType]):
    self.types = types

  @classmethod
  def create_from_accept_header(cls, accept_header: str) -> Self:
    types = set()

    if 'image/avif' in accept_header:
      types.add(OptimImageType.AVIF)

    if 'image/webp' in accept_header:
      types.add(OptimImageType.WEBP)

    return cls(types)

  def is_supported(self, image_type: OptimImageType) -> bool:
    return image_type in self.types

  def optimizable(self) -> bool:
    return len(self.types) != 0

  def response_type(self, image_type: OptimImageType) -> Optional[OptimImageType]:
    if image_type in self.types:
      return image_type
    return None


@dataclasses.dataclass(frozen=True)
class ObjectMeta:
  last_modified: datetime.datetime
  optimize_type: Optional[OptimImageType]
  optimize_quality: Optional[str]

  @classmethod
  def from_object(cls, obj: HeadObjectOutputTypeDef) -> 'ObjectMeta':
    return cls(
        last_modified=obj['LastModified'],
        optimize_type=OptimImageType.create_from_s3_object(obj),
        optimize_quality=obj['Metadata'].get(OPTIMIZE_QUALITY_METADATA))

  @classmethod
  def from_generated_object(cls, obj: HeadObjectOutputTypeDef) -> 'ObjectMeta':
    return cls(
        last_modified=parser.parse(obj['Metadata'][TIMESTAMP_METADATA]),
        optimize_type=OptimImageType.create_from_s3_object(obj),
        optimize_quality=obj['Metadata'].get(OPTIMIZE_QUALITY_METADATA))


def json_dump(obj: Any) -> str:
  return json.dumps(obj, separators=(',', ':'), sort_keys=True)


def is_not_found_client_error(exception: ClientError) -> bool:
  if 'Error' not in exception.response:
    return False
  if 'Code' not in exception.response['Error']:
    return False
  return exception.response['Error']['Code'] == '404'


def get_header(req: Request, name: str) -> str:
  return req['origin']['s3']['customHeaders'][name][0]['value']


def get_header_or(req: Request, name: str, default: str = '') -> str:
  return (get_header(req, name) if name in req['origin']['s3']['customHeaders'] else default)


def get_normalized_extension(path: HttpPath) -> str:
  n = path.lower()
  for le in long_exts:
    if n.endswith(le):
      return le
  _, ext = os.path.splitext(n)
  return ext


def key_from_path(path: HttpPath) -> S3Key:
  return S3Key(parse.unquote(path[1:]))


class ImgServer:
  instances: dict[XParams, 'ImgServer'] = {}

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
      bypass_path_spec: Optional[PathSpec],
      expiration_margin: int,
      basedir: str,
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
    self.bypass_path_spec = bypass_path_spec
    self.expiration_margin = datetime.timedelta(seconds=expiration_margin)
    self.basedir = basedir
    self.log_context = {'path': '', 'accept_header': ''}

  @classmethod
  def from_lambda(
      cls,
      log: Logger,
      req: Request,
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
      bypass_minifier_patterns: str = get_header_or(req, 'x-env-bypass-minifier-patterns')
      basedir: str = get_header_or(req, 'x-env-basedir')
    except KeyError as e:
      log.warning({
          'message': 'environment variable not found',
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
        bypass_minifier_patterns=bypass_minifier_patterns,
        expiration_margin=expiration_margin,
        basedir=basedir)

    if server_key not in cls.instances:
      sqs = boto3.client('sqs', region_name=region)
      s3 = boto3.client('s3', region_name=region)
      path_spec = (
          None if bypass_minifier_patterns == '' else PathSpec.from_lines(
              GitWildMatchPattern, bypass_minifier_patterns.split(',')))
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
          bypass_path_spec=path_spec,
          expiration_margin=expiration_margin,
          basedir=basedir)

    return cls.instances[server_key]

  def log_warning(self, message: str, dict: dict[str, Any]) -> None:
    self.log.warning({
        'message': message,
        **self.log_context,
        **dict,
    })

  def log_debug(self, message: str, dict: dict[str, Any]) -> None:
    self.log.debug({
        'message': message,
        **self.log_context,
        **dict,
    })

  def log_error(self, message: str, dict: dict[str, Any]) -> None:
    self.log.error({
        'message': message,
        **self.log_context,
        **dict,
    })

  def as_permanent(self, update: FieldUpdate) -> FieldUpdate:
    update.res_cache_control = f'public, max-age={self.perm_resp_max_age}'
    return update

  def as_temporary(self, update: FieldUpdate) -> FieldUpdate:
    update.res_cache_control = f'public, max-age={self.temp_resp_max_age}'
    return update

  def as_overridable(self, update: FieldUpdate) -> FieldUpdate:
    update.res_cache_control_overridable = 'true'
    return update

  def origin_as_generated(self, path: HttpPath, update: FieldUpdate) -> FieldUpdate:
    update.origin_domain = self.generated_domain
    update.uri = path
    return update

  def get_time_original(self, key: S3Key) -> Optional[datetime.datetime]:
    try:
      res = self.s3.head_object(Bucket=self.original_bucket, Key=key)
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e

    # Return timezone-aware datetime
    return res['LastModified']

  def get_meta_original(self, key: S3Key) -> Optional[ObjectMeta]:
    try:
      res = self.s3.head_object(Bucket=self.original_bucket, Key=key)
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e
    return ObjectMeta.from_object(res)

  def gen_path_from_path(self, path: HttpPath) -> HttpPath:
    return HttpPath(f'/{self.generated_key_prefix}{path[1:]}')

  def gen_key_from_key(self, key: S3Key) -> S3Key:
    return S3Key(f'{self.generated_key_prefix}{key}')

  def object_expired(
      self,
      now: datetime.datetime,
      key: S3Key,
      expiration: str,
  ) -> bool:
    d = parse_expiration(expiration)
    exp_str = d.get('expiry-date', None)
    if exp_str is None:
      self.log_warning('expiry-date not found', {'expiration': expiration})
      return False

    exp = parser.parse(exp_str)
    return exp < now + self.expiration_margin

  def get_time_generated(
      self,
      now: datetime.datetime,
      key: S3Key,
  ) -> Optional[datetime.datetime]:
    key = self.gen_key_from_key(key)
    try:
      res = self.s3.head_object(Bucket=self.generated_bucket, Key=key)
      if 'Expiration' in res and self.object_expired(now, key, res['Expiration']):
        self.log_debug('expired object found', {'expiration': res['Expiration']})
        return None
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e

    t_str: Optional[str] = res['Metadata'].get(TIMESTAMP_METADATA, None)
    if t_str is None:
      return None
    return parser.parse(t_str)

  def get_meta_generated(
      self,
      now: datetime.datetime,
      key: S3Key,
  ) -> Optional[ObjectMeta]:
    key = self.gen_key_from_key(key)
    try:
      res = self.s3.head_object(Bucket=self.generated_bucket, Key=key)
      if 'Expiration' in res and self.object_expired(now, key, res['Expiration']):
        self.log_debug('expired object found', {'expiration': res['Expiration']})
        return None
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e

    if TIMESTAMP_METADATA not in res['Metadata']:
      return None

    return ObjectMeta.from_generated_object(res)

  def keep_uptodate(
      self,
      key: S3Key,
      t_orig: Optional[datetime.datetime],
      t_gen: Optional[datetime.datetime],
      orig_quality: Optional[str] = None,
      gen_quality: Optional[str] = None,
  ) -> None:
    if t_gen == t_orig and orig_quality == gen_quality:
      return

    body = {
        'version': API_VERSION,
        'path': key,
        'src': {
            'bucket': self.original_bucket,
            'prefix': '',
        },
        'dest': {
            'bucket': self.generated_bucket,
            'prefix': self.generated_key_prefix,
        },
    }

    self.sqs.send_message(QueueUrl=self.sqs_queue_url, MessageBody=json_dump(body))
    self.log_debug('enqueued', {'body': body})

  def process_for_image_requester(self, path: HttpPath, accept_header: str) -> FieldUpdate:
    priority = ImagePriority.create_from_accept_header(accept_header)
    try:
      now = get_now()

      key = key_from_path(path)
      orig_meta = self.get_meta_original(key)

      if orig_meta is None:
        for image_type in OptimImageType:
          gen_meta = self.get_meta_generated(now, S3Key(f'{key}{image_type.extension()}'))
          if gen_meta is not None:
            self.keep_uptodate(key, None, gen_meta.last_modified)

        return self.as_temporary(FieldUpdate())

      if orig_meta.optimize_type is not None:
        gen_meta = self.get_meta_generated(
            now, S3Key(f'{key}{orig_meta.optimize_type.extension()}'))
        gen_last_modified = None if gen_meta is None else gen_meta.last_modified
        gen_optimize_quality = None if gen_meta is None else gen_meta.optimize_quality

        self.keep_uptodate(
            key, orig_meta.last_modified, gen_last_modified, orig_meta.optimize_quality,
            gen_optimize_quality)

      if not priority.optimizable():
        return self.as_permanent(FieldUpdate())

      if orig_meta.optimize_type is None:
        return self.as_temporary(FieldUpdate())

      updated_path = self.gen_path_from_path(f'{path}{orig_meta.optimize_type.extension()}')

      if priority.response_type(orig_meta.optimize_type) is None:
        return self.as_permanent(self.origin_as_generated(updated_path, FieldUpdate()))

      if orig_meta.last_modified != gen_last_modified:
        return self.as_temporary(FieldUpdate())

      if orig_meta.optimize_quality != gen_optimize_quality:
        return self.as_temporary(FieldUpdate())

      return self.as_permanent(self.origin_as_generated(updated_path, FieldUpdate()))
    except Exception as e:
      self.log_error('error during process_for_image_requester()', {'error': str(e)})
      if priority.optimizable():
        return self.as_temporary(FieldUpdate())
      return self.as_permanent(FieldUpdate())

  def process_for_css_requester(self, path: HttpPath) -> FieldUpdate:
    key = key_from_path(path)
    try:
      now = get_now()
      t_gen = self.get_time_generated(now, key)
      t_orig = self.get_time_original(key)

      self.keep_uptodate(key, t_orig, t_gen)

      if t_orig is not None and t_orig == t_gen:
        return self.as_permanent(
            self.origin_as_generated(self.gen_path_from_path(path), FieldUpdate()))
      return self.as_temporary(FieldUpdate())
    except Exception as e:
      self.log_error('error during process_for_css_requester()', {'error': str(e)})
      return self.as_temporary(FieldUpdate())

  def process(self, path: HttpPath, accept_header: str) -> FieldUpdate:

    def run(path: HttpPath):
      if self.bypass_path_spec is not None and self.bypass_path_spec.match_file(path):
        return self.as_overridable(self.as_permanent(FieldUpdate()))

      ext = get_normalized_extension(path)

      if ext in ['.jpg', '.jpeg', '.png', '.gif']:
        return self.process_for_image_requester(path, accept_header)
      elif ext == '.css':
        return self.process_for_css_requester(path)
      else:
        # This condition includes:
        #   '.min.css'
        return self.as_overridable(self.as_permanent(FieldUpdate()))

    if self.basedir != '':
      if not path.startswith(self.basedir):
        self.log_error('path without basedir passed', {'basedir': self.basedir})
      else:
        path = HttpPath(path[len(self.basedir):])

    fu = run(path)
    if self.basedir != '' and fu.uri is None:
      fu.uri = path

    return fu

  def set_log_context(self, path: HttpPath, accept_header: str) -> None:
    self.log_context = {'path': str(path), 'accept_header': accept_header}


def lambda_main(event: OriginRequestEvent) -> Request:
  req = event['Records'][0]['cf']['request']
  accept_header = req['headers']['accept'][0]['value'] if 'accept' in req['headers'] else ''

  # Set default value
  req['headers'][CACHE_CONTROL] = [{'value': ''}]
  req['headers'][OVERRIDABLE] = [{'value': ''}]

  server = ImgServer.from_lambda(logger, req)
  if server is None:
    return req

  path = req['uri']

  server.set_log_context(path, accept_header)
  field_update = server.process(path, accept_header)

  if field_update.origin_domain is not None:
    req['origin']['s3']['domainName'] = field_update.origin_domain
    req['headers']['host'][0]['value'] = field_update.origin_domain

  if field_update.uri is not None:
    req['uri'] = HttpPath(field_update.uri)

  if field_update.res_cache_control is not None:
    req['headers'][CACHE_CONTROL] = [
        {
            'value': field_update.res_cache_control,
        },
    ]

  if field_update.res_cache_control_overridable is not None:
    req['headers'][OVERRIDABLE] = [
        {
            'value': field_update.res_cache_control_overridable,
        },
    ]

  server.log_debug(
      'done', {
          'uri': req['uri'],
          'origin_domain': req['origin']['s3']['domainName'],
          'res_cache_control': req['headers'][CACHE_CONTROL][0]['value'],
          'res_cache_control_overridable': req['headers'][OVERRIDABLE][0]['value'],
      })

  return req
