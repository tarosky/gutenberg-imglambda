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
from mypy_boto3_s3.type_defs import HeadObjectOutputTypeDef
from mypy_boto3_sqs.client import SQSClient
from pathspec import PathSpec
from pathspec.patterns.gitwildmatch import GitWildMatchPattern
from pythonjsonlogger.jsonlogger import JsonFormatter

TIMESTAMP_METADATA = 'original-timestamp'
OPTIMIZE_TYPE_METADATA = 'optimize-type'
OPTIMIZE_QUALITY_METADATA = 'optimize-quality'
HEADERS = 'headers'
EXPIRATION = 'Expiration'
MESSAGE = 'message'
VALUE = 'value'

LAMBDA_EXPIRATION_MARGIN = 60

WEBP_EXTENSION = '.webp'
AVIF_EXTENSION = '.avif'

image_exts = set([
    '.jpg',
    '.jpeg',
    '.png',
    '.gif',
])

long_exts = [
    '.min.css',
]

expiration_re = re.compile(r'\s*([\w-]+)="([^"]*)"(:?,|$)')

API_VERSION = 2


def get_version() -> str:
  return Path(__file__).parent.resolve().with_name('VERSION').read_text().strip()


version = get_version()


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


@dataclasses.dataclass(frozen=True)
class Location:
  path: str
  key: str
  gen_path: str
  gen_key: str

  @classmethod
  def from_path(cls, path: str, gen_prefix: str, gen_postfix: str) -> 'Location':
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


@dataclasses.dataclass(frozen=True)
class ObjectMeta:
  last_modified: datetime.datetime
  metadata_optimize_type: Optional[str]
  metadata_optimize_quality: Optional[str]

  @classmethod
  def from_object(cls, obj: HeadObjectOutputTypeDef) -> 'ObjectMeta':
    return cls(
      last_modified=obj['LastModified'],
      metadata_optimize_type=obj['Metadata'].get(OPTIMIZE_TYPE_METADATA),
      metadata_optimize_quality=obj['Metadata'].get(OPTIMIZE_QUALITY_METADATA))

  @classmethod
  def from_generated_object(cls, obj: HeadObjectOutputTypeDef) -> 'ObjectMeta':
    return cls(
      last_modified=parser.parse(obj['Metadata'][TIMESTAMP_METADATA]),
      metadata_optimize_type=obj['Metadata'].get(OPTIMIZE_TYPE_METADATA),
      metadata_optimize_quality=obj['Metadata'].get(OPTIMIZE_QUALITY_METADATA))


@dataclasses.dataclass(frozen=True)
class ImageLocation:
  path: str
  key: str
  gen_prefix: str
  gen_path_prefix: str
  gen_key_prefix: str

  @classmethod
  def from_path(cls, path: str, gen_prefix: str) -> 'Location':
    key = parse.unquote(path[1:])
    return cls(
        path=path,
        key=key,
        gen_prefix=gen_prefix,
        gen_path_prefix=f'/{gen_prefix}{path[1:]}',
        gen_key_prefix=f'{gen_prefix}{key}')

  def as_dict(self) -> Dict[str, str]:
    return {
        'path': self.path,
        'key': self.key,
        'gen_path_prefix': self.gen_path_prefix,
        'gen_key_prefix': self.gen_key_prefix,
    }

  def get_location(self, postfix: str) -> Location:
    return Location.from_path(self.path, self.gen_prefix, postfix)


def json_dump(obj: Any) -> str:
  return json.dumps(obj, separators=(',', ':'), sort_keys=True)


def is_not_found_client_error(exception: ClientError) -> bool:
  if 'Error' not in exception.response:
    return False
  if 'Code' not in exception.response['Error']:
    return False
  return exception.response['Error']['Code'] == '404'


def get_header(req: Dict[str, Any], name: str) -> str:
  return req['origin']['s3']['customHeaders'][name][0][VALUE]


def get_header_or(req: Dict[str, Any], name: str, default: str = '') -> str:
  return (get_header(req, name) if name in req['origin']['s3']['customHeaders'] else default)


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
      bypass_minifier_path_spec: Optional[PathSpec],
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
    self.bypass_minifier_path_spec = bypass_minifier_path_spec
    self.expiration_margin = datetime.timedelta(seconds=expiration_margin)
    self.basedir = basedir

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
      bypass_minifier_patterns: str = get_header_or(req, 'x-env-bypass-minifier-patterns')
      basedir: str = get_header_or(req, 'x-env-basedir')
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
          bypass_minifier_path_spec=path_spec,
          expiration_margin=expiration_margin,
          basedir=basedir)

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

  def origin_as_generated(self, loc: Location, update: FieldUpdate) -> FieldUpdate:
    update.origin_domain = self.generated_domain
    update.uri = loc.gen_path
    return update

  def supports_webp(self, accept_header: str) -> bool:
    return 'image/webp' in accept_header

  def supports_avif(self, accept_header: str) -> bool:
    return 'image/avif' in accept_header

  def get_time_original(self, loc: Location) -> Optional[datetime.datetime]:
    try:
      res = self.s3.head_object(Bucket=self.original_bucket, Key=loc.key)
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e

    # Return timezone-aware datetime
    return res['LastModified']

  def get_meta_original(self, loc: Location) -> Optional[ObjectMeta]:
    try:
      res = self.s3.head_object(Bucket=self.original_bucket, Key=loc.key)
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e
    return ObjectMeta.from_object(res)

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
      if 'Expiration' in res and self.object_expired(now, loc, res['Expiration']):
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

  def get_meta_generated(
      self,
      now: datetime.datetime,
      loc: Location,
  ) -> Optional[ObjectMeta]:
    try:
      res = self.s3.head_object(Bucket=self.generated_bucket, Key=loc.gen_key)
      if 'Expiration' in res and self.object_expired(now, loc, res['Expiration']):
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

    if TIMESTAMP_METADATA not in res['Metadata']:
      return None

    return ObjectMeta.from_generated_object(res)

  def keep_uptodate(
      self,
      loc: Location,
      t_orig: Optional[datetime.datetime],
      t_gen: Optional[datetime.datetime],
      orig_quality: Optional[str] = None,
      gen_quality: Optional[str] = None,
  ) -> None:
    if t_gen == t_orig and orig_quality == gen_quality:
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

    self.sqs.send_message(QueueUrl=self.sqs_queue_url, MessageBody=json_dump(body))
    self.log.debug({
        MESSAGE: 'enqueued',
        **loc.as_dict(),
        'body': body,
    })

  def process_for_original_image_requester(self, path: str) -> FieldUpdate:
    image_loc = ImageLocation.from_path(path, self.generated_key_prefix)

    try:
      now = get_now()

      orig_meta = self.get_meta_original(image_loc)

      if orig_meta is None:
        avif_loc = image_loc.get_location(AVIF_EXTENSION)
        gen_avif_meta = self.get_meta_generated(now, avif_loc)
        webp_loc = image_loc.get_location(WEBP_EXTENSION)
        gen_webp_meta = self.get_meta_generated(now, webp_loc)
        if gen_avif_meta is not None:
          self.keep_uptodate(avif_loc, None, gen_avif_meta.last_modified)
        if gen_webp_meta is not None:
          self.keep_uptodate(webp_loc, None, gen_webp_meta.last_modified)

        return self.as_temporary(FieldUpdate())

      if orig_meta.metadata_optimize_type == 'avif':
        loc = image_loc.get_location(AVIF_EXTENSION)
      else:
        loc = image_loc.get_location(WEBP_EXTENSION)

      gen_meta = self.get_meta_generated(now, loc)
      gen_last_modified = None if gen_meta is None else gen_meta.last_modified
      gen_optimize_quality = None if gen_meta is None else gen_meta.metadata_optimize_quality

      self.keep_uptodate(
          loc,
          orig_meta.last_modified,
          gen_last_modified,
          orig_meta.metadata_optimize_quality,
          gen_optimize_quality)

      return self.as_permanent(FieldUpdate())

    except Exception as e:
      self.log.error(
          {
              MESSAGE: 'error during process_for_original_image_requester()',
              **image_loc.as_dict(),
              'error': str(e),
          })
      return self.as_permanent(FieldUpdate())

  def process_for_optimized_image_requester(
      self, path: str, accepts_webp: bool, accepts_avif: bool) -> FieldUpdate:
    image_loc = ImageLocation.from_path(path, self.generated_key_prefix)
    try:
      now = get_now()

      orig_meta = self.get_meta_original(image_loc)

      if orig_meta is None:
        avif_loc = image_loc.get_location(AVIF_EXTENSION)
        gen_avif_meta = self.get_meta_generated(now, avif_loc)
        webp_loc = image_loc.get_location(WEBP_EXTENSION)
        gen_webp_meta = self.get_meta_generated(now, webp_loc)
        if gen_avif_meta is not None:
          self.keep_uptodate(avif_loc, None, gen_avif_meta.last_modified)
        if gen_webp_meta is not None:
          self.keep_uptodate(webp_loc, None, gen_webp_meta.last_modified)

        return self.as_temporary(FieldUpdate())

      if orig_meta.metadata_optimize_type == 'avif':
        loc = image_loc.get_location(AVIF_EXTENSION)
      else:
        loc = image_loc.get_location(WEBP_EXTENSION)

      gen_meta = self.get_meta_generated(now, loc)
      gen_last_modified = None if gen_meta is None else gen_meta.last_modified
      gen_optimize_quality = None if gen_meta is None else gen_meta.metadata_optimize_quality

      self.keep_uptodate(
          loc,
          orig_meta.last_modified,
          gen_last_modified,
          orig_meta.metadata_optimize_quality,
          gen_optimize_quality)

      if orig_meta.last_modified == gen_last_modified and (
          orig_meta.metadata_optimize_quality == gen_optimize_quality):
        if orig_meta.metadata_optimize_type == 'avif':
          if accepts_avif:
            return self.as_permanent(self.origin_as_generated(loc, FieldUpdate()))
        else:
          if accepts_webp:
            return self.as_permanent(self.origin_as_generated(loc, FieldUpdate()))

      return self.as_temporary(FieldUpdate())
    except Exception as e:
      self.log.error(
          {
              MESSAGE: 'error during process_for_optimized_image_requester()',
              **loc.as_dict(),
              'error': str(e),
          })
      return self.as_temporary(FieldUpdate())

  def process_for_css_requester(self, loc: Location) -> FieldUpdate:
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
              MESSAGE: 'error during process_for_css_requester()',
              **loc.as_dict(),
              'error': str(e),
          })
      return self.as_temporary(FieldUpdate())

  def process(self, path: str, accept_header: str) -> FieldUpdate:

    def run(path: str):
      if self.bypass_minifier_path_spec is not None and (
          self.bypass_minifier_path_spec.match_file(path)):
        return self.as_overridable(self.as_permanent(FieldUpdate()))

      ext = get_normalized_extension(path)

      if ext in image_exts:
        accepts_webp = self.supports_webp(accept_header)
        accepts_avif = self.supports_avif(accept_header)
        if accepts_webp or accepts_avif:
          return self.process_for_optimized_image_requester(path, accepts_webp, accepts_avif)
        else:
          return self.process_for_original_image_requester(path)
      elif ext == '.css':
        return self.process_for_css_requester(
            Location.from_path(path, self.generated_key_prefix, ''))
      else:
        # This condition includes:
        #   '.min.css'
        return self.as_overridable(self.as_permanent(FieldUpdate()))

    if self.basedir != '':
      if not path.startswith(self.basedir):
        self.log.error(
            {
                MESSAGE: 'path without basedir passed',
                'path': path,
                'basedir': self.basedir,
            })
      else:
        path = path[len(self.basedir):]

    fu = run(path)
    if self.basedir != '' and fu.uri is None:
      fu.uri = path

    return fu


def lambda_main(event: Dict[str, Any]) -> Dict[str, Any]:
  req: Dict[str, Any] = event['Records'][0]['cf']['request']
  accept_header: str = (req[HEADERS]['accept'][0][VALUE] if 'accept' in req[HEADERS] else '')

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
