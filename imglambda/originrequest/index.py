import base64
import dataclasses
import datetime
import json
import logging
import os
import re
import sys
import time
from enum import Enum
from http import HTTPStatus
from logging import Logger
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Optional, Self, Tuple
from urllib import parse

import boto3
from botocore.exceptions import ClientError
from dateutil import parser, tz
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.type_defs import (
    GetObjectOutputTypeDef,
    HeadObjectOutputTypeDef
)
from mypy_boto3_sqs.client import SQSClient
from pathspec import PathSpec
from pathspec.patterns.gitwildmatch import GitWildMatchPattern
from pythonjsonlogger.jsonlogger import JsonFormatter
from pyvips import Extend, Image, Interesting  # type: ignore

import imglambda
from imglambda.typing import (
    HttpPath,
    OriginRequestEvent,
    Request,
    ResponseResult,
    S3Key
)

TIMESTAMP_METADATA = 'original-timestamp'
OPTIMIZE_TYPE_METADATA = 'optimize-type'
OPTIMIZE_QUALITY_METADATA = 'optimize-quality'
FOCALAREA_METADATA = 'focalarea'
SUBSIZES_METADATA = 'subsizes'
OVERRIDABLE = 'x-res-cache-control-overridable'
CACHE_CONTROL = 'x-res-cache-control'

LAMBDA_EXPIRATION_MARGIN = 60

PADDING_COLOR = [230.0, 230.0, 230.0]

long_exts = [
    '.min.css',
]

expiration_re = re.compile(r'\s*([\w-]+)="([^"]*)"(:?,|$)')
path_subsize_re = re.compile(r'-(\d+)x(\d+)(\.[A-Za-z0-9]+)$')

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


@dataclasses.dataclass(eq=True, frozen=True)
class FieldUpdate:
  reason: str
  res_cache_control: Optional[str] = None
  res_cache_control_overridable: Optional[str] = None
  origin_domain: Optional[str] = None
  uri: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class InstantResponse:
  status: int
  b64_body: Optional[str]
  cache_control: str
  content_type: Optional[str]
  vips_us: Optional[int]
  img_size: Optional[int]


class ResizeMode(Enum):
  DISABLED = 0
  STRICT = 1
  RELAXED = 2
  FREESTYLE = 3


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
  resize_mode: ResizeMode


class OptimImageType(Enum):
  WEBP = 0
  AVIF = 1

  @classmethod
  def maybe_from_s3_metadata(
      cls,
      obj: HeadObjectOutputTypeDef | GetObjectOutputTypeDef,
  ) -> Optional['OptimImageType']:
    image_type = obj['Metadata'].get(OPTIMIZE_TYPE_METADATA)
    if image_type is None:
      return cls.WEBP
    if image_type == 'none':
      return None
    if image_type == 'avif':
      return cls.AVIF
    return cls.WEBP

  @classmethod
  def maybe_from_s3_content_type(cls, obj: HeadObjectOutputTypeDef) -> Optional['OptimImageType']:
    mime = obj['ContentType']
    if mime == 'image/webp':
      return cls.WEBP
    if mime == 'image/avif':
      return cls.AVIF
    return None

  def extension(self) -> str:
    if self == OptimImageType.WEBP:
      return '.webp'
    if self == OptimImageType.AVIF:
      return '.avif'
    raise Exception('system error')


@dataclasses.dataclass(eq=True, frozen=True)
class Size:
  width: int
  height: int

  @classmethod
  def from_filename_convention(cls, s: str) -> Optional['Size']:
    ss = s.split('x')
    if len(ss) != 2:
      return None
    try:
      return cls(int(ss[0]), int(ss[1]))
    except ValueError:
      return None

  @classmethod
  def from_image(cls, image: Image) -> 'Size':
    return cls(image.get('width'), image.get('height'))


@dataclasses.dataclass(frozen=True)
class Area:
  x: int
  y: int
  width: int
  height: int

  @classmethod
  def create(cls, x: int, y: int, width: int, height: int) -> 'Area':
    if x < 0 or y < 0 or width < 0 or height < 0:
      raise ValueError(f'Invalid argument: x: {x}, y: {y}, width: {width}, height: {height}')

    return cls(x, y, width, height)

  @property
  def right(self) -> int:
    return self.x + self.width

  @property
  def bottom(self) -> int:
    return self.y + self.height

  def is_in(self, frame: Size) -> bool:
    return self.right < frame.width and self.bottom < frame.height

  def scale(self, numerator: int, denominator: int) -> 'Area':
    x = self.x * numerator // denominator
    y = self.y * numerator // denominator
    width = ((self.x + self.width) * numerator // denominator) - x
    height = ((self.y + self.height) * numerator // denominator) - y

    return Area(x, y, width, height)

  def to_size(self) -> Size:
    return Size(self.width, self.height)


@dataclasses.dataclass(eq=True, frozen=True)
class FourSides:
  left: int
  right: int
  top: int
  bottom: int

  def add_to(self, size: Size) -> Size:
    return Size(self.left + size.width + self.right, self.top + size.height + self.bottom)


NO_FOUR_SIDES = FourSides(0, 0, 0, 0)


class AcceptHeader:
  types: list[bool]

  def __init__(self, types: list[bool]):
    self.types = types

  @classmethod
  def from_str(cls, accept_header: str) -> Self:
    types = [False] * len(OptimImageType)

    if 'image/avif' in accept_header:
      types[OptimImageType.AVIF.value] = True

    if 'image/webp' in accept_header:
      types[OptimImageType.WEBP.value] = True

    return cls(types)

  def supports(self, image_type: OptimImageType) -> bool:
    return self.types[image_type.value]

  def has_response_type(self, image_type: OptimImageType) -> bool:
    return self.types[image_type.value]


class InvalidMetadata(Exception):
  pass


@dataclasses.dataclass(frozen=True)
class ObjectMeta:
  last_modified: datetime.datetime
  optimize_type: Optional[OptimImageType]
  optimize_quality: Optional[str]
  focalarea: Optional[Area]
  subsizes: frozenset[Size]

  @classmethod
  def from_original_object(
      cls,
      obj: HeadObjectOutputTypeDef | GetObjectOutputTypeDef,
  ) -> 'ObjectMeta':
    if obj['ContentType'] in ['image/jpeg', 'image/png']:
      optimize_type = OptimImageType.maybe_from_s3_metadata(obj)
    else:
      optimize_type = None

    metadata = obj['Metadata']

    if FOCALAREA_METADATA in metadata:
      fa = metadata[FOCALAREA_METADATA].split(',')
      if len(fa) == 4:
        try:
          focalarea = Area.create(int(fa[0]), int(fa[1]), int(fa[2]), int(fa[3]))
        except ValueError:
          raise InvalidMetadata(f'invalid "{FOCALAREA_METADATA}": {metadata[FOCALAREA_METADATA]}')
      else:
        raise InvalidMetadata(f'invalid "{FOCALAREA_METADATA}": {metadata[FOCALAREA_METADATA]}')
    else:
      focalarea = None

    if SUBSIZES_METADATA in metadata and 0 < len(metadata[SUBSIZES_METADATA]):
      sss = set()
      for ssstr in metadata[SUBSIZES_METADATA].split(','):
        subsize = Size.from_filename_convention(ssstr)
        if subsize is None:
          raise InvalidMetadata(f'invalid "{SUBSIZES_METADATA}": {metadata[SUBSIZES_METADATA]}')
        sss.add(subsize)
      subsizes = frozenset(sss)
    else:
      subsizes = frozenset()

    if optimize_type is None:
      optimize_quality = None
    else:
      optimize_quality = metadata.get(OPTIMIZE_QUALITY_METADATA, '80')

    return cls(
        last_modified=obj['LastModified'],
        optimize_type=optimize_type,
        optimize_quality=optimize_quality,
        focalarea=focalarea,
        subsizes=subsizes)

  @classmethod
  def maybe_from_generated_object(cls, obj: HeadObjectOutputTypeDef) -> Optional['ObjectMeta']:
    if TIMESTAMP_METADATA not in obj['Metadata']:
      return None

    return cls(
        last_modified=parser.parse(obj['Metadata'][TIMESTAMP_METADATA]),
        optimize_type=OptimImageType.maybe_from_s3_content_type(obj),
        optimize_quality=obj['Metadata'].get(OPTIMIZE_QUALITY_METADATA),
        focalarea=None,
        subsizes=frozenset())

  @staticmethod
  def need_update(original: Optional['ObjectMeta'], generated: Optional['ObjectMeta']) -> bool:
    if original is None and generated is None:
      return False

    if original is None or generated is None:
      return True

    if original.last_modified != generated.last_modified:
      return True

    if original.optimize_type != generated.optimize_type:
      return True

    if original.optimize_quality != generated.optimize_quality:
      return True

    return False


def json_dump(obj: Any) -> str:
  return json.dumps(obj, separators=(',', ':'), sort_keys=True)


def is_not_found_client_error(exception: ClientError) -> bool:
  if 'Error' not in exception.response:
    return False
  if 'Code' not in exception.response['Error']:
    return False
  return exception.response['Error']['Code'] in ['404', 'NoSuchKey']


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


def distribute_margin(lower_space: int, upper_space: int, margin: int) -> Tuple[int, int]:
  assert 0 <= lower_space and 0 <= upper_space

  if lower_space + upper_space < margin:
    remainder = margin - lower_space - upper_space

    # Blank areas are distributed equally.
    q, mod = divmod(remainder, 2)
    lower_addition = lower_space + q
    upper_addition = upper_space + q + mod
  elif margin < 0:
    q, mod = divmod(margin, 2)
    lower_addition = q
    upper_addition = q + mod
  else:
    space = lower_space + upper_space
    lower_addition = (margin * lower_space) // space
    upper_addition = (margin * upper_space) // space
    remainder = margin - lower_addition - upper_addition

    assert 0 <= remainder < 2

    if remainder == 1:
      if upper_addition < upper_space:
        upper_addition += 1
      else:
        assert lower_addition < lower_space
        lower_addition += 1

  assert margin == lower_addition + upper_addition
  return (lower_addition, upper_addition)


def max_aspect_ratios(original: Size, focalarea: Area) -> Tuple[Size, Size]:
  return (Size(original.width, focalarea.height), Size(focalarea.width, original.height))


def ceildiv(a: int, b: int) -> int:
  return -(a // -b)


def calc_resize_to(
    original: Size,
    target: Size,
    focalarea: Area,
) -> Tuple[Optional[int], Optional[int]]:
  too_thin = target.width * original.height < focalarea.width * target.height
  too_wide = original.width * target.height < target.width * focalarea.height

  match (too_thin, too_wide):
    case (True, False):
      if target.height < original.height:
        return (None, target.height)
      else:
        return (None, None)
    case (False, True):
      if target.width < original.width:
        return (target.width, None)
      else:
        return (None, None)
    case (False, False):
      if focalarea.width <= target.width and focalarea.height <= target.height:
        return (None, None)
      else:
        resize_width = ceildiv(original.width * target.width, focalarea.width)
        resize_height = ceildiv(original.height * target.height, focalarea.height)

        # Choose highest reduction to contain entire focal area in the result.
        cmp = original.width * resize_height - resize_width * original.height
        if 0 < cmp:
          return (resize_width, None)
        elif cmp < 0:
          return (None, resize_height)
        else:
          # Just to keep symmetric code
          return (resize_width, resize_height)
    case _:
      raise Exception('system error')


def calc_resize_scale(original: Size, target: Size, focalarea: Area) -> Optional[Tuple[int, int]]:
  match calc_resize_to(original, target, focalarea):
    case (None, None):
      return None
    case (int() as resize_width, None):
      return (resize_width, original.width)
    case (None, int() as resize_height):
      return (resize_height, original.height)
    case (int() as resize_width, int() as resize_height):
      assert resize_width * original.height == original.width * resize_height
      return (resize_width, original.width)
      # This should return the same value:
      #
      # return (resize_height, original.height)
    case _:
      raise Exception('system error')


def calc_crop(resized: Size, target: Size, resized_focalarea: Area) -> Area:
  left_addition, right_addition = distribute_margin(
      lower_space=resized_focalarea.x,
      upper_space=resized.width - resized_focalarea.right,
      margin=target.width - resized_focalarea.width)
  top_addition, bottom_addition = distribute_margin(
      lower_space=resized_focalarea.y,
      upper_space=resized.height - resized_focalarea.bottom,
      margin=target.height - resized_focalarea.height)

  return Area(
      x=resized_focalarea.x - left_addition,
      y=resized_focalarea.y - top_addition,
      width=left_addition + resized_focalarea.width + right_addition,
      height=top_addition + resized_focalarea.height + bottom_addition)


def calc_padding(resized: Size, croparea: Area) -> Tuple[FourSides, Area]:
  padding = FourSides(
      left=max(0, -croparea.x),
      right=max(0, croparea.right - resized.width),
      top=max(0, -croparea.y),
      bottom=max(0, croparea.bottom - resized.height))

  x = croparea.x + padding.left
  y = croparea.y + padding.top

  padded = Area(
      x=x,
      y=y,
      width=croparea.right - padding.right - x,
      height=croparea.bottom - padding.bottom - y)

  return padding, padded


@dataclasses.dataclass(frozen=True)
class ResizeParam:
  source: HttpPath
  width: int
  height: int

  @classmethod
  def maybe_from_querystring(
      cls,
      source: HttpPath,
      qs: dict[str, list[str]],
  ) -> Optional['ResizeParam']:
    if 'w' not in qs or 'h' not in qs:
      return None

    try:
      width = int(qs['w'][0])
      height = int(qs['h'][0])
    except ValueError:
      return None

    if width <= 0 or height <= 0:
      return None

    return cls(source, width, height)

  @classmethod
  def maybe_from_path(cls, path: HttpPath) -> Optional['ResizeParam']:
    m = path_subsize_re.search(str(path))
    if m is None:
      return None

    try:
      width = int(m[1])
      height = int(m[2])
    except ValueError:
      return None

    if width <= 0 or height <= 0:
      return None

    source = HttpPath(path[:m.start()] + str(m[3]))

    return cls(source, width, height)


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
      resize_mode: ResizeMode,
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
    self.resize_mode = resize_mode
    self.log_context = {'path': '', 'qstr': '', 'accept_header': ''}
    self.cache_control_perm = f'public, max-age={self.perm_resp_max_age}'
    self.cache_control_temp = f'public, max-age={self.temp_resp_max_age}'

  @classmethod
  def from_lambda(
      cls,
      log: Logger,
      req: Request,
  ) -> Optional['ImgServer']:
    expiration_margin: int = LAMBDA_EXPIRATION_MARGIN

    try:
      region = get_header(req, 'x-env-region')
      generated_domain = get_header(req, 'x-env-generated-domain')
      original_bucket = req['origin']['s3']['domainName'].split('.', 1)[0]
      generated_key_prefix = get_header(req, 'x-env-generated-key-prefix')
      sqs_queue_url = get_header(req, 'x-env-sqs-queue-url')
      perm_resp_max_age = int(get_header(req, 'x-env-perm-resp-max-age'))
      temp_resp_max_age = int(get_header(req, 'x-env-temp-resp-max-age'))
      bypass_minifier_patterns = get_header_or(req, 'x-env-bypass-minifier-patterns')
      basedir = get_header_or(req, 'x-env-basedir')
      resize_mode = ResizeMode[get_header_or(req, 'x-env-resize-mode', 'Disabled').upper()]
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
        basedir=basedir,
        resize_mode=resize_mode)

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
          basedir=basedir,
          resize_mode=resize_mode)

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

  def get_meta_original(self, key: S3Key) -> Optional[ObjectMeta]:
    try:
      res = self.s3.head_object(Bucket=self.original_bucket, Key=key)
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e
    try:
      return ObjectMeta.from_original_object(res)
    except InvalidMetadata as e:
      self.log_warning('failed to read orig metadata', {'reason': str(e), 'key': key})
      return None

  def gen_path_from_path(self, path: HttpPath) -> HttpPath:
    return HttpPath(f'/{self.generated_key_prefix}{path[1:]}')

  def gen_key_from_key(self, key: S3Key) -> S3Key:
    return S3Key(f'{self.generated_key_prefix}{key}')

  def object_expired(
      self,
      now: datetime.datetime,
      expiration: str,
  ) -> bool:
    d = parse_expiration(expiration)
    exp_str = d.get('expiry-date', None)
    if exp_str is None:
      self.log_warning('expiry-date not found', {'expiration': expiration})
      return False

    exp = parser.parse(exp_str)
    return exp < now + self.expiration_margin

  def get_meta_generated(
      self,
      now: datetime.datetime,
      key: S3Key,
  ) -> Optional[ObjectMeta]:
    key = self.gen_key_from_key(key)
    try:
      res = self.s3.head_object(Bucket=self.generated_bucket, Key=key)
      if 'Expiration' in res and self.object_expired(now, res['Expiration']):
        self.log_debug('expired object found', {'expiration': res['Expiration']})
        return None
    except ClientError as e:
      if is_not_found_client_error(e):
        return None
      raise e

    return ObjectMeta.maybe_from_generated_object(res)

  def keep_uptodate(self, key: S3Key) -> None:
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

  def resize_image(self, filename: str, target: Size, focalarea: Optional[Area]) -> Optional[Image]:
    if focalarea is None:
      return Image.thumbnail(filename, target.width, height=target.height, crop=Interesting.CENTRE)

    image: Image = Image.new_from_file(filename)
    original = Size.from_image(image)

    if not focalarea.is_in(original):
      return None

    scale = calc_resize_scale(original, target, focalarea)
    if scale is None:
      resized = original
      resized_focalarea = focalarea
    else:
      assert scale[0] < scale[1]
      image = image.resize(scale[0] / scale[1])
      resized = Size.from_image(image)
      resized_focalarea = focalarea.scale(scale[0], scale[1])

    croparea = calc_crop(resized, target, resized_focalarea)
    padding, padded = calc_padding(resized, croparea)

    image = image.extract_area(padded.x, padded.y, padded.width, padded.height)

    if padding != NO_FOUR_SIDES:
      size = padding.add_to(padded.to_size())
      image = image.embed(
          padding.left,
          padding.top,
          size.width,
          size.height,
          extend=Extend.BACKGROUND,
          background=PADDING_COLOR)

    self.log_debug(
        'resize param', {
            'original': original,
            'target': target,
            'focalarea': focalarea,
            'resized_focalarea': resized_focalarea,
            'scale': scale,
            'resized': resized,
            'croparea': croparea,
            'padding': padding,
            'padded': padded,
        })

    return image

  def process_resize(
      self,
      resize_param: ResizeParam,
      accept: AcceptHeader,
      strict: bool,
  ) -> Optional[InstantResponse]:
    key = key_from_path(resize_param.source)
    if accept.supports(OptimImageType.WEBP):
      extension = '.webp'
      quality = 80
      content_type = 'image/webp'
    else:
      extension = Path(resize_param.source).suffix
      quality = 80
      if extension in ['.jpg', '.jpeg']:
        content_type = 'image/jpeg'
      elif extension == '.png':
        content_type = 'image/png'
      else:
        raise Exception('system error')

    with NamedTemporaryFile(delete_on_close=False) as orig:
      try:
        res = self.s3.get_object(Bucket=self.original_bucket, Key=key)
        for chunk in res['Body'].iter_chunks():
          orig.write(chunk)
      except ClientError as e:
        if is_not_found_client_error(e):
          return None
        raise e
      orig.close()

      try:
        orig_meta = ObjectMeta.from_original_object(res)
      except InvalidMetadata as e:
        self.log_warning('failed to read orig metadata', {'reason': str(e), 'key': key})
        return None

      target = Size(resize_param.width, resize_param.height)
      if strict and target not in orig_meta.subsizes:
        return None

      start_ns = time.time_ns()

      try:
        match self.resize_image(orig.name, target, orig_meta.focalarea):
          case None:
            return None
          case Image() as image:
            resized: bytes = image.write_to_buffer(extension, Q=quality)

            vips_us = (time.time_ns() - start_ns) // 1000

            return InstantResponse(
                status=HTTPStatus.OK,
                b64_body=base64.b64encode(resized).decode(),
                cache_control=f'public, max-age={self.perm_resp_max_age}',
                content_type=content_type,
                vips_us=vips_us,
                img_size=len(resized))
          case _:
            raise Exception('system error')
      except Exception as e:
        self.log_warning('failed to resize', {'reason': str(e), 'key': key})
        return None

  def process_for_image_requester(
      self,
      path: HttpPath,
      qs: dict[str, list[str]],
      accept_header: AcceptHeader,
  ) -> FieldUpdate | InstantResponse:
    match self.resize_mode:
      case ResizeMode.DISABLED:
        pass
      case ResizeMode.STRICT:
        resize_param = ResizeParam.maybe_from_path(path)
        if resize_param is not None:
          instant_res = self.process_resize(resize_param, accept_header, True)
          if instant_res is not None:
            return instant_res
      case ResizeMode.RELAXED:
        resize_param = ResizeParam.maybe_from_path(path)
        if resize_param is not None:
          instant_res = self.process_resize(resize_param, accept_header, False)
          if instant_res is not None:
            return instant_res
      case ResizeMode.FREESTYLE:
        resize_param = ResizeParam.maybe_from_querystring(path, qs)
        if resize_param is not None:
          instant_res = self.process_resize(resize_param, accept_header, False)
          if instant_res is not None:
            return instant_res

    try:
      now = get_now()

      key = key_from_path(path)
      orig_meta = self.get_meta_original(key)

      if orig_meta is None:
        for image_type in OptimImageType:
          gen_meta = self.get_meta_generated(now, S3Key(f'{key}{image_type.extension()}'))
          if ObjectMeta.need_update(orig_meta, gen_meta):
            self.keep_uptodate(key)

        return FieldUpdate(reason='no orig', res_cache_control=self.cache_control_temp)

      if orig_meta.optimize_type is None:
        return FieldUpdate(reason='opt disabled by orig', res_cache_control=self.cache_control_perm)

      gen_meta = self.get_meta_generated(now, S3Key(f'{key}{orig_meta.optimize_type.extension()}'))

      need_update = ObjectMeta.need_update(orig_meta, gen_meta)

      if need_update:
        self.keep_uptodate(key)

      if not accept_header.has_response_type(orig_meta.optimize_type):
        return FieldUpdate(reason='opt not accepted', res_cache_control=self.cache_control_perm)

      if need_update:
        if gen_meta is None:
          return FieldUpdate(res_cache_control=self.cache_control_temp, reason='no gen')
        return FieldUpdate(res_cache_control=self.cache_control_temp, reason='gen is stale')

      return FieldUpdate(
          reason='gen found',
          res_cache_control=self.cache_control_perm,
          origin_domain=self.generated_domain,
          uri=self.gen_path_from_path(HttpPath(f'{path}{orig_meta.optimize_type.extension()}')))
    except Exception as e:
      self.log_error('error during process_for_image_requester()', {'reason': str(e)})
      return FieldUpdate(reason='error occurred', res_cache_control=self.cache_control_temp)

  def process_for_css_requester(self, path: HttpPath) -> FieldUpdate:
    key = key_from_path(path)
    try:
      now = get_now()
      gen_meta = self.get_meta_generated(now, key)
      orig_meta = self.get_meta_original(key)

      need_update = ObjectMeta.need_update(orig_meta, gen_meta)
      if need_update:
        self.keep_uptodate(key)

      if orig_meta is None:
        return FieldUpdate(reason='no orig', res_cache_control=self.cache_control_temp)

      if need_update:
        if gen_meta is None:
          return FieldUpdate(reason='no gen', res_cache_control=self.cache_control_temp)
        return FieldUpdate(reason='gen is stale', res_cache_control=self.cache_control_temp)

      return FieldUpdate(
          reason='gen found',
          res_cache_control=self.cache_control_perm,
          origin_domain=self.generated_domain,
          uri=self.gen_path_from_path(path))
    except Exception as e:
      self.log_error('error during process_for_css_requester()', {'reason': str(e)})
      return FieldUpdate(reason='error occurred', res_cache_control=self.cache_control_temp)

  def process(
      self,
      path: HttpPath,
      qs: dict[str, list[str]],
      accept_header: AcceptHeader,
  ) -> FieldUpdate | InstantResponse:

    def run(path: HttpPath):
      if self.bypass_path_spec is not None and self.bypass_path_spec.match_file(path):
        return FieldUpdate(
            reason='bypassed',
            res_cache_control=self.cache_control_perm,
            res_cache_control_overridable='true')

      ext = get_normalized_extension(path)

      if ext in ['.jpg', '.jpeg', '.png']:
        return self.process_for_image_requester(path, qs, accept_header)
      elif ext == '.css':
        return self.process_for_css_requester(path)
      else:
        # This condition includes:
        #   '.min.css'
        return FieldUpdate(
            reason='noprocess',
            res_cache_control=self.cache_control_perm,
            res_cache_control_overridable='true')

    if self.basedir != '':
      if not path.startswith(self.basedir):
        self.log_error('path without basedir passed', {'basedir': self.basedir})
      else:
        path = HttpPath(path[len(self.basedir):])

    fu = run(path)
    if isinstance(fu, FieldUpdate) and self.basedir != '' and fu.uri is None:
      fu = FieldUpdate(
          reason=fu.reason,
          res_cache_control=fu.res_cache_control,
          res_cache_control_overridable=fu.res_cache_control_overridable,
          uri=path)

    return fu

  def set_log_context(self, path: HttpPath, qstr: str, accept_header: str) -> None:
    self.log_context = {'path': str(path), 'qstr': qstr, 'accept_header': accept_header}


def lambda_main(event: OriginRequestEvent) -> Request | ResponseResult:
  req = event['Records'][0]['cf']['request']
  accept_header = req['headers']['accept'][0]['value'] if 'accept' in req['headers'] else ''

  # Set default value
  req['headers'][CACHE_CONTROL] = [{'value': ''}]
  req['headers'][OVERRIDABLE] = [{'value': ''}]

  server = ImgServer.from_lambda(logger, req)
  if server is None:
    return req

  path = req['uri']
  qstr = req['querystring']

  server.set_log_context(path, qstr, accept_header)
  result = server.process(path, parse.parse_qs(qstr), AcceptHeader.from_str(accept_header))

  if isinstance(result, FieldUpdate):
    if result.origin_domain is not None:
      req['origin']['s3']['domainName'] = result.origin_domain
      req['headers']['host'][0]['value'] = result.origin_domain

    if result.uri is not None:
      req['uri'] = HttpPath(result.uri)

    if result.res_cache_control is not None:
      req['headers'][CACHE_CONTROL] = [
          {
              'value': result.res_cache_control,
          },
      ]

    if result.res_cache_control_overridable is not None:
      req['headers'][OVERRIDABLE] = [
          {
              'value': result.res_cache_control_overridable,
          },
      ]

    server.log_debug(
        'done', {
            'uri': req['uri'],
            'origin_domain': req['origin']['s3']['domainName'],
            'res_cache_control': req['headers'][CACHE_CONTROL][0]['value'],
            'res_cache_control_overridable': req['headers'][OVERRIDABLE][0]['value'],
            'reason': result.reason,
        })

    return req
  elif isinstance(result, InstantResponse):
    response_result: ResponseResult = {
        'status': str(result.status),
        'headers': {
            'cache-control': [{
                'value': result.cache_control,
            }],
        },
    }

    if result.content_type is not None:
      response_result['headers']['content-type'] = [{'value': result.content_type}]

    if result.b64_body is not None:
      response_result['body'] = result.b64_body
      response_result['bodyEncoding'] = 'base64'

    server.log_debug(
        'responded', {
            'uri': req['uri'],
            'status': result.status,
            'cache_control': result.cache_control,
            'content_type': result.content_type,
            'img_size': result.img_size,
            'vips_us': result.vips_us,
        })

    return response_result
  else:
    raise Exception('system error')
