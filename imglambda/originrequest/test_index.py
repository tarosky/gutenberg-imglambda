import base64
import datetime
import json
import logging
import mimetypes
import secrets
import time
import warnings
from collections import OrderedDict
from http import HTTPStatus
from logging import Logger
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    NotRequired,
    Optional,
    Tuple,
    TypedDict
)

import boto3
import pytest
from mypy_boto3_sqs.type_defs import MessageTypeDef
from pathspec import PathSpec
from pathspec.patterns.gitwildmatch import GitWildMatchPattern
from pyvips import Image  # type: ignore

from imglambda.typing import HttpPath

from .index import (
    OPTIMIZE_QUALITY_METADATA,
    OPTIMIZE_TYPE_METADATA,
    TIMESTAMP_METADATA,
    AcceptHeader,
    FieldUpdate,
    ImgServer,
    InstantResponse,
    MyJsonFormatter,
    OptimImageType
)

PERM_RESP_MAX_AGE = 365 * 24 * 60 * 60
TEMP_RESP_MAX_AGE = 20 * 60
GENERATED_KEY_PREFIX = 'prefix/'
REGION = 'us-east-1'

CSS_MIME = 'text/css'
GIF_MIME = 'image/gif'
JPEG_MIME = 'image/jpeg'
PNG_MIME = 'image/png'
WEBP_MIME = "image/webp"
AVIF_MIME = "image/avif"

CSS_NAME = 'スタイル.css'
CSS_NAME_Q = '%E3%82%B9%E3%82%BF%E3%82%A4%E3%83%AB.css'
JPG_NAME = 'image.jpg'
JPG_NAME_U = 'image.JPG'
JPG_NAME_MB = 'テスト.jpg'
JPG_NAME_MB_Q = '%E3%83%86%E3%82%B9%E3%83%88.jpg'
JPG_WEBP_NAME = 'image.jpg.webp'
JPG_AVIF_NAME = 'image.jpg.avif'
JPG_WEBP_NAME_U = 'image.JPG.webp'
JPG_WEBP_NAME_MB = 'テスト.jpg.webp'
JPG_WEBP_NAME_MB_Q = '%E3%83%86%E3%82%B9%E3%83%88.jpg.webp'
JPG_NOMINIFY_NAME = 'nominify/foo/bar/image.jpg'
PNG_NAME = 'image.png'
MIN_CSS_NAME = 'スタイル.min.css'
MIN_CSS_NAME_Q = '%E3%82%B9%E3%82%BF%E3%82%A4%E3%83%AB.min.css'
BROKEN_JPG_NAME = 'broken-image.jpg'

DUMMY_DATETIME = datetime.datetime(2000, 1, 1)

CACHE_CONTROL_PERM = f'public, max-age={PERM_RESP_MAX_AGE}'
CACHE_CONTROL_TEMP = f'public, max-age={TEMP_RESP_MAX_AGE}'

WEBP_EXTENSION = '.webp'
AVIF_EXTENSION = '.avif'

OLD_SAFARI_ACCEPT_HEADER = AcceptHeader([False] * len(OptimImageType))

LOADER_MAP = {
    'jpegload': 'image/jpeg',
    'jpegload_buffer': 'image/jpeg',
    'pngload': 'image/png',
    'pngload_buffer': 'image/png',
    'webpload': 'image/webp',
    'webpload_buffer': 'image/webp',
    'heifload': 'image/avif',
    'heifload_buffer': 'image/avif',
}


def get_bypass_minifier_patterns(key_prefix: str) -> list[str]:
  return [
      f'/{key_prefix}nominify/**',
  ]


def read_test_config(name: str) -> str:
  path = f'config/test/{name}'
  with open(path, 'r') as f:
    return f.read().strip()


def generate_safe_random_string() -> str:
  return secrets.token_urlsafe(256 // 8)


def create_img_server(
    log: Logger, name: str, expiration_margin: int, key_prefix: str, basedir: str) -> ImgServer:
  account_id = read_test_config('aws-account-id')
  sqs_name = f'test-{name}-{generate_safe_random_string()}'

  sess = boto3.Session(
      aws_access_key_id=read_test_config('access-key-id'),
      aws_secret_access_key=read_test_config('secret-access-key'))
  # https://github.com/boto/boto3/issues/454#issuecomment-380900404
  warnings.filterwarnings('ignore', category=ResourceWarning, message='unclosed.*<ssl.SSLSocket.*>')
  sqs = sess.client('sqs', region_name=REGION)
  s3 = sess.client('s3', region_name=REGION)

  return ImgServer(
      log=log,
      region=REGION,
      sqs=sqs,
      s3=s3,
      generated_domain=f"{read_test_config('generated-bucket')}.s3.example.com",
      original_bucket=read_test_config('original-bucket'),
      generated_key_prefix=GENERATED_KEY_PREFIX,
      sqs_queue_url=f'https://sqs.{REGION}.amazonaws.com/{account_id}/{sqs_name}',
      perm_resp_max_age=PERM_RESP_MAX_AGE,
      temp_resp_max_age=TEMP_RESP_MAX_AGE,
      bypass_path_spec=PathSpec.from_lines(
          GitWildMatchPattern, get_bypass_minifier_patterns(key_prefix)),
      expiration_margin=expiration_margin,
      basedir=basedir,
      enable_resize=True)


def get_test_sqs_queue_name_from_url(sqs_queue_url: str) -> str:
  return sqs_queue_url.split('/')[-1]


def clean_test_environment(img_server: ImgServer) -> None:
  img_server.sqs.delete_queue(QueueUrl=img_server.sqs_queue_url)


def put_original(
    img_server: ImgServer,
    key_prefix: str,
    name: str,
    mime: str,
    metadata: dict[str, str],
) -> datetime.datetime:
  key = f'{key_prefix}{name}'
  path = f'samplefile/original/{name}'
  with open(path, 'rb') as f:
    img_server.s3.put_object(
        Body=f,
        Bucket=img_server.original_bucket,
        ContentType=mime,
        Key=key,
        Metadata=metadata,
    )

  res = img_server.s3.head_object(Bucket=img_server.original_bucket, Key=key)
  return res['LastModified']


def put_generated(
    img_server: ImgServer,
    key_prefix: str,
    name: str,
    mime: str,
    timestamp: Optional[datetime.datetime],
    metadata: dict[str, str],
) -> None:
  metadata2 = {}
  if timestamp is not None:
    metadata2[TIMESTAMP_METADATA] = timestamp.astimezone(
        datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

  with open(f'samplefile/generated/{name}', 'rb') as f:
    img_server.s3.put_object(
        Body=f,
        Bucket=img_server.generated_bucket,
        ContentType=mime,
        Key=f'{img_server.generated_key_prefix}{key_prefix}{name}',
        Metadata={
            **metadata2,
            **metadata,
        })


def receive_sqs_message(img_server: ImgServer) -> Optional[dict[str, Any]]:
  time.sleep(1.0)
  res = img_server.sqs.receive_message(
      QueueUrl=img_server.sqs_queue_url,
      MaxNumberOfMessages=1,
      VisibilityTimeout=1,
      WaitTimeSeconds=1)
  msgs: Optional[List[MessageTypeDef]] = res.get('Messages', None)
  if msgs is None or len(msgs) == 0 or 'Body' not in msgs[0]:
    return None

  return json.loads(msgs[0]['Body'])


@pytest.fixture
def key_prefix() -> str:
  return generate_safe_random_string() + '/'


@pytest.fixture
def logger(key_prefix: str) -> Logger:
  log = logging.getLogger(__name__)

  log_dir = f'work/test/imglambda/{key_prefix}'
  Path(log_dir).mkdir(parents=True, exist_ok=True)
  log_file = open(f'{log_dir}/test.log', 'w')

  log_handler = logging.StreamHandler()
  log_handler.setFormatter(MyJsonFormatter())
  log_handler.setLevel(logging.DEBUG)
  log_handler.setStream(log_file)
  log.addHandler(log_handler)

  return log


@pytest.fixture
def img_server(request: Any, key_prefix: str, logger: Logger) -> Generator[ImgServer, None, None]:
  expiration_margin: int = request.param['expiration_margin']
  basedir: str = request.param['basedir']

  img_server = create_img_server(logger, 'imglambda', expiration_margin, key_prefix, basedir)
  img_server.sqs.create_queue(QueueName=get_test_sqs_queue_name_from_url(img_server.sqs_queue_url))

  yield img_server

  clean_test_environment(img_server)


def assert_sqs_message(img_server: ImgServer, key_prefix: str, key: str) -> None:
  assert {
      'version': 2,
      'path': key_prefix + key,
      'src': {
          'bucket': img_server.original_bucket,
          'prefix': '',
      },
      'dest': {
          'bucket': img_server.generated_bucket,
          'prefix': img_server.generated_key_prefix,
      },
  } == receive_sqs_message(img_server)


@pytest.fixture
def to_path(key_prefix: str) -> Callable[[str], str]:

  def fn(name: str) -> str:
    return f'/{key_prefix}{name}'

  return fn


@pytest.fixture
def to_uri(img_server: ImgServer, key_prefix: str) -> Callable[[str], str]:

  def fn(name: str) -> str:
    return f'/{img_server.generated_key_prefix}{key_prefix}{name}'

  return fn


@pytest.fixture
def field_update(
    request: Any, img_server: ImgServer, to_path: Callable[[str], str],
    to_uri: Callable[[str], str]) -> Optional[FieldUpdate]:
  if request.param is None:
    return None

  res_cache_control: Optional[str] = request.param['res_cache_control']
  res_cache_control_overridable: Optional[str] = request.param['res_cache_control_overridable']
  origin_domain: bool = request.param['origin_domain']
  uri: Optional[str] = request.param['uri']
  uri_for_generated: bool = request.param['uri_for_generated']

  return FieldUpdate(
      res_cache_control=res_cache_control,
      res_cache_control_overridable=res_cache_control_overridable,
      origin_domain=img_server.generated_domain if origin_domain else None,
      uri=None if uri is None else (to_uri(uri) if uri_for_generated else to_path(uri)))


@pytest.fixture
def sqs_message(request: Any, img_server: ImgServer, key_prefix: str) -> Optional[dict[str, Any]]:
  key: Optional[str] = request.param['key']

  return None if key is None else {
      'version': 2,
      'path': key_prefix + key,
      'src': {
          'bucket': img_server.original_bucket,
          'prefix': '',
      },
      'dest': {
          'bucket': img_server.generated_bucket,
          'prefix': img_server.generated_key_prefix,
      },
  }


@pytest.fixture
def request_path(request: Any, to_path: Callable[[str], str]) -> HttpPath:
  prefix: str = request.param['prefix']
  name: str = request.param['name']

  return HttpPath(f'{prefix}{to_path(name)}')


def file_type(name: str) -> str:
  mime = mimetypes.guess_file_type(name)[0]
  if mime is None:
    raise ValueError(f'cannot guess file type: {name}')

  return mime


class ExpectedSqsMessage(TypedDict):
  key: str


class ExpectedFieldUpdate(TypedDict):
  res_cache_control: NotRequired[str]
  res_cache_control_overridable: NotRequired[str]
  origin_domain: NotRequired[bool]
  uri: NotRequired[str | Tuple[bool, str]]


class ExpectedInstantResponse(TypedDict):
  status: int
  cache_control: str
  content_type: NotRequired[str]
  size: NotRequired[Tuple[int, int]]


class Config(TypedDict):
  expiration_margin: NotRequired[int]
  basedir: NotRequired[str]


class Parameters(TypedDict):
  id: str
  original: None | str | tuple[str, str] | tuple[str, str, Dict[str, str]]
  generated: None | str | tuple[str, str] | tuple[str, str, Dict[str, str]] | tuple[str, str, Dict[
      str, str], Callable[[datetime.datetime], datetime.datetime]]
  config: NotRequired[Config]
  request_path: str | tuple[str, str]
  query_string: NotRequired[dict[str, str]]
  accept_header: NotRequired[AcceptHeader]
  expected_field_update: NotRequired[ExpectedFieldUpdate]
  expected_instant_response: NotRequired[ExpectedInstantResponse]
  expected_sqs_message: NotRequired[ExpectedSqsMessage]


def parameters(params: List[Parameters]) -> Dict[str, Any]:
  values: List[OrderedDict] = []
  indirects = []
  ids = []

  for param in params:
    ids.append(param['id'])

    v: OrderedDict[str, Any] = OrderedDict()

    original = param['original']
    if original is None:
      v['original_name'] = None
      v['original_mime'] = ''
      v['original_metadata'] = {}
    elif isinstance(original, str):
      v['original_name'] = original
      v['original_mime'] = file_type(original)
      v['original_metadata'] = {}
    elif isinstance(original, tuple) and len(original) == 2:
      v['original_name'] = original[0]
      v['original_mime'] = original[1]
      v['original_metadata'] = {}
    elif isinstance(original, tuple) and len(original) == 3:
      v['original_name'] = original[0]
      v['original_mime'] = original[1]
      v['original_metadata'] = original[2]
    else:
      raise Exception('system error')

    generated = param['generated']
    if generated is None:
      v['generated_name'] = None
      v['generated_mime'] = ''
      v['generated_metadata'] = {}
      v['modify_ts'] = None
    elif isinstance(generated, str):
      v['generated_name'] = generated
      v['generated_mime'] = file_type(generated)
      v['generated_metadata'] = {}
      v['modify_ts'] = None
    elif isinstance(generated, tuple) and len(generated) == 2:
      v['generated_name'] = generated[0]
      v['generated_mime'] = generated[1]
      v['generated_metadata'] = {}
      v['modify_ts'] = None
    elif isinstance(generated, tuple) and len(generated) == 3:
      v['generated_name'] = generated[0]
      v['generated_mime'] = generated[1]
      v['generated_metadata'] = generated[2]
      v['modify_ts'] = None
    elif isinstance(generated, tuple) and len(generated) == 4:
      v['generated_name'] = generated[0]
      v['generated_mime'] = generated[1]
      v['generated_metadata'] = generated[2]
      v['modify_ts'] = generated[3]
    else:
      raise Exception('system error')

    v['accept_header'] = param.get('accept_header', AcceptHeader([True] * len(OptimImageType)))

    request_path = param['request_path']
    if isinstance(request_path, str):
      prefix = ''
      name = request_path
    elif isinstance(request_path, tuple):
      prefix = request_path[0]
      name = request_path[1]
    else:
      raise Exception('system error')
    indirects.append('request_path')
    v['request_path'] = {
        'prefix': prefix,
        'name': name,
    }

    query_string = param.get('query_string', {})
    v['query_string'] = {}
    for key, value in query_string.items():
      v['query_string'][key] = [value]

    config = param.get('config', {})
    indirects.append('img_server')
    v['img_server'] = {
        'expiration_margin': config.get('expiration_margin', 10),
        'basedir': config.get('basedir', ''),
    }

    efu = param.get('expected_field_update')
    indirects.append('field_update')
    if efu is None:
      v['field_update'] = None
    else:
      if 'uri' in efu and isinstance(efu['uri'], str):
        uri = efu['uri']
        uri_for_generated = True
      elif 'uri' in efu and isinstance(efu['uri'], tuple):
        uri = efu['uri'][1]
        uri_for_generated = efu['uri'][0]
      elif 'uri' not in efu:
        uri = None
        uri_for_generated = False
      else:
        raise Exception('system error')
      v['field_update'] = {
          'res_cache_control': efu.get('res_cache_control', None),
          'res_cache_control_overridable': efu.get('res_cache_control_overridable', None),
          'origin_domain': efu.get('origin_domain', False),
          'uri': uri,
          'uri_for_generated': uri_for_generated,
      }

    eir = param.get('expected_instant_response')
    if eir is None:
      v['instant_response'] = None
    else:
      v['instant_response'] = eir

    indirects.append('sqs_message')
    v['sqs_message'] = {
        'key': param['expected_sqs_message']['key'] if 'expected_sqs_message' in param else None,
    }

    values.append(v)

  return {
      'argnames': list(values[0].keys()),
      'argvalues': [list(v.values()) for v in values],
      'indirect': indirects,
      'ids': ids,
  }


@pytest.mark.parametrize(
    **parameters(
        [
            {
                'id': 'basedir',
                'original': JPG_NAME,
                'generated': JPG_WEBP_NAME,
                'config': {
                    'basedir': '/blog',
                },
                'request_path': ('/blog', JPG_NAME),
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'origin_domain': True,
                    'uri': f'{JPG_NAME}.webp',
                },
            },
            {
                'id': 'basedir/bypass',
                'original': JPG_NAME,
                'generated': JPG_WEBP_NAME,
                'config': {
                    'basedir': '/blog',
                },
                'request_path': ('/blog', JPG_NOMINIFY_NAME),  # <-
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'res_cache_control_overridable': 'true',
                    'uri': (False, JPG_NOMINIFY_NAME),  # <- bypass
                },
            },
            {
                'id': 'basedir/no_basedir_generated',
                'original': JPG_NAME,
                'generated': JPG_WEBP_NAME,
                'config': {
                    'basedir': '/blog',
                },
                'request_path': ('', JPG_NAME),  # <- has no basedir but still works
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'origin_domain': True,
                    'uri': f'{JPG_NAME}.webp',  # <-
                },
            },
            {
                'id': 'expired',
                'original': JPG_NAME,
                'generated': JPG_WEBP_NAME,
                'config': {
                    'expiration_margin': 60 * 60 * 24 * 2,
                },
                'request_path': JPG_NAME,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME,
                },
            },
            {
                # Test_JPGAcceptedS3EFS_L
                'id': 'jpg/accepted/gen/orig/l',
                'original': JPG_NAME,
                'generated': JPG_WEBP_NAME,
                'request_path': JPG_NAME,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'origin_domain': True,
                    'uri': f'{JPG_NAME}{WEBP_EXTENSION}',
                },
            },
            {
                # Test_JPGAcceptedS3EFS_U
                'id': 'jpg/accepted/gen/orig/u',
                'original': JPG_NAME_U,
                'generated': JPG_WEBP_NAME_U,
                'request_path': JPG_NAME_U,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'origin_domain': True,
                    'uri': f'{JPG_NAME_U}{WEBP_EXTENSION}',
                },
            },
            {
                # Test_JPGAcceptedS3EFS_MB
                'id': 'jpg/accepted/gen/orig/mb',
                'original': JPG_NAME_MB,
                'generated': JPG_WEBP_NAME_MB,
                'request_path': JPG_NAME_MB_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'origin_domain': True,
                    'uri': f'{JPG_NAME_MB_Q}{WEBP_EXTENSION}',
                },
            },
            {
                'id': 'jpg/accepted/gen/orig/avif',
                'original': (
                    JPG_NAME, JPEG_MIME, {
                        OPTIMIZE_TYPE_METADATA: 'avif',
                        OPTIMIZE_QUALITY_METADATA: '60',
                    }),
                'generated': (JPG_AVIF_NAME, AVIF_MIME, {
                    OPTIMIZE_QUALITY_METADATA: '60',
                }),
                'request_path': JPG_NAME,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'origin_domain': True,
                    'uri': f'{JPG_NAME}{AVIF_EXTENSION}',
                },
            },
            {
                # Test_JPGAcceptedS3NoEFS_L
                'id': 'jpg/accepted/gen/no_orig/l/webp',
                'original': None,
                'generated': JPG_WEBP_NAME,
                'request_path': JPG_NAME,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME,
                },
            },
            {
                'id': 'jpg/accepted/gen/no_orig/l/avif',
                'original': None,
                'generated': JPG_AVIF_NAME,
                'request_path': JPG_NAME,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME,
                },
            },
            {
                # Test_JPGAcceptedS3NoEFS_U
                'id': 'jpg/accepted/gen/no_orig/u',
                'original': None,
                'generated': JPG_WEBP_NAME_U,
                'request_path': JPG_NAME_U,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME_U,
                },
            },
            {
                # Test_JPGAcceptedS3NoEFS_MB
                'id': 'jpg/accepted/gen/no_orig/mb',
                'original': None,
                'generated': JPG_WEBP_NAME_MB,
                'request_path': JPG_NAME_MB_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME_MB,
                },
            },
            {
                'id': 'jpg/accepted/quality_mismatch',
                'original': (
                    JPG_NAME, JPEG_MIME, {
                        OPTIMIZE_TYPE_METADATA: 'avif',
                        OPTIMIZE_QUALITY_METADATA: '60',
                    }),
                'generated': (JPG_AVIF_NAME, AVIF_MIME, {
                    OPTIMIZE_QUALITY_METADATA: '50'
                }),
                'request_path': JPG_NAME,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME,
                },
            },
            {
                # Test_JPGAcceptedNoS3EFS_L
                'id': 'jpg/accepted/no_gen/orig/l',
                'original': JPG_NAME,
                'generated': None,
                'request_path': JPG_NAME,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME,
                },
            },
            {
                'id': 'jpg/accepted/no_gen/orig/avif',
                'original': (
                    JPG_NAME, JPEG_MIME, {
                        OPTIMIZE_TYPE_METADATA: 'avif',
                        OPTIMIZE_QUALITY_METADATA: '60'
                    }),
                'generated': None,
                'request_path': JPG_NAME,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME,
                },
            },
            {
                # Test_JPGAcceptedNoS3EFS_U
                'id': 'jpg/accepted/no_gen/orig/u',
                'original': JPG_NAME_U,
                'generated': None,
                'request_path': JPG_NAME_U,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME_U,
                },
            },
            {
                # Test_JPGAcceptedNoS3EFS_MB
                'id': 'jpg/accepted/no_gen/orig/mb',
                'original': JPG_NAME_MB,
                'generated': None,
                'request_path': JPG_NAME_MB_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME_MB,
                },
            },
            {
                # Test_JPGAcceptedNoS3NoEFS_L
                'id': 'jpg/accepted/no_gen/no_orig/l',
                'original': None,
                'generated': None,
                'request_path': JPG_NAME,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
            },
            {
                # Test_JPGAcceptedNoS3NoEFS_U
                'id': 'jpg/accepted/no_gen/no_orig/u',
                'original': None,
                'generated': None,
                'request_path': JPG_NAME_U,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
            },
            {
                # Test_JPGAcceptedNoS3NoEFS_MB
                'id': 'jpg/accepted/no_gen/no_orig/mb',
                'original': None,
                'generated': None,
                'request_path': JPG_NAME_MB_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
            },
            {
                # Test_JPGUnacceptedS3EFS_L
                'id': 'jpg/unaccepted/gen/orig/l',
                'original': JPG_NAME,
                'generated': JPG_WEBP_NAME,
                'request_path': JPG_NAME,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                },
            },
            {
                'id': 'jpg/unaccepted/gen/orig/avif',
                'original': (
                    JPG_NAME, JPEG_MIME, {
                        OPTIMIZE_TYPE_METADATA: 'avif',
                        OPTIMIZE_QUALITY_METADATA: '60'
                    }),
                'generated': (JPG_AVIF_NAME, AVIF_MIME, {
                    OPTIMIZE_QUALITY_METADATA: '60'
                }),
                'request_path': JPG_NAME,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                },
            },
            {
                # Test_JPGUnacceptedS3EFS_U
                'id': 'jpg/unaccepted/gen/orig/u',
                'original': JPG_NAME_U,
                'generated': JPG_WEBP_NAME_U,
                'request_path': JPG_NAME_U,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                },
            },
            {
                # Test_JPGUnacceptedS3EFS_MB
                'id': 'jpg/unaccepted/gen/orig/mb',
                'original': JPG_NAME_MB,
                'generated': JPG_WEBP_NAME_MB,
                'request_path': JPG_NAME_MB_Q,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                },
            },
            {
                # Test_JPGUnacceptedS3NoEFS_L
                'id': 'jpg/unaccepted/gen/no_orig/l',
                'original': None,
                'generated': JPG_WEBP_NAME,
                'request_path': JPG_NAME,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME,
                },
            },
            {
                'id': 'jpg/unaccepted/gen/no_orig/avif',
                'original': None,
                'generated': JPG_AVIF_NAME,
                'request_path': JPG_NAME,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME,
                },
            },
            {
                # Test_JPGUnacceptedS3NoEFS_U
                'id': 'jpg/unaccepted/gen/no_orig/u',
                'original': None,
                'generated': JPG_WEBP_NAME_U,
                'request_path': JPG_NAME_U,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME_U,
                },
            },
            {
                # Test_JPGUnacceptedS3NoEFS_MB
                'id': 'jpg/unaccepted/gen/no_orig/mb',
                'original': None,
                'generated': JPG_WEBP_NAME_MB,
                'request_path': JPG_NAME_MB_Q,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME_MB,
                },
            },
            {
                # Test_JPGUnacceptedNoS3EFS_L
                'id': 'jpg/unaccepted/no_gen/orig/l',
                'original': JPG_NAME,
                'generated': None,
                'request_path': JPG_NAME,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME,
                },
            },
            {
                # Test_JPGUnacceptedNoS3EFS_U
                'id': 'jpg/unaccepted/no_gen/orig/u',
                'original': JPG_NAME_U,
                'generated': None,
                'request_path': JPG_NAME_U,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME_U,
                },
            },
            {
                # Test_JPGUnacceptedNoS3EFS_MB
                'id': 'jpg/unaccepted/no_gen/orig/mb',
                'original': JPG_NAME_MB,
                'generated': None,
                'request_path': JPG_NAME_MB_Q,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME_MB,
                },
            },
            {
                # Test_JPGUnacceptedNoS3NoEFS_L
                'id': 'jpg/unaccepted/no_gen/no_orig/l',
                'original': None,
                'generated': None,
                'request_path': JPG_NAME,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
            },
            {
                # Test_JPGUnacceptedNoS3NoEFS_U
                'id': 'jpg/unaccepted/no_gen/no_orig/u',
                'original': None,
                'generated': None,
                'request_path': JPG_NAME_U,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
            },
            {
                # Test_JPGUnacceptedNoS3NoEFS_MB
                'id': 'jpg/unaccepted/no_gen/no_orig/mb',
                'original': None,
                'generated': None,
                'request_path': JPG_NAME_MB_Q,
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
            },
            {
                # Test_JPGAcceptedS3EFSOld_L
                'id': 'jpg/accepted/gen/orig/old/l',
                'original': JPG_NAME,
                'generated': (JPG_WEBP_NAME, JPEG_MIME, {}, lambda ts: ts - datetime.timedelta(1)),
                'request_path': JPG_NAME,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME,
                },
            },
            {
                # Test_JPGAcceptedS3EFSOld_U
                'id': 'jpg/accepted/gen/orig/old/u',
                'original': JPG_NAME_U,
                'generated': (
                    JPG_WEBP_NAME_U, JPEG_MIME, {}, lambda ts: ts - datetime.timedelta(1)),
                'request_path': JPG_NAME_U,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME_U,
                },
            },
            {
                # Test_JPGAcceptedS3EFSOld_MB
                'id': 'jpg/accepted/gen/orig/old/mb',
                'original': JPG_NAME_MB,
                'generated': (
                    JPG_WEBP_NAME_MB, JPEG_MIME, {}, lambda ts: ts - datetime.timedelta(1)),
                'request_path': JPG_NAME_MB_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': JPG_NAME_MB,
                },
            },
            {
                # Test_CSSS3EFS
                'id': 'css/gen/orig',
                'original': CSS_NAME,
                'generated': CSS_NAME,
                'request_path': CSS_NAME_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'origin_domain': True,
                    'uri': CSS_NAME_Q,
                },
            },
            {
                # Test_CSSS3NoEFS
                'id': 'css/gen/no_orig',
                'original': None,
                'generated': CSS_NAME,
                'request_path': CSS_NAME_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': CSS_NAME,
                },
            },
            {
                # Test_CSSNoS3EFS
                'id': 'css/no_gen/orig',
                'original': CSS_NAME,
                'generated': None,
                'request_path': CSS_NAME_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': CSS_NAME,
                },
            },
            {
                # Test_CSSNoS3NoEFS
                'id': 'css/no_gen/no_orig',
                'original': None,
                'generated': None,
                'request_path': CSS_NAME_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
            },
            {
                # Test_CSSS3EFSOld
                'id': 'css/gen/orig/old',
                'original': CSS_NAME,
                'generated': (CSS_NAME, CSS_MIME, {}, lambda ts: ts - datetime.timedelta(1)),
                'request_path': CSS_NAME_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': CSS_NAME,
                },
            },
            {
                # Test_MinCSSS3EFS
                'id': 'min_css/gen/orig',
                'original': MIN_CSS_NAME,
                'generated': MIN_CSS_NAME,
                'request_path': MIN_CSS_NAME_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'res_cache_control_overridable': 'true',
                },
            },
            {
                # Test_MinCSSS3NoEFS
                'id': 'min_css/gen/no_orig',
                'original': None,
                'generated': MIN_CSS_NAME,
                'request_path': MIN_CSS_NAME_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'res_cache_control_overridable': 'true',
                },
            },
            {
                # Test_MinCSSNoS3EFS
                'id': 'min_css/no_gen/orig',
                'original': MIN_CSS_NAME,
                'generated': None,
                'request_path': MIN_CSS_NAME_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'res_cache_control_overridable': 'true',
                },
            },
            {
                # Test_MinCSSNoS3NoEFS
                'id': 'min_css/no_gen/no_orig',
                'original': None,
                'generated': None,
                'request_path': MIN_CSS_NAME_Q,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_PERM,
                    'res_cache_control_overridable': 'true',
                },
            },
            {
                'id': 'resize/jpg/accepted',
                'original': JPG_NAME,
                'generated': None,
                'request_path': JPG_NAME,
                'query_string': {
                    'w': '200',
                    'h': '150',
                },
                'expected_instant_response': {
                    'status': HTTPStatus.OK,
                    'content_type': WEBP_MIME,
                    'cache_control': CACHE_CONTROL_PERM,
                    'size': (200, 150),
                },
            },
            {
                'id': 'resize/jpg/unaccepted',
                'original': JPG_NAME,
                'generated': None,
                'request_path': JPG_NAME,
                'query_string': {
                    'w': '200',
                    'h': '150',
                },
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_instant_response': {
                    'status': HTTPStatus.OK,
                    'content_type': JPEG_MIME,
                    'cache_control': CACHE_CONTROL_PERM,
                    'size': (200, 150),
                },
            },
            {
                'id': 'resize/png/accepted',
                'original': PNG_NAME,
                'generated': None,
                'request_path': PNG_NAME,
                'query_string': {
                    'w': '200',
                    'h': '150',
                },
                'expected_instant_response': {
                    'status': HTTPStatus.OK,
                    'content_type': WEBP_MIME,
                    'cache_control': CACHE_CONTROL_PERM,
                    'size': (200, 150),
                },
            },
            {
                'id': 'resize/png/unaccepted',
                'original': PNG_NAME,
                'generated': None,
                'request_path': PNG_NAME,
                'query_string': {
                    'w': '200',
                    'h': '150',
                },
                'accept_header': OLD_SAFARI_ACCEPT_HEADER,
                'expected_instant_response': {
                    'status': HTTPStatus.OK,
                    'content_type': PNG_MIME,
                    'cache_control': CACHE_CONTROL_PERM,
                    'size': (200, 150),
                },
            },
            {
                'id': 'resize/broken-jpg',
                'original': BROKEN_JPG_NAME,
                'generated': None,
                'request_path': BROKEN_JPG_NAME,
                'expected_field_update': {
                    'res_cache_control': CACHE_CONTROL_TEMP,
                },
                'expected_sqs_message': {
                    'key': BROKEN_JPG_NAME,
                },
            },
        ]))
def test_generated(
    original_name: Optional[str],
    original_mime: str,
    original_metadata: dict[str, str],
    generated_name: Optional[str],
    generated_mime: str,
    generated_metadata: dict[str, str],
    accept_header: AcceptHeader,
    request_path: HttpPath,
    query_string: dict[str, list[str]],
    img_server: ImgServer,
    field_update: Optional[FieldUpdate],
    instant_response: Optional[ExpectedInstantResponse],
    sqs_message: Optional[dict[str, Any]],
    key_prefix: str,
    modify_ts: Optional[Callable[[datetime.datetime], datetime.datetime]],
) -> None:
  if original_name is None:
    ts = DUMMY_DATETIME
  else:
    ts = put_original(img_server, key_prefix, original_name, original_mime, original_metadata)

  if modify_ts is not None:
    ts = modify_ts(ts)

  if generated_name is not None:
    put_generated(img_server, key_prefix, generated_name, generated_mime, ts, generated_metadata)

  update = img_server.process(request_path, query_string, accept_header)

  if field_update is not None:
    assert isinstance(update, FieldUpdate)
    assert field_update == update

  if instant_response is not None:
    assert isinstance(update, InstantResponse)
    assert update.content_type == instant_response['content_type']
    assert update.status == instant_response['status']
    assert update.cache_control == instant_response['cache_control']
    if update.b64_body is None:
      assert 'size' not in instant_response
    else:
      bs = base64.b64decode(update.b64_body)
      image = Image.new_from_buffer(bs, '')
      assert 'size' in instant_response
      assert instant_response['size'] == (image.get('width'), image.get('height'))
      assert update.content_type == LOADER_MAP[image.get('vips-loader')]

  if sqs_message is None:
    assert receive_sqs_message(img_server) is None
  else:
    assert receive_sqs_message(img_server) == sqs_message
