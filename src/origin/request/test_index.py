import datetime
import json
import logging
import os
import secrets
import time
import warnings
from logging import Logger
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import TestCase

import boto3
import pytz
from mypy_boto3_sqs.type_defs import MessageTypeDef

from .index import TIMESTAMP_METADATA, FieldUpdate, ImgServer, MyJsonFormatter

PERM_RESP_MAX_AGE = 365 * 24 * 60 * 60
TEMP_RESP_MAX_AGE = 20 * 60
GENERATED_KEY_PREFIX = 'prefix/'
REGION = 'us-east-1'

CSS_MIME = 'text/css'
GIF_MIME = "image/gif"
JPEG_MIME = 'image/jpeg'
JS_MIME = 'text/javascript'
PNG_MIME = 'image/png'
SOURCEMAP_MIME = 'application/octet-stream'
WEBP_MIME = "image/webp"

CSS_NAME = 'style.css'
JPG_NAME = 'image.jpg'
JPG_NAME_U = 'image.JPG'
JPG_WEBP_NAME = 'image.jpg.webp'
JPG_WEBP_NAME_U = 'image.JPG.webp'
JS_NAME = 'fizzbuzz.js'
MIN_CSS_NAME = 'style.min.css'
MIN_JS_NAME = 'script.min.js'
SOURCEMAP_NAME = 'script.js.map'

DUMMY_DATETIME = datetime.datetime(2000, 1, 1)

CHROME_ACCEPT_HEADER = 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8'
OLD_SAFARI_ACCEPT_HEADER = (
    'image/png,image/svg+xml,image/*;q=0.8,video/*;q=0.8,*/*;q=0.5')

CACHE_CONTROL_PERM = f'public, max-age={PERM_RESP_MAX_AGE}'
CACHE_CONTROL_TEMP = f'public, max-age={TEMP_RESP_MAX_AGE}'


def read_test_config(name: str) -> str:
  path = f'{os.getcwd()}/config/test/{name}'
  with open(path, 'r') as f:
    return f.read().strip()


def generate_safe_random_string() -> str:
  return secrets.token_urlsafe(256 // 8)


def create_img_server(
    log: Logger, name: str, expiration_margin: int) -> ImgServer:
  account_id = read_test_config('aws-account-id')
  sqs_name = f'test-{name}-{generate_safe_random_string()}'

  sess = boto3.Session(
      aws_access_key_id=read_test_config('access-key-id'),
      aws_secret_access_key=read_test_config('secret-access-key'))
  # https://github.com/boto/boto3/issues/454#issuecomment-380900404
  warnings.filterwarnings(
      'ignore', category=ResourceWarning, message='unclosed.*<ssl.SSLSocket.*>')
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
      sqs_queue_url=(
          f'https://sqs.{REGION}.amazonaws.com/{account_id}/{sqs_name}'),
      perm_resp_max_age=PERM_RESP_MAX_AGE,
      temp_resp_max_age=TEMP_RESP_MAX_AGE,
      expiration_margin=expiration_margin)


def get_test_sqs_queue_name_from_url(sqs_queue_url: str) -> str:
  return sqs_queue_url.split('/')[-1]


def create_test_environment(
    log: Logger,
    name: str,
    expiration_margin: int,
) -> ImgServer:
  img_server = create_img_server(log, name, expiration_margin)
  img_server.sqs.create_queue(
      QueueName=get_test_sqs_queue_name_from_url(img_server.sqs_queue_url))
  return img_server


def clean_test_environment(img_server: ImgServer) -> None:
  img_server.sqs.delete_queue(QueueUrl=img_server.sqs_queue_url)


def put_original(
    img_server: ImgServer,
    key: str,
    name: str,
    mime: str,
) -> datetime.datetime:
  path = f'{os.getcwd()}/samplefile/original/{name}'
  with open(path, 'rb') as f:
    img_server.s3.put_object(
        Body=f,
        Bucket=img_server.original_bucket,
        ContentType=mime,
        Key=key,
    )

  return get_original_object_time(img_server, key)


def put_generated(
    img_server: ImgServer,
    key: str,
    name: str,
    mime: str,
    timestamp: Optional[datetime.datetime] = None,
) -> None:
  path = f'{os.getcwd()}/samplefile/generated/{name}'
  metadata = {}
  if timestamp is not None:
    metadata[TIMESTAMP_METADATA] = timestamp.astimezone(
        pytz.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  with open(path, 'rb') as f:
    img_server.s3.put_object(
        Body=f,
        Bucket=img_server.generated_bucket,
        ContentType=mime,
        Key=key,
        Metadata=metadata)


def get_original_object_time(
    img_server: ImgServer,
    key: str,
) -> datetime.datetime:
  res = img_server.s3.head_object(Bucket=img_server.original_bucket, Key=key)
  return res['LastModified']


def receive_sqs_message(img_server: ImgServer) -> Optional[Dict[str, Any]]:
  time.sleep(1.0)
  res = img_server.sqs.receive_message(
      QueueUrl=img_server.sqs_queue_url,
      MaxNumberOfMessages=1,
      VisibilityTimeout=1,
      WaitTimeSeconds=1)
  msgs: Optional[List[MessageTypeDef]] = res.get('Messages', None)
  if msgs is None or len(msgs) == 0:
    return None
  body: str = msgs[0]['Body']
  obj: Dict[str, Any] = json.loads(body)
  return obj


class BaseTestCase(TestCase):
  maxDiff = None

  def setUp(self) -> None:
    self._key_prefix = generate_safe_random_string() + '/'
    self._log = logging.getLogger(__name__)

    log_dir = f'{os.getcwd()}/work/test/imglambda/{self._key_prefix}'
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    self._log_file = open(f'{log_dir}/test.log', 'w')

    log_handler = logging.StreamHandler()
    log_handler.setFormatter(MyJsonFormatter())
    log_handler.setLevel(logging.DEBUG)
    log_handler.setStream(self._log_file)
    self._log.addHandler(log_handler)

    self._img_server = create_test_environment(
        self._log, 'imglambda', self.get_expiration_margin())

  def get_expiration_margin(self) -> int:
    return 10

  def put_original(self, name: str, mime: str) -> datetime.datetime:
    return put_original(
        self._img_server, f'{self._key_prefix}{name}', name, mime)

  def put_generated(
      self, name: str, mime: str,
      timestamp: Optional[datetime.datetime]) -> None:
    key = f'{self._img_server.generated_key_prefix}{self._key_prefix}{name}'

    put_generated(self._img_server, key, name, mime, timestamp)

  def receive_sqs_message(self) -> Optional[Dict[str, Any]]:
    return receive_sqs_message(self._img_server)

  def assert_no_sqs_message(self) -> None:
    self.assertIsNone(self.receive_sqs_message())

  def assert_sqs_message(self, key: str) -> None:
    self.assertEqual(
        {
            'version': 2,
            'path': key,
            'src': {
                'bucket': self._img_server.original_bucket,
                'prefix': '',
            },
            'dest': {
                'bucket': self._img_server.generated_bucket,
                'prefix': '',
            },
        }, self.receive_sqs_message())

  def tearDown(self) -> None:
    clean_test_environment(self._img_server)


class ImgserverExpiredTestCase(BaseTestCase):

  def get_expiration_margin(self) -> int:
    return 60 * 60 * 24 * 2

  def test_jpg_accepted_gen_orig(self) -> None:
    ts = self.put_original(JPG_NAME, JPEG_MIME)
    self.put_generated(JPG_WEBP_NAME, JPEG_MIME, ts)

    path = f'{self._key_prefix}{JPG_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_sqs_message(path)


class ImgserverTestCase(BaseTestCase):

  # Test_JPGAcceptedS3EFS_L
  def test_jpg_accepted_gen_orig_l(self) -> None:
    self.jpg_accepted_gen_orig(JPG_WEBP_NAME, JPG_NAME)

  # Test_JPGAcceptedS3EFS_U
  def test_jpg_accepted_gen_orig_u(self) -> None:
    self.jpg_accepted_gen_orig(JPG_WEBP_NAME_U, JPG_NAME_U)

  # JPGAcceptedS3EFS
  def jpg_accepted_gen_orig(self, gen_name: str, orig_name: str) -> None:
    ts = self.put_original(orig_name, JPEG_MIME)
    self.put_generated(gen_name, JPEG_MIME, ts)

    path = f'{self._key_prefix}{orig_name}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(
        FieldUpdate(
            res_cache_control=CACHE_CONTROL_PERM,
            origin_domain=self._img_server.generated_domain,
            uri=f'/{self._img_server.generated_key_prefix}{path}.webp',
        ), update)
    self.assert_no_sqs_message()

  # Skipped:
  #
  # Test_PublicContentJPG

  # Test_JPGAcceptedS3NoEFS_L
  def test_jpg_accepted_gen_no_orig_l(self) -> None:
    self.jpg_accepted_gen_no_orig(JPG_WEBP_NAME, JPG_NAME)

  # Test_JPGAcceptedS3NoEFS_U
  def test_jpg_accepted_gen_no_orig_u(self) -> None:
    self.jpg_accepted_gen_no_orig(JPG_WEBP_NAME_U, JPG_NAME_U)

  # JPGAcceptedS3NoEFS
  def jpg_accepted_gen_no_orig(self, gen_name: str, orig_name: str) -> None:
    self.put_generated(gen_name, JPEG_MIME, DUMMY_DATETIME)

    path = f'{self._key_prefix}{orig_name}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_sqs_message(path)

  # Test_JPGAcceptedNoS3EFS_L
  def test_jpg_accepted_no_gen_orig_l(self) -> None:
    self.jpg_accepted_no_gen_orig(JPG_NAME)

  # Test_JPGAcceptedNoS3EFS_U
  def test_jpg_accepted_no_gen_orig_u(self) -> None:
    self.jpg_accepted_no_gen_orig(JPG_NAME_U)

  # JPGAcceptedNoS3EFS
  def jpg_accepted_no_gen_orig(self, orig_name: str) -> None:
    self.put_original(orig_name, JPEG_MIME)

    path = f'{self._key_prefix}{orig_name}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_sqs_message(path)

  # Test_JPGAcceptedNoS3NoEFS_L
  def test_jpg_accepted_no_gen_no_orig_l(self) -> None:
    self.jpg_accepted_no_gen_no_orig(JPG_NAME)

  # Test_JPGAcceptedNoS3NoEFS_U
  def test_jpg_accepted_no_gen_no_orig_u(self) -> None:
    self.jpg_accepted_no_gen_no_orig(JPG_NAME_U)

  # JPGAcceptedNoS3NoEFS
  def jpg_accepted_no_gen_no_orig(self, orig_name: str) -> None:
    path = f'{self._key_prefix}{orig_name}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_no_sqs_message()

  # Test_JPGUnacceptedS3EFS_L
  def test_jpg_unaccepted_gen_orig_l(self) -> None:
    self.jpg_unaccepted_gen_orig(JPG_WEBP_NAME, JPG_NAME)

  # Test_JPGUnacceptedS3EFS_U
  def test_jpg_unaccepted_gen_orig_u(self) -> None:
    self.jpg_unaccepted_gen_orig(JPG_WEBP_NAME_U, JPG_NAME_U)

  # JPGUnacceptedS3EFS
  def jpg_unaccepted_gen_orig(self, gen_name: str, orig_name: str) -> None:
    ts = self.put_original(orig_name, JPEG_MIME)
    self.put_generated(gen_name, JPEG_MIME, ts)

    path = f'{self._key_prefix}{orig_name}'
    update = self._img_server.process(path, OLD_SAFARI_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_PERM), update)
    self.assert_no_sqs_message()

  # Test_JPGUnacceptedS3NoEFS_L
  def test_jpg_unaccepted_gen_no_orig_l(self) -> None:
    self.jpg_unaccepted_gen_no_orig(JPG_WEBP_NAME, JPG_NAME)

  # Test_JPGUnacceptedS3NoEFS_U
  def test_jpg_unaccepted_gen_no_orig_u(self) -> None:
    self.jpg_unaccepted_gen_no_orig(JPG_WEBP_NAME_U, JPG_NAME_U)

  # JPGUnacceptedS3NoEFS
  def jpg_unaccepted_gen_no_orig(self, gen_name: str, orig_name: str) -> None:
    self.put_generated(gen_name, JPEG_MIME, DUMMY_DATETIME)

    path = f'{self._key_prefix}{orig_name}'
    update = self._img_server.process(path, OLD_SAFARI_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_sqs_message(path)

  # Test_JPGUnacceptedNoS3EFS_L
  def test_jpg_unaccepted_no_gen_orig_l(self) -> None:
    self.jpg_unaccepted_no_gen_orig(JPG_NAME)

  # Test_JPGUnacceptedNoS3EFS_U
  def test_jpg_unaccepted_no_gen_orig_u(self) -> None:
    self.jpg_unaccepted_no_gen_orig(JPG_NAME_U)

  # JPGUnacceptedNoS3EFS
  def jpg_unaccepted_no_gen_orig(self, orig_name: str) -> None:
    self.put_original(orig_name, JPEG_MIME)

    path = f'{self._key_prefix}{orig_name}'
    update = self._img_server.process(path, OLD_SAFARI_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_PERM), update)
    self.assert_sqs_message(path)

  # Test_JPGUnacceptedNoS3NoEFS_L
  def test_jpg_unaccepted_no_gen_no_orig_l(self) -> None:
    self.jpg_unaccepted_no_gen_no_orig(JPG_NAME)

  # Test_JPGUnacceptedNoS3NoEFS_U
  def test_jpg_unaccepted_no_gen_no_orig_u(self) -> None:
    self.jpg_unaccepted_no_gen_no_orig(JPG_NAME_U)

  # JPGUnacceptedNoS3NoEFS
  def jpg_unaccepted_no_gen_no_orig(self, orig_name: str) -> None:
    path = f'{self._key_prefix}{orig_name}'
    update = self._img_server.process(path, OLD_SAFARI_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_no_sqs_message()

  # Test_JPGAcceptedS3EFSOld_L
  def test_jpg_accepted_gen_orig_old_l(self) -> None:
    self.jpg_accepted_gen_orig_old(JPG_WEBP_NAME, JPG_NAME)

  # Test_JPGAcceptedS3EFSOld_U
  def test_jpg_accepted_gen_orig_old_u(self) -> None:
    self.jpg_accepted_gen_orig_old(JPG_WEBP_NAME_U, JPG_NAME_U)

  # JPGAcceptedS3EFSOld
  def jpg_accepted_gen_orig_old(self, gen_name: str, orig_name: str) -> None:
    ts = self.put_original(orig_name, JPEG_MIME)
    self.put_generated(gen_name, JPEG_MIME, ts + datetime.timedelta(1))

    path = f'{self._key_prefix}{orig_name}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_sqs_message(path)

  # Skipped:
  #
  # Test_JPGAcceptedNoS3EFSBatchSendRepeat
  # Test_JPGAcceptedNoS3EFSBatchSendWait
  # Test_ReopenLogFile

  # Test_JSS3EFS
  def test_js_gen_orig(self) -> None:
    ts = self.put_original(JS_NAME, JS_MIME)
    self.put_generated(JS_NAME, JS_MIME, ts)

    path = f'{self._key_prefix}{JS_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(
        FieldUpdate(
            res_cache_control=CACHE_CONTROL_PERM,
            origin_domain=self._img_server.generated_domain,
            uri=f'/{self._img_server.generated_key_prefix}{path}',
        ), update)
    self.assert_no_sqs_message()

  # Test_JSS3NoEFS
  def test_js_gen_no_orig(self) -> None:
    self.put_generated(JS_NAME, JS_MIME, DUMMY_DATETIME)

    path = f'{self._key_prefix}{JS_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_sqs_message(path)

  # Test_JSNoS3EFS
  def test_js_no_gen_orig(self) -> None:
    self.put_original(JS_NAME, JS_MIME)

    path = f'{self._key_prefix}{JS_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_sqs_message(path)

  # Test_JSNoS3NoEFS
  def test_js_no_gen_no_orig(self) -> None:
    path = f'{self._key_prefix}{JS_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_no_sqs_message()

  # Test_JSS3EFSOld
  def test_js_gen_orig_old(self) -> None:
    ts = self.put_original(JS_NAME, JS_MIME)
    self.put_generated(JS_NAME, JS_MIME, ts + datetime.timedelta(1))

    path = f'{self._key_prefix}{JS_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_sqs_message(path)

  # Test_CSSS3EFS
  def test_css_gen_orig(self) -> None:
    ts = self.put_original(CSS_NAME, CSS_MIME)
    self.put_generated(CSS_NAME, CSS_MIME, ts)

    path = f'{self._key_prefix}{CSS_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(
        FieldUpdate(
            res_cache_control=CACHE_CONTROL_PERM,
            origin_domain=self._img_server.generated_domain,
            uri=f'/{self._img_server.generated_key_prefix}{path}',
        ), update)
    self.assert_no_sqs_message()

  # Test_CSSS3NoEFS
  def test_css_gen_no_orig(self) -> None:
    self.put_generated(CSS_NAME, CSS_MIME, DUMMY_DATETIME)

    path = f'{self._key_prefix}{CSS_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_sqs_message(path)

  # Test_CSSNoS3EFS
  def test_css_no_gen_orig(self) -> None:
    self.put_original(CSS_NAME, CSS_MIME)

    path = f'{self._key_prefix}{CSS_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_sqs_message(path)

  # Test_CSSNoS3NoEFS
  def test_css_no_gen_no_orig(self) -> None:
    path = f'{self._key_prefix}{CSS_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_no_sqs_message()

  # Test_CSSS3EFSOld
  def test_css_gen_orig_old(self) -> None:
    ts = self.put_original(CSS_NAME, CSS_MIME)
    self.put_generated(CSS_NAME, CSS_MIME, ts + datetime.timedelta(1))

    path = f'{self._key_prefix}{CSS_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assert_sqs_message(path)

  # Test_SourceMapS3EFS
  def test_sourcemap_gen_orig(self) -> None:
    ts = self.put_original(SOURCEMAP_NAME, SOURCEMAP_MIME)
    self.put_generated(SOURCEMAP_NAME, SOURCEMAP_MIME, ts)

    path = f'{self._key_prefix}{SOURCEMAP_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_PERM), update)
    self.assert_no_sqs_message()

  # Test_SourceMapS3NoEFS
  def test_sourcemap_gen_no_orig(self) -> None:
    self.put_generated(SOURCEMAP_NAME, SOURCEMAP_MIME, DUMMY_DATETIME)

    path = f'{self._key_prefix}{SOURCEMAP_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(
        FieldUpdate(
            res_cache_control=CACHE_CONTROL_PERM,
            origin_domain=self._img_server.generated_domain,
            uri=f'/{self._img_server.generated_key_prefix}{path}',
        ), update)
    self.assert_no_sqs_message()

  # Test_SourceMapNoS3EFS
  def test_sourcemap_no_gen_orig(self) -> None:
    self.put_original(SOURCEMAP_NAME, SOURCEMAP_MIME)

    path = f'{self._key_prefix}{SOURCEMAP_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_PERM), update)
    self.assert_no_sqs_message()

  # Test_SourceMapNoS3NoEFS
  def test_sourcemap_no_gen_no_orig(self) -> None:
    path = f'{self._key_prefix}{SOURCEMAP_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(
        FieldUpdate(
            res_cache_control=CACHE_CONTROL_PERM,
            origin_domain=self._img_server.generated_domain,
            uri=f'/{self._img_server.generated_key_prefix}{path}',
        ), update)
    self.assert_no_sqs_message()

  def test_sourcemap_gen_orig_old(self) -> None:
    ts = self.put_original(SOURCEMAP_NAME, SOURCEMAP_MIME)
    self.put_generated(
        SOURCEMAP_NAME, SOURCEMAP_MIME, ts + datetime.timedelta(1))

    path = f'{self._key_prefix}{SOURCEMAP_NAME}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_PERM), update)
    self.assert_no_sqs_message()

  # Test_MinJSS3EFS
  def test_min_js_gen_orig(self) -> None:
    self.file_gen_orig(MIN_JS_NAME, JS_MIME)

  # Test_MinCSSS3EFS
  def test_min_css_gen_orig(self) -> None:
    self.file_gen_orig(MIN_CSS_NAME, CSS_MIME)

  # FileS3EFS
  def file_gen_orig(self, name: str, mime: str) -> None:
    ts = self.put_original(name, mime)
    self.put_generated(name, mime, ts)

    path = f'{self._key_prefix}{name}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_PERM), update)
    self.assert_no_sqs_message()

  # Test_MinJSS3NoEFS
  def test_min_js_gen_no_orig(self) -> None:
    self.file_gen_no_orig(MIN_JS_NAME, JS_MIME)

  # Test_MinCSSS3NoEFS
  def test_min_css_gen_no_orig(self) -> None:
    self.file_gen_no_orig(MIN_CSS_NAME, CSS_MIME)

  # FileS3NoEFS
  def file_gen_no_orig(self, name: str, mime: str) -> None:
    self.put_generated(name, mime, DUMMY_DATETIME)

    path = f'{self._key_prefix}{name}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_PERM), update)
    self.assert_no_sqs_message()

  # Test_MinJSNoS3EFS
  def test_min_js_no_gen_orig(self) -> None:
    self.file_no_gen_orig(MIN_JS_NAME, JS_MIME)

  # Test_MinCSSNoS3EFS
  def test_min_css_no_gen_orig(self) -> None:
    self.file_no_gen_orig(MIN_CSS_NAME, CSS_MIME)

  # FileNoS3EFS
  def file_no_gen_orig(self, name: str, mime: str) -> None:
    self.put_original(name, mime)

    path = f'{self._key_prefix}{name}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_PERM), update)
    self.assert_no_sqs_message()

  # Test_MinJSNoS3NoEFS
  def test_min_js_no_gen_no_orig(self) -> None:
    self.file_no_gen_no_orig(MIN_JS_NAME, JS_MIME)

  # Test_MinCSSNoS3NoEFS
  def test_min_css_no_gen_no_orig(self) -> None:
    self.file_no_gen_no_orig(MIN_CSS_NAME, CSS_MIME)

  # FileNoS3NoEFS
  def file_no_gen_no_orig(self, name: str, mime: str) -> None:
    path = f'{self._key_prefix}{name}'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_PERM), update)
    self.assert_no_sqs_message()


if 'unittest.util' in __import__('sys').modules:
  # Show full diff in self.assertEqual.
  __import__('sys').modules['unittest.util']._MAX_LENGTH = 999999999
