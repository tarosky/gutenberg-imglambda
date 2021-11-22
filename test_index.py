import datetime
import json
import logging
import os
import secrets
import sys
import time
import warnings
from logging import Logger
from pathlib import Path
from typing import Dict, List, Optional
from unittest import TestCase

import boto3
import pytz
from mypy_boto3_sqs.type_defs import MessageTypeDef

from index import TIMESTAMP_METADATA, FieldUpdate, ImgServer, MyJsonFormatter

PERM_RESP_MAX_AGE = 365 * 24 * 60 * 60
TEMP_RESP_MAX_AGE = 20 * 60
GENERATED_KEY_PREFIX = 'prefix/'
REGION = 'ap-northeast-1'

CSS_MIME = 'text/css'
GIF_MIME = "image/gif"
JPEG_MIME = 'image/jpeg'
JS_MIME = 'text/javascript'
PNG_MIME = 'image/png'
SOURCE_MAP_MIME = 'application/octet-stream'
WEBP_MIME = "image/webp"

JPG_NAME = 'image.jpg'
JPG_NAME_U = 'image.JPG'
JPG_WEBP_NAME = 'image.jpg.webp'
JPG_WEBP_NAME_U = 'image.JPG.webp'
JS_NAME = 'fizzbuzz.js'

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


def create_img_server(log: Logger, name: str) -> ImgServer:
  account_id = read_test_config('aws-account-id')
  sqs_name = f'test-{name}-{generate_safe_random_string()}'

  sess = boto3.Session(
      aws_access_key_id=read_test_config('access-key-id'),
      aws_secret_access_key=read_test_config('secret-access-key'))
  warnings.filterwarnings(
      "ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")
  sqs = sess.client('sqs', region_name=REGION)
  s3 = sess.client('s3', region_name=REGION)

  return ImgServer(
      log=log,
      region=REGION,
      sqs=sqs,
      s3=s3,
      generated_domain=f"{read_test_config('s3-bucket')}.s3.example.com",
      original_bucket=read_test_config('public-content-bucket'),
      generated_key_prefix=GENERATED_KEY_PREFIX,
      sqs_queue_url=(
          f'https://sqs.{REGION}.amazonaws.com/{account_id}/{sqs_name}'),
      perm_resp_max_age=PERM_RESP_MAX_AGE,
      temp_resp_max_age=TEMP_RESP_MAX_AGE)


def get_test_sqs_queue_name_from_url(sqs_queue_url: str) -> str:
  return sqs_queue_url.split('/')[-1]


def create_test_environment(log: Logger, name: str) -> ImgServer:
  img_server = create_img_server(log, name)
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


def receive_sqs_message(img_server: ImgServer) -> Optional[Dict[str, str]]:
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
  obj: Dict[str, str] = json.loads(body)
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

    self._img_server = create_test_environment(self._log, 'imglambda')

  def put_original(self, name: str, mime: str) -> datetime.datetime:
    return put_original(
        self._img_server, f'{self._key_prefix}{name}', name, mime)

  def put_generated(
      self, name: str, mime: str,
      timestamp: Optional[datetime.datetime]) -> None:
    key = f'{self._img_server.generated_key_prefix}{self._key_prefix}{name}'

    self._log.debug({'testgen': key})
    put_generated(self._img_server, key, name, mime, timestamp)

  def receive_sqs_message(self) -> Optional[Dict[str, str]]:
    return receive_sqs_message(self._img_server)

  def assert_no_sqs_message(self) -> None:
    self.assertIsNone(self.receive_sqs_message())

  def assert_sqs_message(self, key: str) -> None:
    self.assertEqual(
        {
            'bucket': self._img_server.original_bucket,
            'path': key,
        }, self.receive_sqs_message())

  def tearDown(self) -> None:
    clean_test_environment(self._img_server)


class ImgserveTestCase(BaseTestCase):

  def setUp(self) -> None:
    super().setUp()

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
            origin_path=f'{self._img_server.generated_key_prefix}{path}.webp',
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
            origin_path=f'{self._img_server.generated_key_prefix}{path}',
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


# func (s *ImgServerSuite) Test_JSS3EFSOld() {
# 	s.uploadFileToS3Dest(s.ctx, jsPathL, toWebPPath(jsPathL), sampleMinJS, &oldModTime)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+jsPathL, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
# 		s.Assert().Equal(jsMIME, header.contentType())
# 		s.Assert().Equal(sampleJSSize, res.ContentLength)
# 		s.Assert().Equal(sampleJSETag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		// Send message to update S3 object
# 		s.assertDelayedSQSMessage(ctx, jsPathL)
# 		s.assertS3SrcExists(ctx, jsPathL, &sampleModTime, jsMIME, sampleJSSize)
# 	})
# }

# func (s *ImgServerSuite) Test_CSSS3EFS() {
# 	eTag := s.uploadFileToS3Dest(s.ctx, cssPathL, cssPathL, sampleMinCSS, nil)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+cssPathL, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
# 		s.Assert().Equal(cssMIME, header.contentType())
# 		s.Assert().Equal(sampleMinCSSSize, res.ContentLength)
# 		s.Assert().Equal(eTag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, cssPathL)
# 	})
# }

# func (s *ImgServerSuite) Test_CSSS3NoEFS() {
# 	const longTextLen = int64(1024)

# 	s.uploadFileToS3Dest(s.ctx, cssPathL, cssPathL, sampleMinCSS, nil)
# 	s.uploadFileToS3Src(s.ctx, cssPathL, cssPathL, sampleCSS, nil)
# 	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + cssPathL))

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+cssPathL, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
# 		s.Assert().Equal(plainContentType, header.contentType())
# 		s.Assert().Greater(longTextLen, res.ContentLength)
# 		s.Assert().Equal("", header.eTag())
# 		s.Assert().Equal("", header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertDelayedSQSMessage(ctx, cssPathL)
# 		// Ensure source file on S3 is also removed
# 		s.assertS3SrcNotExists(ctx, cssPathL)
# 	})
# }

# func (s *ImgServerSuite) Test_CSSNoS3EFS() {
# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+cssPathL, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
# 		s.Assert().Equal(cssMIME, header.contentType())
# 		s.Assert().Equal(sampleCSSSize, res.ContentLength)
# 		s.Assert().Equal(sampleCSSETag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertDelayedSQSMessage(ctx, cssPathL)
# 		s.assertS3SrcExists(ctx, cssPathL, &sampleModTime, cssMIME, sampleCSSSize)
# 	})
# }

# func (s *ImgServerSuite) Test_CSSNoS3NoEFS() {
# 	const longTextLen = int64(1024)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+cssNonExistentPathL, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
# 		s.Assert().Equal(plainContentType, header.contentType())
# 		s.Assert().Greater(longTextLen, res.ContentLength)
# 		s.Assert().Equal("", header.eTag())
# 		s.Assert().Equal("", header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, cssNonExistentPathL)
# 	})
# }

# func (s *ImgServerSuite) Test_CSSS3EFSOld() {
# 	s.uploadFileToS3Dest(s.ctx, cssPathL, toWebPPath(cssPathL), sampleMinCSS, &oldModTime)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+cssPathL, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
# 		s.Assert().Equal(cssMIME, header.contentType())
# 		s.Assert().Equal(sampleCSSSize, res.ContentLength)
# 		s.Assert().Equal(sampleCSSETag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		// Send message to update S3 object
# 		s.assertDelayedSQSMessage(ctx, cssPathL)
# 		s.assertS3SrcExists(ctx, cssPathL, &sampleModTime, cssMIME, sampleCSSSize)
# 	})
# }

# func (s *ImgServerSuite) Test_SourceMapS3EFS() {
# 	s.uploadFileToS3Dest(s.ctx, sourceMapPathL, sourceMapPathL, sampleSourceMap, nil)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+sourceMapPathL, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
# 		s.Assert().Equal(sourceMapMIME, header.contentType())
# 		s.Assert().Equal(sampleSourceMap2Size, res.ContentLength)
# 		s.Assert().Equal(sampleSourceMap2ETag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, sourceMapPathL)
# 	})
# }

# func (s *ImgServerSuite) Test_SourceMapS3NoEFS() {
# 	const longTextLen = int64(1024)

# 	eTag := s.uploadFileToS3Dest(s.ctx, sourceMapPathL, sourceMapPathL, sampleSourceMap, nil)
# 	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + sourceMapPathL))

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+sourceMapPathL, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
# 		s.Assert().Equal(sourceMapMIME, header.contentType())
# 		s.Assert().Equal(sampleSourceMapSize, res.ContentLength)
# 		s.Assert().Equal(eTag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, sourceMapPathL)
# 	})
# }

# func (s *ImgServerSuite) Test_SourceMapNoS3EFS() {
# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+sourceMapPathL, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
# 		s.Assert().Equal(sourceMapMIME, header.contentType())
# 		s.Assert().Equal(sampleSourceMap2Size, res.ContentLength)
# 		s.Assert().Equal(sampleSourceMap2ETag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, sourceMapPathL)
# 	})
# }

# func (s *ImgServerSuite) Test_SourceMapNoS3NoEFS() {
# 	const longTextLen = int64(1024)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+sourceMapNonExistentPathL, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
# 		s.Assert().Equal(plainContentType, header.contentType())
# 		s.Assert().Greater(longTextLen, res.ContentLength)
# 		s.Assert().Equal("", header.eTag())
# 		s.Assert().Equal("", header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, sourceMapNonExistentPathL)
# 	})
# }

# func (s *ImgServerSuite) Test_MinJSS3EFS() {
# 	s.FileS3EFS(minJSPathL, jsMIME, sampleMinJSSize, sampleMinJSETag)
# }

# func (s *ImgServerSuite) Test_MinCSSS3EFS() {
# 	s.FileS3EFS(minCSSPathL, cssMIME, sampleMinCSSSize, sampleMinCSSETag)
# }

# func (s *ImgServerSuite) FileS3EFS(
# 	path string,
# 	contentType string,
# 	size int64,
# 	eTag string,
# ) {
# 	s.uploadFileToS3Dest(s.ctx, path, path, sampleSourceMap, nil)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
# 		s.Assert().Equal(contentType, header.contentType())
# 		s.Assert().Equal(size, res.ContentLength)
# 		s.Assert().Equal(eTag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, path)
# 	})
# }

# func (s *ImgServerSuite) Test_MinJSS3NoEFS() {
# 	s.FileS3NoEFS(minJSPathL)
# }

# func (s *ImgServerSuite) Test_MinCSSS3NoEFS() {
# 	s.FileS3NoEFS(minCSSPathL)
# }

# func (s *ImgServerSuite) FileS3NoEFS(path string) {
# 	const longTextLen = int64(1024)

# 	s.uploadFileToS3Dest(s.ctx, path, path, sampleSourceMap, nil)
# 	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + path))

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
# 		s.Assert().Equal(plainContentType, header.contentType())
# 		s.Assert().Greater(longTextLen, res.ContentLength)
# 		s.Assert().Equal("", header.eTag())
# 		s.Assert().Equal("", header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, path)
# 	})
# }

# func (s *ImgServerSuite) Test_MinJSNoS3EFS() {
# 	s.FileS3EFS(minJSPathL, jsMIME, sampleMinJSSize, sampleMinJSETag)
# }

# func (s *ImgServerSuite) Test_MinCSSNoS3EFS() {
# 	s.FileS3EFS(minCSSPathL, cssMIME, sampleMinCSSSize, sampleMinCSSETag)
# }

# func (s *ImgServerSuite) FileNoS3EFS(
# 	path string,
# 	contentType string,
# 	size int64,
# 	eTag string,
# ) {
# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
# 		s.Assert().Equal(contentType, header.contentType())
# 		s.Assert().Equal(size, res.ContentLength)
# 		s.Assert().Equal(eTag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, path)
# 	})
# }

# func (s *ImgServerSuite) Test_MinJSNoS3NoEFS() {
# 	s.FileNoS3NoEFS(minJSPathL)
# }

# func (s *ImgServerSuite) Test_MinCSSNoS3NoEFS() {
# 	s.FileNoS3NoEFS(minCSSPathL)
# }

# func (s *ImgServerSuite) FileNoS3NoEFS(path string) {
# 	const longTextLen = int64(1024)

# 	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + path))

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusNotFound, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
# 		s.Assert().Equal(plainContentType, header.contentType())
# 		s.Assert().Greater(longTextLen, res.ContentLength)
# 		s.Assert().Equal("", header.eTag())
# 		s.Assert().Equal("", header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))
# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, path)
# 	})
# }
if 'unittest.util' in __import__('sys').modules:
  # Show full diff in self.assertEqual.
  __import__('sys').modules['unittest.util']._MAX_LENGTH = 999999999
