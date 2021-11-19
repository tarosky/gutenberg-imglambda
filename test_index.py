import datetime
import json
import logging
import os
import secrets
import time
import warnings
from typing import Dict, List, Optional
from unittest import TestCase

import boto3
from mypy_boto3_sqs.type_defs import MessageTypeDef

from index import FieldUpdate, ImgServer

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

PERM_RESP_MAX_AGE = 365 * 24 * 60 * 60
TEMP_RESP_MAX_AGE = 20 * 60
GENERATED_KEY_PREFIX = 'prefix'
REGION = 'ap-northeast-1'

CSS_MIME = 'text/css'
GIF_MIME = "image/gif"
JPEG_MIME = 'image/jpeg'
JS_MIME = 'text/javascript'
PNG_MIME = 'image/png'
SOURCE_MAP_MIME = 'application/octet-stream'
WEBP_MIME = "image/webp"

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


def create_img_server(name: str) -> ImgServer:
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


def create_test_environment(name: str) -> ImgServer:
  img_server = create_img_server(name)
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
) -> None:
  path = f'{os.getcwd()}/samplefile/original/{name}'
  with open(path, 'rb') as f:
    img_server.s3.put_object(
        Body=f,
        Bucket=img_server.original_bucket,
        ContentType=mime,
        Key=key,
    )


def put_generated(
    img_server: ImgServer,
    key: str,
    name: str,
    mime: str,
) -> None:
  path = f'{os.getcwd()}/samplefile/generated/{name}'
  with open(path, 'rb') as f:
    img_server.s3.put_object(
        Body=f,
        Bucket=img_server.generated_bucket,
        ContentType=mime,
        Key=key,
    )


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

  def setUp(self) -> None:
    self._img_server = create_test_environment('imglambda')
    self._key_prefix = generate_safe_random_string()

  def put_original(self, name: str, mime: str) -> None:
    put_original(self._img_server, f'{self._key_prefix}/{name}', name, mime)

  def put_generated(self, name: str, mime: str) -> None:
    put_generated(self._img_server, f'{self._key_prefix}/{name}', name, mime)

  def receive_sqs_message(self) -> Optional[Dict[str, str]]:
    return receive_sqs_message(self._img_server)

  def tearDown(self) -> None:
    clean_test_environment(self._img_server)


class MyTestCase(BaseTestCase):

  def setUp(self) -> None:
    super().setUp()

  def test_png_original_exists(self) -> None:
    self.put_original('image.png', PNG_MIME)

    path = f'{self._key_prefix}/image.png'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assertEqual(
        {
            'bucket': self._img_server.generated_bucket,
            'path': path,
        }, self.receive_sqs_message())

  def test_png_both_not_exist(self) -> None:
    path = f'{self._key_prefix}/image.png'
    update = self._img_server.process(path, CHROME_ACCEPT_HEADER)
    self.assertEqual(FieldUpdate(res_cache_control=CACHE_CONTROL_TEMP), update)
    self.assertIsNone(self.receive_sqs_message())


# func (s *ImgServerSuite) Test_JPGAcceptedS3EFS_L() {
# 	s.JPGAcceptedS3EFS(jpgPathL)
# }

# func (s *ImgServerSuite) Test_JPGAcceptedS3EFS_U() {
# 	s.JPGAcceptedS3EFS(jpgPathU)
# }

# func (s *ImgServerSuite) JPGAcceptedS3EFS(path string) {
# 	eTag := s.uploadFileToS3Dest(s.ctx, path, toWebPPath(path), sampleJPEGWebP, nil)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
# 		s.Assert().Equal(webPMIME, header.contentType())
# 		s.Assert().Equal(sampleJPEGWebPSize, res.ContentLength)
# 		s.Assert().Equal(eTag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, path)
# 	})
# }

# func (s *ImgServerSuite) Test_PublicContentJPG() {
# 	keyPrefix := strings.SplitN(s.env.publicCotnentPathPattern, "/", 2)[0]
# 	path := keyPrefix + "/wp-content/uploads/sample.jpg"
# 	eTag := s.uploadToPublicContentS3(
# 		s.ctx,
# 		path,
# 		sampleJPEG,
# 		jpegMIME,
# 		publicContentCacheControl)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(publicContentCacheControl, header.cacheControl())
# 		s.Assert().Equal(jpegMIME, header.contentType())
# 		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
# 		s.Assert().Equal(eTag, header.eTag())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))
# 	})
# }

# func (s *ImgServerSuite) Test_JPGAcceptedS3NoEFS_L() {
# 	s.JPGAcceptedS3NoEFS(jpgPathL)
# }

# func (s *ImgServerSuite) Test_JPGAcceptedS3NoEFS_U() {
# 	s.JPGAcceptedS3NoEFS(jpgPathU)
# }

# func (s *ImgServerSuite) JPGAcceptedS3NoEFS(path string) {
# 	const longTextLen = int64(1024)

# 	s.uploadFileToS3Dest(s.ctx, path, toWebPPath(path), sampleJPEGWebP, nil)
# 	s.uploadFileToS3Src(s.ctx, path, path, sampleJPEG, nil)
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

# 		s.assertDelayedSQSMessage(ctx, path)
# 		// Ensure source file on S3 is also removed
# 		s.assertS3SrcNotExists(ctx, path)
# 	})
# }

# func (s *ImgServerSuite) Test_JPGAcceptedNoS3EFS_L() {
# 	s.JPGAcceptedNoS3EFS(jpgPathL)
# }

# func (s *ImgServerSuite) Test_JPGAcceptedNoS3EFS_U() {
# 	s.JPGAcceptedNoS3EFS(jpgPathU)
# }

# func (s *ImgServerSuite) JPGAcceptedNoS3EFS(path string) {
# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
# 		s.Assert().Equal(jpegMIME, header.contentType())
# 		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
# 		s.Assert().Equal(sampleJPEGETag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertDelayedSQSMessage(ctx, path)
# 		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegMIME, sampleJPEGSize)
# 	})
# }

# func (s *ImgServerSuite) Test_JPGAcceptedNoS3NoEFS_L() {
# 	s.JPGAcceptedNoS3NoEFS(jpgNonExistentPathL)
# }

# func (s *ImgServerSuite) Test_JPGAcceptedNoS3NoEFS_U() {
# 	s.JPGAcceptedNoS3NoEFS(jpgNonExistentPathU)
# }

# func (s *ImgServerSuite) JPGAcceptedNoS3NoEFS(path string) {
# 	const longTextLen = int64(1024)

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

# func (s *ImgServerSuite) Test_JPGUnacceptedS3EFS_L() {
# 	s.JPGUnacceptedS3EFS(jpgPathL)
# }

# func (s *ImgServerSuite) Test_JPGUnacceptedS3EFS_U() {
# 	s.JPGUnacceptedS3EFS(jpgPathU)
# }

# func (s *ImgServerSuite) JPGUnacceptedS3EFS(path string) {
# 	s.uploadFileToS3Dest(s.ctx, path, toWebPPath(path), sampleJPEGWebP, nil)
# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
# 		s.Assert().Equal(jpegMIME, header.contentType())
# 		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
# 		s.Assert().Equal(sampleJPEGETag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, path)
# 	})
# }

# func (s *ImgServerSuite) Test_JPGUnacceptedS3NoEFS_L() {
# 	s.JPGUnacceptedS3NoEFS(jpgPathL)
# }

# func (s *ImgServerSuite) Test_JPGUnacceptedS3NoEFS_U() {
# 	s.JPGUnacceptedS3NoEFS(jpgPathU)
# }

# func (s *ImgServerSuite) JPGUnacceptedS3NoEFS(path string) {
# 	const longTextLen = int64(1024)

# 	s.uploadFileToS3Dest(s.ctx, path, toWebPPath(path), sampleJPEGWebP, nil)
# 	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + path))
# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

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

# 		s.assertDelayedSQSMessage(ctx, path)
# 		s.assertS3SrcNotExists(ctx, path)
# 	})
# }

# func (s *ImgServerSuite) Test_JPGUnacceptedNoS3EFS_L() {
# 	s.JPGUnacceptedNoS3EFS(jpgPathL)
# }

# func (s *ImgServerSuite) Test_JPGUnacceptedNoS3EFS_U() {
# 	s.JPGUnacceptedNoS3EFS(jpgPathU)
# }

# func (s *ImgServerSuite) JPGUnacceptedNoS3EFS(path string) {
# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
# 		s.Assert().Equal(jpegMIME, header.contentType())
# 		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
# 		s.Assert().Equal(sampleJPEGETag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertDelayedSQSMessage(ctx, path)
# 		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegMIME, sampleJPEGSize)
# 	})
# }

# func (s *ImgServerSuite) Test_JPGUnacceptedNoS3NoEFS_L() {
# 	s.JPGUnacceptedNoS3NoEFS(jpgNonExistentPathL)
# }

# func (s *ImgServerSuite) Test_JPGUnacceptedNoS3NoEFS_U() {
# 	s.JPGUnacceptedNoS3NoEFS(jpgNonExistentPathU)
# }

# func (s *ImgServerSuite) JPGUnacceptedNoS3NoEFS(path string) {
# 	const longTextLen = int64(1024)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, oldSafariAcceptHeader)

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

# func (s *ImgServerSuite) Test_JPGAcceptedS3EFSOld_L() {
# 	s.JPGAcceptedS3EFSOld(jpgPathL)
# }

# func (s *ImgServerSuite) Test_JPGAcceptedS3EFSOld_U() {
# 	s.JPGAcceptedS3EFSOld(jpgPathU)
# }

# func (s *ImgServerSuite) JPGAcceptedS3EFSOld(path string) {
# 	s.uploadFileToS3Dest(s.ctx, path, toWebPPath(path), sampleJPEGWebP, &oldModTime)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+path, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.temporaryCache.value, header.cacheControl())
# 		s.Assert().Equal(jpegMIME, header.contentType())
# 		s.Assert().Equal(sampleJPEGSize, res.ContentLength)
# 		s.Assert().Equal(sampleJPEGETag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		// Send message to update S3 object
# 		s.assertDelayedSQSMessage(ctx, path)
# 		s.assertS3SrcExists(ctx, path, &sampleModTime, jpegMIME, sampleJPEGSize)
# 	})
# }

# func (s *ImgServerSuite) Test_JPGAcceptedNoS3EFSBatchSendRepeat() {
# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		for i := 0; i < 20; i++ {
# 			s.request(ctx, ts, fmt.Sprintf("/dir/image%03d.jpg", i), chromeAcceptHeader)
# 		}
# 		time.Sleep(3 * time.Second)

# 		msgs := s.receiveSQSMessages(ctx)
# 		s.Assert().Len(msgs, 20)
# 	})
# }

# func (s *ImgServerSuite) Test_JPGAcceptedNoS3EFSBatchSendWait() {
# 	s.env.configure.sqsBatchWaitTime = 5

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		for i := 0; i < 15; i++ {
# 			s.request(ctx, ts, fmt.Sprintf("/dir/image%03d.jpg", i), chromeAcceptHeader)
# 		}
# 		time.Sleep(3 * time.Second)

# 		msgs := s.receiveSQSMessages(ctx)
# 		s.Assert().Len(msgs, 10)
# 		s.deleteSQSMessages(ctx, msgs)

# 		time.Sleep(3 * time.Second)

# 		s.Assert().Len(s.receiveSQSMessages(ctx), 5)
# 	})
# }

# func (s *ImgServerSuite) Test_ReopenLogFile() {
# 	oldLogPath := s.env.efsMountPath + "/imgserver.log.old"
# 	currentLogPath := s.env.efsMountPath + "/imgserver.log"

# 	s.env.log.Info("first message")

# 	s.Require().NoError(os.Rename(currentLogPath, oldLogPath))

# 	s.env.log.Info("second message")

# 	p, err := os.FindProcess(os.Getpid())
# 	s.Require().NoError(err)
# 	s.Require().NoError(p.Signal(syscall.SIGUSR1)) // Reopen log files

# 	time.Sleep(time.Second)

# 	s.env.log.Info("third message")

# 	oldBytes, err := ioutil.ReadFile(oldLogPath)
# 	s.Require().NoError(err)

# 	currentBytes, err := ioutil.ReadFile(currentLogPath)
# 	s.Require().NoError(err)

# 	oldLog := string(oldBytes)
# 	currentLog := string(currentBytes)

# 	s.Assert().Contains(oldLog, "first")
# 	s.Assert().Contains(oldLog, "second")

# 	s.Assert().Contains(currentLog, "third")
# }

# func (s *ImgServerSuite) Test_JSS3EFS() {
# 	eTag := s.uploadFileToS3Dest(s.ctx, jsPathL, jsPathL, sampleMinJS, nil)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+jsPathL, chromeAcceptHeader)

# 		header := httpHeader(*res)
# 		s.Assert().Equal(http.StatusOK, res.StatusCode)
# 		s.Assert().Equal(s.env.configure.permanentCache.value, header.cacheControl())
# 		s.Assert().Equal(jsMIME, header.contentType())
# 		s.Assert().Equal(sampleMinJSSize, res.ContentLength)
# 		s.Assert().Equal(eTag, header.eTag())
# 		s.Assert().Equal(sampleLastModified, header.lastModified())
# 		body, err := ioutil.ReadAll(res.Body)
# 		s.Assert().NoError(err)
# 		s.Assert().Len(body, int(res.ContentLength))

# 		s.assertNoSQSMessage(ctx)
# 		s.assertS3SrcNotExists(ctx, jsPathL)
# 	})
# }

# func (s *ImgServerSuite) Test_JSS3NoEFS() {
# 	const longTextLen = int64(1024)

# 	s.uploadFileToS3Dest(s.ctx, jsPathL, jsPathL, sampleMinJS, nil)
# 	s.uploadFileToS3Src(s.ctx, jsPathL, jsPathL, sampleJS, nil)
# 	s.Require().NoError(os.Remove(s.env.efsMountPath + "/" + jsPathL))

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+jsPathL, chromeAcceptHeader)

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

# 		s.assertDelayedSQSMessage(ctx, jsPathL)
# 		// Ensure source file on S3 is also removed
# 		s.assertS3SrcNotExists(ctx, jsPathL)
# 	})
# }

# func (s *ImgServerSuite) Test_JSNoS3EFS() {
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

# 		s.assertDelayedSQSMessage(ctx, jsPathL)
# 		s.assertS3SrcExists(ctx, jsPathL, &sampleModTime, jsMIME, sampleJSSize)
# 	})
# }

# func (s *ImgServerSuite) Test_JSNoS3NoEFS() {
# 	const longTextLen = int64(1024)

# 	s.serve(func(ctx context.Context, ts *httptest.Server) {
# 		res := s.request(ctx, ts, "/"+jsNonExistentPathL, chromeAcceptHeader)

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
# 		s.assertS3SrcNotExists(ctx, jsNonExistentPathL)
# 	})
# }

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
