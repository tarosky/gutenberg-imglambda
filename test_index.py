import os
import secrets

from aiobotocore import session

from index import ImgServer

PERM_RESP_MAX_AGE = 365 * 24 * 60 * 60
TEMP_RESP_MAX_AGE = 20 * 60
S3_PREFIX = 'prefix'


def read_test_config(name: str) -> str:
  path = f'{os.getcwd()}/config/test/{name}'
  with open(path, 'r') as f:
    return f.read().strip()


def generate_safe_random_string() -> str:
  return secrets.token_urlsafe(256 // 8)


async def create_img_server(name: str) -> ImgServer:
  region = 'ap-northeast-1'
  account_id = read_test_config('aws-account-id')
  sqs_name = f'test-{name}-{generate_safe_random_string()}'
  access_key_id = read_test_config('access-key-id')
  s3_bucket = read_test_config('s3-bucket')
  secret_access_key = read_test_config('secret-access-key')

  sess = session.get_session()
  sqs = await sess.create_client(
      'sqs',
      region_name=region,
      aws_access_key_id=access_key_id,
      aws_secret_access_key=secret_access_key).__aenter__()
  s3 = await sess.create_client(
      's3',
      region_name=region,
      aws_access_key_id=access_key_id,
      aws_secret_access_key=secret_access_key).__aenter__()

  return ImgServer(
      region=region,
      sqs=sqs,
      s3=s3,
      s3_domain=f'{generate_safe_random_string()}.s3.example.com',
      public_content_bucket=read_test_config('public-content-bucket'),
      s3_prefix=S3_PREFIX,
      sqs_queue_url=(
          f'https://sqs.{region}.amazonaws.com/{account_id}/{sqs_name}'),
      perm_resp_max_age=PERM_RESP_MAX_AGE,
      temp_resp_max_age=TEMP_RESP_MAX_AGE)


# func getTestSQSQueueNameFromURL(url string) string {
# 	parts := strings.Split(url, "/")
# 	return parts[len(parts)-1]
# }

# func newTestEnvironment(ctx context.Context, name string, s *TestSuite) *Environment {
# 	e := NewEnvironment(ctx, getTestConfig(name))

# 	sqsName := getTestSQSQueueNameFromURL(e.SQSQueueURL)

# 	_, err := e.SQSClient.CreateQueue(s.ctx, &sqs.CreateQueueInput{
# 		QueueName: &sqsName,
# 	})
# 	require.NoError(s.T(), err, "failed to create SQS queue")

# 	return e
# }

# func initTestSuite(name string, t require.TestingT) *TestSuite {
# 	InitTest()
# 	require.NoError(t, os.RemoveAll("work/test/"+name), "failed to remove directory")
# 	ctx := context.Background()

# 	return &TestSuite{ctx: ctx}
# }

# func cleanTestEnvironment(ctx context.Context, s *TestSuite) {
# 	if _, err := s.env.SQSClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
# 		QueueUrl: &s.env.SQSQueueURL,
# 	}); err != nil {
# 		s.env.log.Error("failed to clean up SQS queue", zap.Error(err))
# 	}
# }

# // TestSuite holds configs and sessions required to execute program.
# type TestSuite struct {
# 	suite.Suite
# 	env *Environment
# 	ctx context.Context
# }

# func copy(ctx context.Context, src, dst string, s *TestSuite) {
# 	in, err := os.Open(src)
# 	s.Require().NoError(err)
# 	defer func() {
# 		s.Require().NoError(in.Close())
# 	}()

# 	info, err := in.Stat()
# 	s.Require().NoError(err)

# 	{
# 		_, err := s.env.S3Client.PutObject(ctx, &s3.PutObjectInput{
# 			Bucket:       &s.env.S3Bucket,
# 			Key:          &dst,
# 			Body:         in,
# 			StorageClass: types.StorageClassStandardIa,
# 			Metadata: map[string]string{
# 				pathMetadata:      src,
# 				timestampMetadata: info.ModTime().UTC().Format(time.RFC3339Nano),
# 			},
# 		})
# 		s.Require().NoError(err)
# 	}
# }
