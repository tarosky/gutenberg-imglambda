import asyncio
import json
import logging
import os

from aiobotocore import session
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

PATH_METADATA = 'original-path'
TIMESTAMP_METADATA = 'original-timestamp'
RES_CACHE_CONTROL = 'x-res-cache-control'
HEADERS = 'headers'

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


def json_dump(obj):
  return json.dumps(obj, separators=(',', ':'), sort_keys=True)


def is_no_such_key_client_error(exception):
  return exception['Error']['Code'] == 'NoSuchKey'


def get_normalized_extension(path):
  n = path.lower()
  for le in long_exts:
    if n.endswith(le):
      return le
  _, ext = os.path.splitext(path)
  return ext


class ImgServer:
  instaces = {}

  def __init__(
      self, region, sqs, s3, s3_domain, public_content_bucket, s3_prefix,
      perm_resp_max_age, temp_resp_max_age):
    self.region = region
    self.sqs = sqs
    self.s3 = s3
    self.s3_domain = s3_domain
    self.s3_bucket = s3_domain.split('.', 1)[0]
    self.public_content_bucket = public_content_bucket
    self.s3_prefix = s3_prefix
    self.perm_resp_max_age = perm_resp_max_age
    self.temp_resp_max_age = temp_resp_max_age

  @classmethod
  async def from_lambda(cls, req):
    region = req[HEADERS]['x-env-region']
    s3_domain = req[HEADERS]['x-env-s3-dns']
    public_content_bucket = req['origin']['s3']['domainName'].split('.', 1)[0]
    s3_prefix = req[HEADERS]['x-env-s3-prefix']
    perm_resp_max_age = req[HEADERS]['x-env-perm-resp-max-age']
    temp_resp_max_age = req[HEADERS]['x-env-temp-resp-max-age']

    server_key = (
        region, s3_domain, public_content_bucket, s3_prefix, perm_resp_max_age,
        temp_resp_max_age)

    if server_key not in cls.instaces:
      sess = session.get_session()
      sqs = await sess.create_client('sqs', region_name=region).__aenter__()
      s3 = await sess.create_client('s3', region_name=region).__aenter__()
      cls.instaces[server_key] = cls(
          region=region,
          sqs=sqs,
          s3=s3,
          s3_domain=s3_domain,
          public_content_bucket=public_content_bucket,
          s3_prefix=s3_prefix,
          perm_resp_max_age=perm_resp_max_age,
          temp_resp_max_age=temp_resp_max_age)

    return cls.instaces[server_key]
    # session = get_session()
    # async with session.create_client('s3', region_name='us-west-2',
    #                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    #                                aws_access_key_id=AWS_ACCESS_KEY_ID) as client:
    #     # upload object to amazon s3
    #     data = b'\x01'*1024
    #     resp = await client.put_object(Bucket=bucket,
    #                                         Key=key,
    #                                         Body=data)
    #     print(resp)

    #     # getting s3 object properties of file we just uploaded
    #     resp = await client.get_object_acl(Bucket=bucket, Key=key)
    #     print(resp)

  def as_permanent(self, req):
    req[HEADERS][RES_CACHE_CONTROL] = {
        'key': RES_CACHE_CONTROL,
        'value': 'public, max-age={}'.format(self.perm_resp_max_age),
    }
    return req

  def as_temporary(self, req):
    req[HEADERS][RES_CACHE_CONTROL] = {
        'key': RES_CACHE_CONTROL,
        'value': 'public, max-age={}'.format(self.temp_resp_max_age),
    }
    return req

  def origin_as_generated(self, req, gen_key):
    req['origin']['s3']['domainName'] = self.s3_domain
    req['origin']['s3']['path'] = gen_key
    return req

  def supports_webp(self, accept_header):
    return 'image/webp' in accept_header

  def time_original(self, path):
    try:
      res = self.s3.head_object(
          Bucket=self.s3_bucket, Key=self.s3_prefix + path)
    except ClientError as e:
      if is_no_such_key_client_error():
        return None
      raise e

    return res['LastModified']

  def time_generated(self, path):
    try:
      res = self.s3.head_object(
          Bucket=self.s3_bucket, Key=self.s3_prefix + path)
    except ClientError as e:
      if is_no_such_key_client_error():
        return None
      raise e

    return res['Metadata'].get(TIMESTAMP_METADATA, None)

  async def generate_and_respond_with_original(self, path, gen_key, req):
    try:
      # TODO: Use ThreadPool
      t_gen = self.time_generated(gen_key)
      t_orig = self.time_original(path)

      if t_gen is None or t_gen != t_orig:
        # TODO: Enqueue
        pass

      return self.as_permanent(req)
    except Exception as e:
      log.warning(
          json_dump({
              'path': path,
              'gen_key': gen_key,
              'error': e.message,
          }))
      return self.as_permanent(req)

  async def res_with_generated_or_generate_and_res_with_original(
      self, path, gen_key, req):
    try:
      # TODO: Use ThreadPool
      t_gen = self.time_generated(gen_key)
      t_orig = self.time_original(path)

      if t_gen is not None and t_gen == t_orig:
        return self.as_permanent(self.origin_as_generated(req, gen_key))

      # TODO: Enqueue

      return self.as_temporary(req)
    except Exception as e:
      log.warning(
          json_dump({
              'path': path,
              'gen_key': gen_key,
              'error': e.message,
          }))
      return self.as_temporary(req)

  async def res_with_original_or_generated(self, path, gen_key, req):
    if self.time_original(path) is not None:
      return self.as_permanent(req)
    return self.as_permanent(self.origin_as_generated(req, gen_key))

  async def respond_with_original(self, req):
    return self.as_permanent(req)

  async def process(self, path, accept_header, req):
    ext = get_normalized_extension(path)
    gen_key = self.s3_prefix + path

    if ext in image_exts:
      if self.supports_webp(accept_header):
        return asyncio.run(
            self.res_with_generated_or_generate_and_res_with_original(
                path, gen_key + '.webp', req))
      else:
        return asyncio.run(
            self.generate_and_respond_with_original(path, gen_key, req))
    elif ext in minifiable_exts:
      return asyncio.run(
          self.res_with_generated_or_generate_and_res_with_original(
              path, gen_key, req))
    elif ext == '.js.map':
      return asyncio.run(
          self.res_with_original_or_generated(path, gen_key, req))
    else:
      # This condition includes:
      #   '.min.js' and '.min.css'
      return self.respond_with_original(req)


async def main(event, context):
  req = event['Records'][0]['cf']['request']
  server = await ImgServer.from_lambda(req)
  accept_header = req[HEADERS].get('accept', {'value': ''})['value']
  return await server.process(req['uri'], accept_header, req)


def lambda_handler(event, context):
  return asyncio.run(main(event, context))
