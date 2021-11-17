import asyncio
import dataclasses
import json
import logging
import os
from typing import Any, Dict, Optional, Tuple

from aiobotocore import session
from aiobotocore.client import AioBaseClient
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

TIMESTAMP_METADATA = 'original-timestamp'
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


@dataclasses.dataclass
class FieldUpdate:
  res_cache_control: Optional[str] = None
  origin_domain: Optional[str] = None
  origin_path: Optional[str] = None


def json_dump(obj: Any) -> str:
  return json.dumps(obj, separators=(',', ':'), sort_keys=True)


def is_no_such_key_client_error(exception: ClientError) -> bool:
  return exception['Error']['Code'] == 'NoSuchKey'


def get_normalized_extension(path: str) -> str:
  n = path.lower()
  for le in long_exts:
    if n.endswith(le):
      return le
  _, ext = os.path.splitext(path)
  return ext


class ImgServer:
  instances: Dict[Tuple[str, str, str, str, str, int, int], 'ImgServer'] = {}

  def __init__(
      self,
      region: str,
      sqs: AioBaseClient,
      s3: AioBaseClient,
      s3_domain: str,
      public_content_bucket: str,
      s3_prefix: str,
      sqs_queue_url: str,
      perm_resp_max_age: int,
      temp_resp_max_age: int,
  ):
    self.region = region
    self.sqs = sqs
    self.s3 = s3
    self.s3_domain = s3_domain
    self.s3_bucket = s3_domain.split('.', 1)[0]
    self.public_content_bucket = public_content_bucket
    self.s3_prefix = s3_prefix
    self.sqs_queue_url = sqs_queue_url
    self.perm_resp_max_age = perm_resp_max_age
    self.temp_resp_max_age = temp_resp_max_age

  @classmethod
  async def from_lambda(cls, req: Dict[str, Any]) -> 'ImgServer':
    region: str = req[HEADERS]['x-env-region']
    s3_domain: str = req[HEADERS]['x-env-s3-dns']
    public_content_bucket: str = req['origin']['s3']['domainName'].split(
        '.', 1)[0]
    s3_prefix: str = req[HEADERS]['x-env-s3-prefix']
    sqs_queue_url: str = req[HEADERS]['x-env-sqs-queue-url']
    perm_resp_max_age: int = int(req[HEADERS]['x-env-perm-resp-max-age'])
    temp_resp_max_age: int = int(req[HEADERS]['x-env-temp-resp-max-age'])

    server_key = (
        region,
        s3_domain,
        public_content_bucket,
        s3_prefix,
        sqs_queue_url,
        perm_resp_max_age,
        temp_resp_max_age,
    )

    if server_key not in cls.instances:
      sess = session.get_session()
      sqs = await sess.create_client('sqs', region_name=region).__aenter__()
      s3 = await sess.create_client('s3', region_name=region).__aenter__()
      cls.instances[server_key] = cls(
          region=region,
          sqs=sqs,
          s3=s3,
          s3_domain=s3_domain,
          public_content_bucket=public_content_bucket,
          s3_prefix=s3_prefix,
          sqs_queue_url=sqs_queue_url,
          perm_resp_max_age=perm_resp_max_age,
          temp_resp_max_age=temp_resp_max_age)

    return cls.instances[server_key]

  def as_permanent(self, update: FieldUpdate) -> FieldUpdate:
    update.res_cache_control = 'public, max-age={}'.format(
        self.perm_resp_max_age)
    return update

  def as_temporary(self, update: FieldUpdate) -> FieldUpdate:
    update.res_cache_control = 'public, max-age={}'.format(
        self.temp_resp_max_age)
    return update

  def origin_as_generated(
      self, gen_key: str, update: FieldUpdate) -> FieldUpdate:
    update.origin_domain = self.s3_domain
    update.origin_path = gen_key
    return update

  def supports_webp(self, accept_header: str) -> bool:
    return 'image/webp' in accept_header

  async def get_time_original(self, path: str) -> Optional[str]:
    try:
      res: Dict[str, Any] = await self.s3.head_object(
          Bucket=self.public_content_bucket, Key=path)
    except ClientError as e:
      if is_no_such_key_client_error(e):
        return None
      raise e

    return res['LastModified']

  async def get_time_generated(self, path: str) -> Optional[str]:
    try:
      res: Dict[str, Any] = await self.s3.head_object(
          Bucket=self.s3_bucket, Key=self.s3_prefix + path)
    except ClientError as e:
      if is_no_such_key_client_error(e):
        return None
      raise e

    return res['Metadata'].get(TIMESTAMP_METADATA, None)

  def needs_generation(
      self, t_orig: Optional[str], t_gen: Optional[str]) -> bool:
    return t_gen is None or t_gen != t_orig

  async def keep_uptodate(
      self, path: str, t_orig: Optional[str], t_gen: Optional[str]) -> None:
    if not self.needs_generation(t_orig, t_gen):
      return

    await self.sqs.send_message(
        QueueUrl=self.sqs_queue_url,
        MessageBody=json_dump({
            'bucket': self.s3_bucket,
            'path': path,
        }))
    log.debug(
        json_dump(
            {
                'message': 'enqueued',
                'bucket': self.s3_bucket,
                'path': path,
            }))

  async def generate_and_respond_with_original(
      self, path: str, gen_key: str) -> FieldUpdate:
    try:
      t_gen_task = asyncio.create_task(self.get_time_generated(gen_key))
      t_orig_task = asyncio.create_task(self.get_time_original(path))

      t_gen = await t_gen_task
      t_orig = await t_orig_task

      await self.keep_uptodate(path, t_orig, t_gen)

      return self.as_permanent(FieldUpdate())
    except Exception as e:
      log.warning(
          json_dump({
              'path': path,
              'gen_key': gen_key,
              'error': str(e),
          }))
      return self.as_permanent(FieldUpdate())

  async def res_with_generated_or_generate_and_res_with_original(
      self, path: str, gen_key: str) -> FieldUpdate:
    try:
      t_gen_task = asyncio.create_task(self.get_time_generated(gen_key))
      t_orig_task = asyncio.create_task(self.get_time_original(path))

      t_gen = await t_gen_task
      t_orig = await t_orig_task

      if not self.needs_generation(t_orig, t_gen):
        return self.as_permanent(
            self.origin_as_generated(gen_key, FieldUpdate()))

      await self.keep_uptodate(path, t_orig, t_gen)

      return self.as_temporary(FieldUpdate())
    except Exception as e:
      log.warning(
          json_dump({
              'path': path,
              'gen_key': gen_key,
              'error': str(e),
          }))
      return self.as_temporary(FieldUpdate())

  async def res_with_original_or_generated(
      self, path: str, gen_key: str) -> FieldUpdate:
    t_orig = await self.get_time_original(path)
    if t_orig is not None:
      return self.as_permanent(FieldUpdate())
    return self.as_permanent(self.origin_as_generated(gen_key, FieldUpdate()))

  def respond_with_original(self) -> FieldUpdate:
    return self.as_permanent(FieldUpdate())

  async def process(self, path: str, accept_header: str) -> FieldUpdate:
    ext = get_normalized_extension(path)
    gen_key = self.s3_prefix + path

    if ext in image_exts:
      if self.supports_webp(accept_header):
        return await self.res_with_generated_or_generate_and_res_with_original(
            path, gen_key + '.webp')
      else:
        return await self.generate_and_respond_with_original(path, gen_key)
    elif ext in minifiable_exts:
      return await self.res_with_generated_or_generate_and_res_with_original(
          path, gen_key)
    elif ext == '.js.map':
      return await self.res_with_original_or_generated(path, gen_key)
    else:
      # This condition includes:
      #   '.min.js' and '.min.css'
      return self.respond_with_original()


async def lambda_main(event: Dict[str, Any]) -> Dict[str, Any]:
  req: Dict[str, Any] = event['Records'][0]['cf']['request']
  accept_header: str = req[HEADERS].get('accept', {'value': ''})['value']

  server = await ImgServer.from_lambda(req)
  field_update = await server.process(req['uri'], accept_header)

  if field_update.origin_domain is not None:
    req['origin']['s3']['domainName'] = field_update.origin_domain

  if field_update.origin_path is not None:
    req['origin']['s3']['path'] = field_update.origin_path

  if field_update.res_cache_control is not None:
    req[HEADERS]['x-res-cache-control'] = {
        'key': 'X-Res-Cache-Control',
        'value': field_update.res_cache_control,
    }

  return req


def lambda_handler(event: Dict[str, Any], _: Any) -> Any:
  return asyncio.run(lambda_main(event))
