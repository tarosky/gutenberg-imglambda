import datetime
import logging
import sys
from logging import Logger
from typing import Any

from pythonjsonlogger.jsonlogger import JsonFormatter

import imglambda
from imglambda.typing import Request, Response


class MyJsonFormatter(JsonFormatter):

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

  log = logging.getLogger(__name__)
  log_handler = logging.StreamHandler()
  log_handler.setFormatter(MyJsonFormatter())
  log_handler.setLevel(logging.DEBUG)
  log_handler.setStream(sys.stderr)
  log.addHandler(log_handler)
  log.propagate = False
  return log


log = init_logging()


def get_header(req: Request, name: str, default: str) -> str:
  if name not in req['headers']:
    return default

  if req['headers'][name][0]['value'] == '':
    return default

  return req['headers'][name][0]['value']


def override_cache_control(req: Request, res: Response) -> bool:
  if 'cache-control' not in res['headers']:
    return False

  if 'x-res-cache-control-overridable' not in req['headers']:
    return False

  return req['headers']['x-res-cache-control-overridable'][0]['value'] == 'true'


def new_cache_control(req: Request, res: Response) -> str:
  if 400 <= int(res['status']) < 600:
    error_max_age = int(get_header(req, 'x-env-error-max-age', '0'))
    return f'public, max-age={error_max_age}'

  if override_cache_control(req, res):
    return res['headers']['cache-control'][0]['value']

  return get_header(req, 'x-res-cache-control', 'public, max-age=0')


def lambda_main(req: Request, res: Response) -> Response:
  cache_control = new_cache_control(req, res)
  path = req['uri'][1:]
  log.debug({
      'message': 'new cache-control',
      'cache-control': cache_control,
      'path': path,
  })
  res['headers']['cache-control'] = [{'value': cache_control}]
  return res
