import datetime
import logging
import sys
from logging import Logger
from pathlib import Path
from typing import Any, Dict

from pythonjsonlogger.jsonlogger import JsonFormatter

HEADERS = 'headers'
VALUE = 'value'
MESSAGE = 'message'


def get_version() -> str:
  return Path(__file__).resolve().with_name('VERSION').read_text().strip()


version = get_version()


class MyJsonFormatter(JsonFormatter):

  def add_fields(self, log_record: Any, record: Any, message_dict: Any) -> None:
    log_record['_ts'] = datetime.datetime.utcnow().strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ')

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

  log = logging.getLogger(__name__)
  log_handler = logging.StreamHandler()
  log_handler.setFormatter(MyJsonFormatter())
  log_handler.setLevel(logging.DEBUG)
  log_handler.setStream(sys.stderr)
  log.addHandler(log_handler)
  log.propagate = False
  return log


log = init_logging()


def get_header(req: Dict[str, Any], name: str, default: str) -> str:
  if name not in req[HEADERS]:
    return default

  if req[HEADERS][name][0][VALUE] == '':
    return default

  return req[HEADERS][name][0][VALUE]


def override_cache_control(req: Dict[str, Any], res: Dict[str, Any]) -> bool:
  if 'cache-control' not in res[HEADERS]:
    return False

  if 'x-res-cache-control-overridable' not in req[HEADERS]:
    return False

  return req[HEADERS]['x-res-cache-control-overridable'][0][VALUE] == 'true'


def new_cache_control(req: Dict[str, Any], res: Dict[str, Any]) -> str:
  if 400 <= int(res['status']) < 600:
    error_max_age = int(get_header(req, 'x-env-error-max-age', '0'))
    return f'public, max-age={error_max_age}'

  if override_cache_control(req, res):
    return res[HEADERS]['cache-control'][0][VALUE]

  return get_header(req, 'x-res-cache-control', 'public, max-age=0')


def main(req: Dict[str, Any], res: Dict[str, Any]) -> Dict[str, Any]:
  cache_control = new_cache_control(req, res)
  path = req['uri'][1:]
  log.debug(
      {
          MESSAGE: 'new cache-control',
          'cache-control': cache_control,
          'path': path,
      })
  res[HEADERS]['cache-control'] = [{VALUE: cache_control}]
  return res


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
  # # For debugging
  # print('event:')
  # print(json.dumps(event))

  cf: Dict[str, Any] = event['Records'][0]['cf']
  ret = main(cf['request'], cf['response'])

  # # For debugging
  # print('return:')
  # print(json.dumps(ret))

  return ret
