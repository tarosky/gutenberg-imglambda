import datetime
import logging
import sys
from typing import Any, Dict

from pythonjsonlogger.jsonlogger import JsonFormatter

HEADERS = 'headers'
ERROR_MAX_AGE = 'x-env-error-max-age'
VALUE = 'value'
MESSAGE = 'message'

# https://stackoverflow.com/a/11548754/1160341
logging.getLogger().setLevel(logging.DEBUG)


class MyJsonFormatter(JsonFormatter):

  def add_fields(self, log_record: Any, record: Any, message_dict: Any) -> None:
    log_record['_ts'] = datetime.datetime.utcnow().strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ')

    if log_record.get('level'):
      log_record['level'] = log_record['level'].upper()
    else:
      log_record['level'] = record.levelname

    super().add_fields(log_record, record, message_dict)


def logger() -> logging.Logger:
  log = logging.getLogger(__name__)
  log_handler = logging.StreamHandler()
  log_handler.setFormatter(MyJsonFormatter())
  log_handler.setLevel(logging.DEBUG)
  log_handler.setStream(sys.stderr)
  log.addHandler(log_handler)
  return log


log = logger()


def new_cache_control(req: Dict[str, Any], res: Dict[str, Any]) -> str:
  if 400 <= int(res['status']) < 600:
    error_max_age = int(req[HEADERS].get(ERROR_MAX_AGE, {VALUE: '0'})[VALUE])
    return f'public, max-age={error_max_age}'

  return req[HEADERS].get('x-res-cache-control',
                          {VALUE: 'public, max-age=0'})[VALUE]


def main(req: Dict[str, Any], res: Dict[str, Any]) -> Dict[str, Any]:
  cache_control = new_cache_control(req, res)
  path = req['uri'][1:]
  log.debug(
      {
          MESSAGE: 'new cache-control',
          'cache-control': cache_control,
          'path': path,
      })
  res[HEADERS]['cache-control'] = {VALUE: cache_control}
  return res


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
  cf: Dict[str, Any] = event['Records'][0]['cf']
  return main(cf['request'], cf['response'])
