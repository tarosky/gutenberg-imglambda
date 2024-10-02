from originresponse import index
from typing import Any, Dict


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
  # # For debugging
  # print('event:')
  # print(json.dumps(event))

  cf: Dict[str, Any] = event['Records'][0]['cf']
  ret = index.main(cf['request'], cf['response'])

  # # For debugging
  # print('return:')
  # print(json.dumps(ret))

  return ret
