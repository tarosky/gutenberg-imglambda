from originrequest import index
from typing import Any, Dict


def lambda_handler(event: Dict[str, Any], _: Any) -> Any:
  # # For debugging
  # print('event:')
  # print(json.dumps(event))

  ret = index.lambda_main(event)

  # # For debugging
  # print('return:')
  # print(json.dumps(ret))

  return ret
