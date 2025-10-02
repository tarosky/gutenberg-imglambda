from aws_lambda_powertools.utilities.typing import LambdaContext

from imglambda.originrequest import index as originrequest
from imglambda.originresponse import index as originresponse
from imglambda.typing import (
    OriginRequestEvent,
    OriginResponseEvent,
    Request,
    ResizeRequestPayload,
    ResizeResponsePayload,
    Response,
    ResponseResult
)


def origin_request_lambda_handler(
    event: OriginRequestEvent,
    _: LambdaContext,
) -> Request | ResponseResult:
  # # For debugging
  # print('event:')
  # print(json.dumps(event))

  ret = originrequest.lambda_main(event)

  # # For debugging
  # print('return:')
  # print(json.dumps(ret))

  return ret


def resize_lambda_handler(
    event: ResizeRequestPayload,
    _: LambdaContext,
) -> ResizeResponsePayload:
  # # For debugging
  # print('event:')
  # print(json.dumps(event))

  ret = originrequest.lambda_resize(event)

  # # For debugging
  # print('return:')
  # print(json.dumps(ret))

  return ret


def origin_response_lambda_handler(
    event: OriginResponseEvent,
    _: LambdaContext,
) -> Response:
  # # For debugging
  # print('event:')
  # print(json.dumps(event))

  cf = event['Records'][0]['cf']
  ret = originresponse.lambda_main(cf['request'], cf['response'])

  # # For debugging
  # print('return:')
  # print(json.dumps(ret))

  return ret
