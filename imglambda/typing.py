from typing import Dict, List, Literal, NotRequired, ReadOnly, TypedDict


class Header(TypedDict):
  key: NotRequired[ReadOnly[str]]
  value: str


class S3Origin(TypedDict):
  customHeaders: Dict[str, List[Header]]
  domainName: str
  path: str
  readTimeout: int
  responseCompletionTimeout: int
  authMethod: Literal['origin-access-identity', 'none']
  region: NotRequired[str]


class CustomOrigin(TypedDict):
  customHeaders: Dict[str, List[Header]]
  domainName: str
  path: str
  keepaliveTimeout: int
  port: int
  protocol: Literal['http', 'https']
  readTimeout: int
  responseCompletionTimeout: int
  sslProtocols: List[Literal['TLSv1.2', 'TLSv1.1', 'TLSv1', 'SSLv3']]


class Origin(TypedDict):
  custom: NotRequired[CustomOrigin]
  s3: NotRequired[S3Origin]


class Body(TypedDict):
  inputTruncated: ReadOnly[bool]
  action: Literal['read-only', 'replace']
  encoding: Literal['base64', 'text']
  data: str


class Request(TypedDict):
  method: ReadOnly[Literal[
      'GET', 'HEAD', 'OPTIONS', 'TRACE', 'PUT', 'DELETE', 'POST', 'PATCH', 'CONNECT']]
  uri: str
  querystring: str
  headers: Dict[str, List[Header]]
  clientIp: ReadOnly[str]
  body: NotRequired[Body]
  origin: Origin


class OriginRequestConfig(TypedDict):
  distributionDomainName: ReadOnly[str]
  distributionId: ReadOnly[str]
  eventType: ReadOnly[Literal['origin-request']]
  requestId: ReadOnly[str]


class OriginRequestRecord(TypedDict):
  config: ReadOnly[OriginRequestConfig]
  request: Request


class OriginRequestRecordContainer(TypedDict):
  cf: OriginRequestRecord


class OriginRequestEvent(TypedDict):
  Records: List[OriginRequestRecordContainer]


class OriginResponseConfig(TypedDict):
  distributionDomainName: ReadOnly[str]
  distributionId: ReadOnly[str]
  eventType: ReadOnly[Literal['origin-response']]
  requestId: ReadOnly[str]


class Response(TypedDict):
  headers: Dict[str, List[Header]]
  status: str
  statusDescription: str


class OriginResponseRecord(TypedDict):
  config: ReadOnly[OriginResponseConfig]
  request: Request
  response: Response


class OriginResponseRecordContainer(TypedDict):
  cf: OriginResponseRecord


class OriginResponseEvent(TypedDict):
  Records: List[OriginResponseRecordContainer]


class ResponseResult(TypedDict):
  body: NotRequired[str]
  bodyEncoding: NotRequired[Literal['text', 'base64']]
  headers: NotRequired[Dict[str, List[Header]]]
  status: str
  statusDescription: NotRequired[str]
