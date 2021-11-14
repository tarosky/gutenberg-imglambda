# AWS Headquarters

AWS Headquarters (AWSHQ) is a set of CloudFormation templates which manages AWS accounts.

## AWSHQ Autoacc

AWSHQ Autoacc is a CloudFormation template which automates creation of AWS accounts.

### Usage

Run Step Functions `AutoaccStateMachine-*` with the following parameter:

```json
{
  "account": "myproj",
  "env": "test",
  "group": "level3"
}
```

Be sure to check the final output.  There's an important message!

### Parameters
#### `account`

`account` is the name of an account you want to create.

The name must match regex: `r'^[a-z][a-z0-9]{2,16}$'`.

#### `env`

`env` is the environment of an account you want to create, such as `production` or `staging`.

Allowed values are:

* `production`
* `staging`
* `demo`
* `test`
* `development`
* `lab`

#### `group`

`group` is the importance of the account.  `group` is used to decide the AWSHQ stack set to which the account belongs.

Allowed values are:

* `level1` - AWS admin's development account, which is completely under control
* `level2` - account of Tarosky's experimental projects and employee's lab accounts
* `level3` - all other accounts
* `manual` - never associate the account with AWSHQ stack
