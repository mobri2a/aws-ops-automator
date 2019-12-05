###################################################################################################################### 
#  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           # 
#                                                                                                                    # 
#  Licensed under the Apache License Version 2.0 (the "License"). You may not use this file except in compliance     # 
#  with the License. A copy of the License is located at                                                             # 
#                                                                                                                    # 
#      http://www.apache.org/licenses/                                                                               # 
#                                                                                                                    # 
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES # 
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    # 
#  and limitations under the License.                                                                                # 
######################################################################################################################
import services.dynamodb_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json

TAG_PLACEHOLDER_TABLE_NAME = "table-name"
TAG_PLACEHOLDER_BACKUP_NAME = "backup-name"

BACKUP_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"

GROUP_TITLE_BACKUP_OPTIONS = "Backup creation options"

PARAM_BACKUP_NAME = "BackupName"
PARAM_TABLE_TAGS = "TableTags"

PARAM_DESC_BACKUP_NAME = "Name of the created image, leave blank for default name tablename-yyyymmddhhmm"
PARAM_DESC_TABLE_TAGS = "Tags to set on source table after backup is completed. Note that tag values for DynamoDB cannot " \
                        "contain ',' characters. When specifying multiple values for follow-up tasks for the backup in the " \
                        "value of the OpsAutomatorTask tag, or any other tag used to list the actions,  " \
                        "use the '/' character instead"

PARAM_LABEL_BACKUP_NAME = "Backup name"
PARAM_LABEL_TABLE_TAGS = "Table tags"

ERR_CREATING_BACKUP = "Error creating backup with name \"{}\" for  table \"{}\", {}"


class DynamodbCreateBackupAction(ActionBase):
    """
    Implements action to create image for an EC2 instance
    """
    properties = {
        ACTION_TITLE: "DynamoDB Create backup",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Creates a backup for a DynamoDB Table",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "479023f5-bc36-4d02-bbda-40cc559f67fc",

        ACTION_SERVICE: "dynamodb",
        ACTION_RESOURCES: services.dynamodb_service.TABLES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_MIN_INTERVAL_MIN: 5,

        ACTION_SELECT_PARAMETERS: {"tags": False},

        ACTION_PARAMETERS: {
            PARAM_BACKUP_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_BACKUP_NAME,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_BACKUP_NAME
            },
            PARAM_TABLE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_TABLE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_LABEL: PARAM_LABEL_TABLE_TAGS
            },
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_BACKUP_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_BACKUP_NAME,
                    PARAM_TABLE_TAGS
                ],
            }],

        ACTION_PERMISSIONS: ["dynamodb:CreateBackup",
                             "dynamodb:DescribeBackup",
                             "dynamodb:ListTables",
                             "dynamodb:ListTagsOfResource",
                             "dynamodb:TagResource",
                             "dynamodb:UntagResource"],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.table = self._resources_
        self.table_name = self.table["TableName"]

        self._db_client = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "table": self.table_name,
            "task": self._task_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        table = arguments[ACTION_PARAM_RESOURCES]
        table_name = table["TableName"]
        account = table["AwsAccount"]
        region = table["Region"]
        return "{}-{}-{}-{}".format(account, region, table_name, log_stream_date())

    @property
    def db_client(self):
        if self._db_client is None:
            methods = [
                "create_backup",
                "list_tables",
                "tag_resource",
                "untag_resource"
            ]

            self._db_client = get_client_with_retries("dynamodb", methods,
                                                      region=self.table["Region"],
                                                      session=self._session_,
                                                      logger=self._logger_)

        return self._db_client

    def _set_table_tags(self, backup_name):

        tags = self.build_tags_from_template(PARAM_TABLE_TAGS,
                                             tag_variables={
                                                 TAG_PLACEHOLDER_BACKUP_NAME: backup_name
                                             }, restricted_value_set=True)

        if len(tags) > 0:

            arn = "arn:aws:dynamodb:{}:{}:table/{}".format(self._region_, self._account_, self.table_name)

            try:
                tagging.set_dynamodb_tags(ddb_client=self.db_client,
                                          resource_arns=[arn],
                                          tags=tags,
                                          logger=self._logger_)

                self._logger_.info("Set tags {} to table {}", ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                   self.table_name)
            except Exception as ex:
                raise Exception("Error setting tags to instance {}, {}".format(self.table_name, ex))

    def is_completed(self, backup_start_data):

        self._logger_.info("Create start result data is {}", safe_json(backup_start_data, indent=3))

        backup_name = backup_start_data["backup"]['BackupName']
        self._logger_.info("Checking status of image {}", backup_name)

        backup_arn = backup_start_data["backup"]["BackupArn"]

        # create service instance to test is image is available
        db = services.create_service("dynamodb", session=self._session_,
                                     service_retry_strategy=get_default_retry_strategy("dynamodb", context=self._context_))

        # get image information
        backup = db.get(services.dynamodb_service.BACKUP,
                        BackupArn=backup_arn,
                        region=self.table["Region"])

        if backup is None:
            raise Exception("Backup {} for table {} not found".format(backup_name, self.table_name))

        self._logger_.debug("Backup description is {}", safe_json(backup, indent=3))

        # get and test image state
        status = backup["BackupDetails"]["BackupStatus"]
        self._logger_.info("Backup status is {}", status)

        if status == "CREATING":
            self._logger_.info("Backup is not being created")
            return None

        # backup creation is done
        if status == "AVAILABLE":
            # tag created image
            self._set_table_tags(backup_name)

            return self.result

        return None

    def execute(self):
        """
        Starts creation of an backup for an DynamoDB instance
        :return: result of the action
        """
        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info("Start creating backup for table {} for task {}", self.table_name, self._task_)

        backup_name = self.build_str_from_template(parameter_name=PARAM_BACKUP_NAME,
                                                   tag_variables={
                                                       TAG_PLACEHOLDER_TABLE_NAME: self.table_name
                                                   })
        if backup_name == "":
            dt = self._datetime_.utcnow()
            backup_name = BACKUP_NAME.format(self.table_name, dt.year, dt.month, dt.day, dt.hour, dt.minute)

        try:
            backup = self.db_client.create_backup_with_retries(TableName=self.table_name, BackupName=backup_name)["BackupDetails"]

            self.result["backup"] = {a: backup[a] for a in backup if
                                     a not in ["AwsAccount", "Region", "ResourceTypeName", "Service"]}

            self.result[METRICS_DATA] = build_action_metrics(
                action=self,
                Backups=1)

        except Exception as ex:
            self._logger_.error(ERR_CREATING_BACKUP, backup_name, self.table_name, ex)
            raise ex

        return self.result
