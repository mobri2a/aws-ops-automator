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
from datetime import timedelta

import pytz
import services.dynamodb_service
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from outputs import raise_value_error

GROUP_TITLE_DELETE_OPTIONS = "Backup delete options"

PARAM_DESC_RETENTION_COUNT = "Number of backups to keep for an instance, use 0 to use retention days"
PARAM_DESC_RETENTION_DAYS = "Backup retention period in days, use 0 to use retention count"

PARAM_LABEL_RETENTION_COUNT = "Retention count"
PARAM_LABEL_RETENTION_DAYS = "Retention days"

ERR_RETENTION_PARAM_BOTH = "Only one of {} or {} parameters can be specified"
ERR_RETENTION_PARAM_NONE = "{} or {} parameter must be specified"
ERR_DELETING_BACKUP = "Error deleting backup {} for table {}, {}"

INF_ART_DELETE_BACKUPS = "Deleting backups for table {} in account {},region {}"
INF_KEEP_RETENTION_COUNT = "Retaining latest {} backups for each DynamoDB table"
INF_RETENTION_DAYS = "Deleting DynamoDB table backups older than {}"
INF_SN_DELETE_RETENTION_COUNT = "Deleting backup {}, because count for its table is {}"
INF_SN_RETENTION_DAYS = "Deleting backup {} ({}) because it is older than retention period of {} days"
INF_DELETED_BACKUP = "Deleted backup {}"
INF_BACKUPS_FOR_TABLE = "Number of backups for table {} is {}"
INF_BACKUP_DELETED = "Deleted backup {} for table {}"

PARAM_RETENTION_DAYS = "RetentionDays"
PARAM_RETENTION_COUNT = "RetentionCount"


class DynamodbDeleteBackupAction(ActionBase):
    properties = {
        ACTION_TITLE: "DynamoDB delete Backup",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Deletes DynamoDB after retention period or count",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "00a6a972-47bb-4517-a8ba-f03f4c04166e",

        ACTION_SERVICE: "dynamodb",
        ACTION_RESOURCES: services.dynamodb_service.TABLES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_MIN_INTERVAL_MIN: 5,

        ACTION_KEEP_RESOURCE_TAGS: True,

        ACTION_PARAMETERS: {
            PARAM_RETENTION_DAYS: {
                PARAM_DESCRIPTION: PARAM_DESC_RETENTION_DAYS,
                PARAM_TYPE: type(0),
                PARAM_REQUIRED: False,
                PARAM_MIN_VALUE: 0,
                PARAM_LABEL: PARAM_LABEL_RETENTION_DAYS
            },
            PARAM_RETENTION_COUNT: {
                PARAM_DESCRIPTION: PARAM_DESC_RETENTION_COUNT,
                PARAM_TYPE: type(0),
                PARAM_REQUIRED: False,
                PARAM_MIN_VALUE: 0,
                PARAM_LABEL: PARAM_LABEL_RETENTION_COUNT
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_DELETE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_RETENTION_DAYS,
                    PARAM_RETENTION_COUNT

                ],
            }],

        ACTION_PERMISSIONS: [
            "dynamodb:ListTables",
            "dynamodb:ListBackups",
            "dynamodb:ListTagsOfResource",
            "dynamodb:DeleteBackup"
        ]
    }

    @staticmethod
    def action_logging_subject(arguments, _):
        table = arguments[ACTION_PARAM_RESOURCES]
        table_name = table["TableName"]
        account = table["AwsAccount"]
        region = table["Region"]
        return "{}-{}-{}-{}".format(account, region, table_name, log_stream_date())

    @staticmethod
    def action_validate_parameters(parameters, _, __):

        retention_days = parameters.get(PARAM_RETENTION_DAYS)
        retention_count = parameters.get(PARAM_RETENTION_COUNT)
        if not retention_count and not retention_days:
            raise_value_error(ERR_RETENTION_PARAM_NONE, PARAM_RETENTION_COUNT, PARAM_RETENTION_DAYS)

        if retention_days and retention_count:
            raise_value_error(ERR_RETENTION_PARAM_BOTH, PARAM_RETENTION_COUNT, PARAM_RETENTION_DAYS)

        return parameters

    @property
    def db_client(self):
        if self._db_client is None:
            methods = ["delete_backup",
                       "list_tables",
                       "list_backups"]

            self._db_client = get_client_with_retries("dynamodb",
                                                      methods,
                                                      region=self.table["Region"],
                                                      session=self._session_,
                                                      logger=self._logger_)

        return self._db_client

    @property
    def _table_backups(self):

        db = services.create_service("dynamodb",
                                     session=self._session_,
                                     service_retry_strategy=get_default_retry_strategy("dynamodb",
                                                                                       context=self._context_))
        available_backups = "BackupSummaries[?BackupStatus=='AVAILABLE']"
        return list(db.describe(services.dynamodb_service.BACKUPS,
                                TableName=self.table_name,
                                select=available_backups,
                                region=self._region_))

    def __init__(self, action_arguments, action_parameters):

        self._retention_days_ = None
        self._retention_count_ = None

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.table = self._resources_
        self.table_name = self.table["TableName"]

        self._db_client = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_
        }

    def execute(self):

        def backups_to_delete(table_backups):

            def by_retention_days():

                utc_time_now = self._datetime_.utcnow().replace(tzinfo=pytz.timezone("UTC"))
                delete_before_dt = utc_time_now - timedelta(
                    days=int(self._retention_days_))
                self._logger_.info(INF_RETENTION_DAYS, delete_before_dt)

                for bk in table_backups:
                    backup_dt = bk["BackupCreationDateTime"].replace(tzinfo=pytz.timezone("UTC"))
                    if backup_dt < delete_before_dt:
                        self._logger_.info(INF_SN_RETENTION_DAYS, bk["BackupName"], bk["BackupCreationDateTime"],
                                           self._retention_days_)
                        yield bk

            def by_retention_count():

                self._logger_.info(INF_KEEP_RETENTION_COUNT, self._retention_count_)
                sorted_backups = sorted(table_backups,
                                        key=lambda b: b["BackupCreationDateTime"],
                                        reverse=True)
                count_for_table = 0
                for bk in sorted_backups:

                    count_for_table += 1
                    if count_for_table > self._retention_count_:
                        self._logger_.info(INF_SN_DELETE_RETENTION_COUNT, bk["BackupName"], count_for_table)
                        yield bk

            return by_retention_days() if self._retention_days_ else by_retention_count()

        deleted = []

        self._logger_.info(INF_ART_DELETE_BACKUPS, self.table_name, self._account_, self._region_)

        backups_for_table = self._table_backups
        self._logger_.info(INF_BACKUPS_FOR_TABLE, self.table_name, len(backups_for_table))

        for backup in backups_to_delete(backups_for_table):
            try:
                self.db_client.delete_backup(BackupArn=backup["BackupArn"])
                self._logger_.info(INF_DELETED_BACKUP, backup["BackupName"])
                deleted.append({a: backup[a] for a in backup if a not in ["AwsAccount", "Region", "ResourceTypeName", "Service"]})
            except Exception as ex:
                self._logger_.error(ERR_DELETING_BACKUP, backup["BackupName"], self.table_name, ex)

        self.result.update({
            "backups": len(backups_for_table),
            "backups-deleted": deleted,
            METRICS_DATA: build_action_metrics(self, DeletedTableBackups=deleted)

        })

        return self.result
