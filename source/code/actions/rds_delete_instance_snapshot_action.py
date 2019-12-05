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

import dateutil.parser

import pytz
import services.rds_service
from actions import *
from actions import MARKER_RDS_TAG_SOURCE_DB_INSTANCE_ID
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries
from outputs import raise_exception, raise_value_error

GROUP_TITLE_DELETE_OPTIONS = "RDS Snapshot delete options"

PARAM_DESC_RETENTION_COUNT = "Number of snapshots to keep for an RDS instance, use 0 to use retention days"
PARAM_DESC_RETENTION_DAYS = "Snapshot retention period in days, use 0 to use retention count"

PARAM_LABEL_RETENTION_COUNT = "Retention count"
PARAM_LABEL_RETENTION_DAYS = "Retention days"

INFO_ACCOUNT_SNAPSHOTS = "{} RDS instance snapshots for account {}"
INFO_KEEP_RETENTION_COUNT = "Retaining latest {} snapshots for each RDS instance"
INFO_REGION = "Processing snapshots in region {}"
INFO_RETENTION_DAYS = "Deleting RDS snapshots older than {}"
INFO_SN_DELETE_RETENTION_COUNT = "Deleting RDS snapshot {}, because count for its instance is {}"
INFO_SN_RETENTION_DAYS = "Deleting RDS snapshot {} ({}) because it is older than retention period of {} days"
INFO_SNAPSHOT_DELETED = "Deleted RDS snapshot {} for instance {}"
INFO_NO_SOURCE_INSTANCE_ID_WITH_RETENTION = \
    "Original db instance id can not be retrieved for snapshot {}, original instance id is required for " \
    "use with Retention count parameter not equal to 0, snapshot skipped"

ERR_RETENTION_PARAM_BOTH = "Only one of {} or {} parameters can be specified"
ERR_RETENTION_PARAM_NONE = "{} or {} parameter must be specified"
ERR_MAX_RETENTION_COUNT_SNAPSHOTS = "Can not delete if number of snapshots is larger than {} for instance {}"
ERR_DELETING_SNAPSHOT = "Error deleting snapshot {} for RDS instance {}, ({})"

PARAM_RETENTION_DAYS = "RetentionDays"
PARAM_RETENTION_COUNT = "RetentionCount"


class RdsDeleteInstanceSnapshotAction(ActionBase):
    properties = {
        ACTION_TITLE: "RDS Delete Instance Snapshot",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Deletes RDS instance snapshots after retention period or count",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "1cc6eaa3-a08b-497d-b757-dfcbbfb4d5e3",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_SNAPSHOTS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION: "DBSnapshots[?SnapshotType=='manual'].{DBSnapshotIdentifier:DBSnapshotIdentifier, "
                                  "DBInstanceIdentifier:DBInstanceIdentifier, DBSnapshotArn:DBSnapshotArn, "
                                  "SnapshotCreateTime:SnapshotCreateTime, Status:Status} | [?Status=='available']",

        ACTION_KEEP_RESOURCE_TAGS: True,

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_SELECT_SIZE: [ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_MEDIUM],
        ACTION_COMPLETION_SIZE: [ACTION_SIZE_MEDIUM],

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
            "rds:DescribeDBSnapshots",
            "rds:ListTagsForResource",
            "rds:DeleteDBSnapshot",
            "tag:GetResources"
        ]

    }

    # noinspection PyUnusedLocal,PyUnusedLocal
    @staticmethod
    def custom_aggregation(resources, params, logger):

        if params.get(PARAM_RETENTION_COUNT, 0) == 0:
            yield resources
        else:
            snapshots_sorted_by_instanceid = sorted(resources, key=lambda k: k['DBInstanceIdentifier'])
            db_instance_id = snapshots_sorted_by_instanceid[0]["DBInstanceIdentifier"] if len(
                snapshots_sorted_by_instanceid) > 0 else None
            snapshots_for_db_instance = []
            for snapshot in snapshots_sorted_by_instanceid:
                if db_instance_id != snapshot["DBInstanceIdentifier"]:
                    yield snapshots_for_db_instance
                    db_instance_id = snapshot["DBInstanceIdentifier"]
                    snapshots_for_db_instance = [snapshot]
                else:
                    snapshots_for_db_instance.append(snapshot)
            yield snapshots_for_db_instance

    # noinspection PyUnusedLocal
    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):

        retention_days = parameters.get(PARAM_RETENTION_DAYS)
        retention_count = parameters.get(PARAM_RETENTION_COUNT)
        if not retention_count and not retention_days:
            raise_value_error(ERR_RETENTION_PARAM_NONE, PARAM_RETENTION_COUNT, PARAM_RETENTION_DAYS)

        if retention_days and retention_count:
            raise_value_error(ERR_RETENTION_PARAM_BOTH, PARAM_RETENTION_COUNT, PARAM_RETENTION_DAYS)

        return parameters

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):
        source_db_instance_id = resource.get("DBInstanceIdentifier", None)
        if source_db_instance_id is None:
            source_db_instance_id_from_tag = resource.get("Tags", {}).get(
                MARKER_RDS_TAG_SOURCE_DB_INSTANCE_ID.format(os.getenv(handlers.ENV_STACK_NAME)), None)
            if source_db_instance_id_from_tag is not None:
                resource["DBInstanceIdentifier"] = source_db_instance_id_from_tag
            else:
                if task.get("parameters", {}).get(PARAM_RETENTION_COUNT, 0) > 0:
                    logger.info(INFO_NO_SOURCE_INSTANCE_ID_WITH_RETENTION, resource["SnapshotId"])
                    return None
        return resource

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.snapshots = sorted(self._resources_)
        self.retention_days = int(self.get(PARAM_RETENTION_DAYS))
        self.retention_count = int(self.get(PARAM_RETENTION_COUNT))

        self.dryrun = self.get(ACTION_PARAM_DRYRUN, False)

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_,
            "deleted-snapshots": []
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]

        retention_count = int(arguments["event"][ACTION_PARAMETERS].get(PARAM_RETENTION_COUNT, 0))
        if retention_count == 0:
            return "{}-{}-{}".format(account, region, log_stream_date())
        else:
            return "{}-{}-{}-{}".format(account, region, arguments[ACTION_PARAM_RESOURCES][0].get("DBSnapshotIdentifier", ""),
                                        log_stream_date())

    def execute(self):

        def get_creation_time(s):
            if isinstance(s["SnapshotCreateTime"], datetime):
                return s["SnapshotCreateTime"]
            return dateutil.parser.parse(s["SnapshotCreateTime"])

        def snapshots_to_delete():

            def by_retention_days():

                delete_before_dt = self._datetime_.utcnow().replace(tzinfo=pytz.timezone("UTC")) - timedelta(
                    days=int(self.retention_days))
                self._logger_.info(INFO_RETENTION_DAYS, delete_before_dt)

                for sn in sorted(self.snapshots, key=lambda s: s["Region"]):
                    snapshot_dt = get_creation_time(sn)
                    if snapshot_dt < delete_before_dt:
                        self._logger_.info(INFO_SN_RETENTION_DAYS, sn["DBSnapshotIdentifier"], get_creation_time(sn),
                                           self.retention_days)
                        yield sn

            def by_retention_count():

                self._logger_.info(INFO_KEEP_RETENTION_COUNT, self.retention_count)
                sorted_snapshots = sorted(self.snapshots,
                                          key=lambda s: (s["DBInstanceIdentifier"], get_creation_time(s)),
                                          reverse=True)
                db_instance = None
                count_for_instance = 0
                for sn in sorted_snapshots:
                    if sn["DBInstanceIdentifier"] != db_instance:
                        db_instance = sn["DBInstanceIdentifier"]
                        count_for_instance = 0

                    count_for_instance += 1
                    if count_for_instance > self.retention_count:
                        self._logger_.info(INFO_SN_DELETE_RETENTION_COUNT, sn["DBSnapshotIdentifier"], count_for_instance)
                        yield sn

            return by_retention_days() if self.retention_days else by_retention_count()

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        deleted_count = 0

        self._logger_.info(INFO_ACCOUNT_SNAPSHOTS, len(self.snapshots), self._account_)

        self._logger_.debug("Snapshots : {}", self.snapshots)

        snapshot_id = ""
        db_instance_id = ""
        for snapshot in list(snapshots_to_delete()):

            if self.time_out():
                break

            rds = get_client_with_retries("rds", ["delete_db_snapshot"],
                                          region=self._region_,
                                          context=self._context_,
                                          session=self._session_,
                                          logger=self._logger_)

            try:
                snapshot_id = snapshot["DBSnapshotIdentifier"]
                db_instance_id = snapshot["DBInstanceIdentifier"]
                rds.delete_db_snapshot_with_retries(DBSnapshotIdentifier=snapshot_id,
                                                    _expected_boto3_exceptions_=["DBSnapshotNotFoundFault"])

                deleted_count += 1
                self._logger_.info(INFO_SNAPSHOT_DELETED, snapshot_id, db_instance_id)
                self.result["deleted-snapshots"].append(snapshot_id)
            except Exception as ex:
                exception_name = type(ex).__name__
                if exception_name == "DBSnapshotNotFoundFault":
                    self._logger_.warning(str(ex))
                else:
                    raise_exception(ERR_DELETING_SNAPSHOT, snapshot_id, db_instance_id, ex)

        self.result.update({
            "snapshots": len(self.snapshots),
            METRICS_DATA: build_action_metrics(self, DeletedDBSnapshots=deleted_count)

        })

        return self.result
