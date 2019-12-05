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
# Brought forward from AWS Ops Automator version 2.2.0.61, November 2018, build for Novartis

from botocore.exceptions import ClientError

import handlers.ec2_tag_event_handler
import services.ec2_service
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy

INF_NO_LONGER_AVAILABLE = "Snapshot {} is not longer available"

INF_DELETED_SNAPSHOT = "Deleted snapshot {}"

INF_SNAPSHOT_NOT_FOUND = "Snapshot \"{}\" was not found and could not be deleted"

GROUP_TITLE_REMOVE_OPTIONS = "Snapshot remove options"

INFO_ACCOUNT_SNAPSHOTS = "Processing set of {} snapshots for account {} in region {}"
INFO_REGION = "Deleting snapshots in region {}"


class Ec2RemoveSnapshotAction(ActionBase):
    properties = {
        ACTION_TITLE: "EC2 Remove Snapshot",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Deletes EC2 snapshots",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "a3ff1b34-19a9-4760-b6b3-212abef245b6",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.SNAPSHOTS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION: "Snapshots[?State=='completed'].{SnapshotId:SnapshotId,Tags:Tags}",
        ACTION_KEEP_RESOURCE_TAGS: True,

        ACTION_SELECT_PARAMETERS: {'OwnerIds': ["self"]},

        ACTION_EVENTS: {
            handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.ec2_tag_event_handler.EC2_CHANGED_SNAPSHOT_TAGS_EVENT]
            }
        },

        ACTION_SELECT_SIZE: [ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_MEDIUM,
                              ACTION_SIZE_LARGE,
                              ACTION_SIZE_XLARGE,
                              ACTION_SIZE_XXLARGE,
                              ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_COMPLETION_SIZE: [ACTION_SIZE_MEDIUM],

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_PARAMETERS: {},

        ACTION_PARAMETER_GROUPS: [],

        ACTION_PERMISSIONS: [
            "ec2:DescribeSnapshots",
            "ec2:DeleteSnapshot"
        ]

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.snapshots = sorted(self._resources_)

        self.dryrun = self.get(ACTION_PARAM_DRYRUN, False)

        self._ec2_client = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_
        }

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            self._ec2_client = get_client_with_retries("ec2",
                                                       methods=[
                                                           "delete_snapshot"
                                                       ],
                                                       region=self._region_,
                                                       context=self._context_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._ec2_client

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        snapshots = list(set([s["SnapshotId"] for s in arguments.get(ACTION_PARAM_RESOURCES, [])]))
        if len(snapshots) == 1:
            return "{}-{}-{}-{}".format(account, region, snapshots[0], log_stream_date())
        else:
            return "{}-{}-{}".format(account, region, log_stream_date())

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        deleted_count = 0

        self._logger_.debug("Snapshots : {}", self.snapshots)

        snapshot_id = ""

        ec2 = services.create_service("ec2", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

        for snapshot in self.snapshots:

            if self.time_out():
                break

            if "deleted" not in self.result:
                self.result["deleted"] = []

            try:
                snapshot_id = snapshot["SnapshotId"]
                snapshot = ec2.get(services.ec2_service.SNAPSHOTS,
                                   region=self._region_,
                                   OwnerIds=["self"],
                                   Filters=[{"Name": "snapshot-id", "Values": [snapshot_id]}])
                if snapshot is None:
                    self._logger_.info(INF_NO_LONGER_AVAILABLE, snapshot_id)
                else:
                    self.ec2_client.delete_snapshot_with_retries(DryRun=self.dryrun, SnapshotId=snapshot_id)
                    deleted_count += 1
                    self._logger_.info(INF_DELETED_SNAPSHOT, snapshot_id)
                    self.result["deleted"].append(snapshot_id)
            except ClientError as ex_client:
                if ex_client.response.get("Error", {}).get("Code", "") == "InvalidSnapshot.NotFound":
                    self._logger_.info(INF_SNAPSHOT_NOT_FOUND, snapshot_id)
                else:
                    raise ex_client
            except Exception as ex:
                if self.dryrun:
                    self._logger_.debug(str(ex))
                    self.result["delete_snapshot"] = str(ex)
                    return self.result
                else:
                    raise ex

        self.result.update({
            "snapshots": len(self.snapshots),
            "snapshots-deleted": deleted_count,
            METRICS_DATA: build_action_metrics(self, DeletedSnapshots=deleted_count)

        })

        return self.result
