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


import boto_retry
import handlers.rds_tag_event_handler
import services.rds_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_default_retry_strategy
from helpers import safe_json
from outputs import raise_exception
from tagging import tag_key_value_list
from tagging.tag_filter_set import TagFilterSet

TAG_PLACEHOLDER_DB_DOMAIN_MEMBERSHIPS = "db-domain-memberships"
TAG_PLACEHOLDER_DB_INSTANCE_CLASS = "db-instance-class"
TAG_PLACEHOLDER_DB_INSTANCE_ID = "db-instance-id"
TAG_PLACEHOLDER_DB_NAME = "db-name"
TAG_PLACEHOLDER_DB_SUBNET_GROUP = "db-subnet-group"
TAG_PLACEHOLDER_DB_SUBNETS = "db-subnets"
TAG_PLACEHOLDER_ENGINE = "db-engine"
TAG_PLACEHOLDER_ENGINE_VERSION = "db-engine-version"
TAG_PLACEHOLDER_OPTION_GROUPS = "db-option-groups"

INF_DELETING_INSTANCE_FOR_TASK = "Deleting RDS instance {} for task {}"
INF_FINAL_SNAPSHOT = "A final snapshot {} will be created for RDS instance {}"
INF_GRANTING_RESTORE_PERMISSION = "Granting restore permissions to accounts {}"
INF_INSTANCE_ALREADY_DELETED = "RDS instance {} already being deleted by task {}"
INF_INSTANCE_DELETED = "RDS instance is {} deleted"
INF_INSTANCE_AVAILABLE = "Instance {} is available for snapshot"
INF_INSTANCE_STATUS = "Status of instance {} is {}"
INF_NO_SNAPSHOT_YET = "No final snapshot {} yet"
INF_SETTING_FINAL_SNAPSHOT_TAGS = "Setting tags {} to final snapshot {}"
INF_START_CHECKING_STATUS_OF_INSTANCE = "Checking status of instance {} if it is available for creating a final snapshot"
INF_STARTING_STOPPED_INSTANCE = "RDS instance {} is not running, starting in order to create a final snapshot"
INF_TERMINATION_COMPLETED = "Termination of RDS instance {} completed"
INF_TERMINATION_COMPLETED_WITH_SNAPSHOT = "Termination of RDS instance {} completed, snapshot {} is created and available"
INF_WAITING_FOR_INSTANCE__AVAILABLE = "Waiting for RDS instance {} to become available for creating snapshot, current status is {}"
INF_CREATING_FINAL_SNAPSHOT_PROGRESS = "Creating final snapshot {} for instance {}, progress is {}%"
INF_STOP_STARTED_INSTANCE = "Instance {} could not be deleted, instance was started for making final snapshot and will be stopped."

ERR_FINAL_SNAPSHOT_FAILED = "Error creating final snapshot {} for instance {}"
ERR_INSTANCE_IS_STOPPED = "Cannot create a final snapshot because RDS instance {} is not available, status is {}"
ERR_SETTING_FINAL_SNAPSHOT_TAGS = "Error setting tags to final snapshot {}, {}"
ERR_SETTING_RESTORE_PERMISSION = "Error granting restore permissions for last snapshot to accounts {}, {}"
ERR_STARTED_DB_INSTANCE_NO_LONGER_EXISTS = "RDS instance {} does not longer exists"
ERR_STARTING_STOPPED_INSTANCE_FOR_SNAPSHOT = "Error starting stopped RDS instance for taking final snapshot"

INSTANCE_STATUS_AVAILABLE = "available"
INSTANCE_STATUS_STOPPED = "stopped"

SNAPSHOT_STATUS_CREATING = "creating"
SNAPSHOT_STATUS_AVAILABLE = "available"
SNAPSHOT_STATUS_FAILED = "failed"

SNAPSHOT_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"

PARAM_CREATE_SNAPSHOT = "CreateSnapshot"
PARAM_SNAPSHOT_NAME_PREFIX = "SnapshotNamePrefix"
PARAM_SNAPSHOT_NAME = "SnapshotName"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"
PARAM_COPIED_INSTANCE_TAGS = "CopiedInstanceTags"
PARAM_RESTORE_PERMISSION = "GrantRestorePermission"
PARAM_START_STOPPED_FOR_SNAPSHOT = "StartStopped"

PARAM_DESC_CREATE_SNAPSHOT = "Creates a final snapshot before deleting the RDS instance."
PARAM_DESC_SNAPSHOT_TAGS = \
    "Tags to add to the created final snapshot. Note that tag values for RDS cannot contain ',' characters. When specifying " \
    "multiple follow up tasks in the value of the Ops Automator task list tag use  a '/' character instead"
PARAM_DESC_SNAPSHOT_NAME_PREFIX = "Prefix for name final snapshot."
PARAM_DESC_SNAPSHOT_NAME = "Name of the final snapshot, leave blank for default snapshot name"
PARAM_DESC_RESTORE_PERMISSION = "Accounts authorized to copy or restore the RDS snapshot"
PARAM_DESC_COPIED_INSTANCE_TAGS = \
    "Use a tag filter to copy tags from the RDS instance to the final snapshot. For example, use * to copy all tags " \
    "from the RDS instance to the snapshot."
PARAM_DESC_START_STOPPED_FOR_SNAPSHOT = \
    "In order to create a final snapshot the database should be available. Start stopped instances if a final snapshot is required."

PARAM_LABEL_CREATE_SNAPSHOT = "Create final snapshot"
PARAM_LABEL_SNAPSHOT_NAME_PREFIX = "Final snapshot name prefix"
PARAM_LABEL_SNAPSHOT_NAME = "Final snapshot name"
PARAM_LABEL_SNAPSHOT_TAGS = "Final snapshot tags"
PARAM_LABEL_COPIED_INSTANCE_TAGS = "Copied RDS instance tags"
PARAM_LABEL_RESTORE_PERMISSION = "Accounts with restore permissions"
PARAM_LABEL_START_STOPPED_FOR_SNAPSHOT = "Allow starting stopped instances for final snapshot"

GROUP_TITLE_SNAPSHOT_OPTIONS = "Snapshot options"


class RdsDeleteInstanceAction(ActionBase):
    """
    Implements action to delete a RDS Instance with an optional final snapshot
    """
    properties = {
        ACTION_TITLE: "RDS Delete Instance",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Deletes RDS instance with optional snapshot",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "23a4e22f-15d4-4815-9abf-4730a7bbc663",

        ACTION_SERVICE: "rds",
        ACTION_RESOURCES: services.rds_service.DB_INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION:
            "DBInstances[].{DBInstanceIdentifier:DBInstanceIdentifier," +
            "DBInstanceStatus:DBInstanceStatus, " +
            "DBInstanceArn:DBInstanceArn, " +
            "DBName:DBName, " +
            "DBInstanceClass:DBInstanceClass, " +
            "Engine:Engine," +
            "EngineVersion:EngineVersion, " +
            "DBSubnetGroupName:DBSubnetGroup.DBSubnetGroupName, " +
            "DBSubnets:DBSubnetGroup.Subnets[].SubnetIdentifier, " +
            "DomainMemberships:DomainMemberships[].FQDN, " +
            "OptionGroupMemberships:OptionGroupMemberships[].OptionGroupName }" +
            "|[?contains(['stopped','available','creating','stopping','modifying','backing-up'],DBInstanceStatus)]",

        ACTION_SELECTION_REQUIRES_TAGS: True,

        ACTION_EVENTS: {
            handlers.rds_tag_event_handler.RDS_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.rds_tag_event_handler.RDS_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {
            PARAM_COPIED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_INSTANCE_TAGS
            },
            PARAM_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_TAGS
            },
            PARAM_SNAPSHOT_NAME_PREFIX: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_NAME_PREFIX,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_NAME_PREFIX
            },
            PARAM_SNAPSHOT_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_NAME,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_NAME
            },
            PARAM_CREATE_SNAPSHOT: {
                PARAM_DESCRIPTION: PARAM_DESC_CREATE_SNAPSHOT,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: True,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_CREATE_SNAPSHOT
            },
            PARAM_START_STOPPED_FOR_SNAPSHOT: {
                PARAM_DESCRIPTION: PARAM_DESC_START_STOPPED_FOR_SNAPSHOT,
                PARAM_TYPE: bool,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_START_STOPPED_FOR_SNAPSHOT
            },
            PARAM_RESTORE_PERMISSION: {
                PARAM_DESCRIPTION: PARAM_DESC_RESTORE_PERMISSION,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_RESTORE_PERMISSION
            }
        },
        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_SNAPSHOT_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_CREATE_SNAPSHOT,
                    PARAM_START_STOPPED_FOR_SNAPSHOT,
                    PARAM_COPIED_INSTANCE_TAGS,
                    PARAM_SNAPSHOT_TAGS,
                    PARAM_SNAPSHOT_NAME,
                    PARAM_SNAPSHOT_NAME_PREFIX,
                    PARAM_RESTORE_PERMISSION
                ]
            }
        ],

        ACTION_PERMISSIONS: [
            "rds:DeleteDBInstance",
            "rds:AddTagsToResource",
            "rds:DescribeDBInstances",
            "rds:DescribeDBSnapshots",
            "rds:ModifyDBSnapshotAttribute",
            "rds:RemoveTagsFromResource",
            "rds:StartDBInstance",
            "rds:ListTagsForResource",
            "rds:StopDBInstance",
            "tag:GetResources"]

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.db_instance = self._resources_

        self.db_instance_id = self.db_instance["DBInstanceIdentifier"]
        self.db_instance_arn = self.db_instance["DBInstanceArn"]
        self._rds_client = None

        self.create_snapshot = self.get(PARAM_CREATE_SNAPSHOT, True)
        self.start_stopped_instance = self.get(PARAM_START_STOPPED_FOR_SNAPSHOT, True)

        # tags from the RDS instance
        self.instance_tags = self.db_instance.get("Tags", {})
        # filter for tags copied from RDS  instance to image
        self.copied_instance_tagfilter = TagFilterSet(self.get(PARAM_COPIED_INSTANCE_TAGS, ""))

        self.accounts_with_restore_permissions = self.get(PARAM_RESTORE_PERMISSION, [])

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "db-instance": self.db_instance_id,
            "task": self._task_
        }

    @property
    def rds_client(self):

        if self._rds_client is None:
            methods = [
                "delete_db_instance",
                "describe_db_instances",
                "add_tags_to_resource",
                "describe_db_snapshots",
                "modify_db_snapshot_attribute",
                "remove_tags_from_resource",
                "start_db_instance",
                "stop_db_instance"
            ]

            self._rds_client = boto_retry.get_client_with_retries("rds",
                                                                  methods=methods,
                                                                  region=self._region_,
                                                                  session=self._session_,
                                                                  context=self._context_,
                                                                  logger=self._logger_)

        return self._rds_client

    @staticmethod
    def filter_resource(rds_inst):
        return rds_inst["DBInstanceStatus"] in [
            "stopped",
            "available",
            "creating",
            "stopping",
            "creating",
            "modifying",
            "backing-up"
        ]

    @staticmethod
    def action_logging_subject(arguments, _):
        db_instance = arguments[ACTION_PARAM_RESOURCES]
        db_instance_id = db_instance["DBInstanceIdentifier"]
        account = db_instance["AwsAccount"]
        region = db_instance["Region"]
        return "{}-{}-{}-{}".format(account, region, db_instance_id, log_stream_date())

    def is_completed(self, exec_result):

        def grant_restore_permissions(snap_id):

            if self.accounts_with_restore_permissions is not None and len(self.accounts_with_restore_permissions) > 0:

                args = {
                    "DBSnapshotIdentifier": snap_id,
                    "AttributeName": "restore",
                    "ValuesToAdd": [a.strip() for a in self.accounts_with_restore_permissions]
                }

                try:
                    self.rds_client.modify_db_snapshot_attribute_with_retries(**args)
                    self._logger_.info(INF_GRANTING_RESTORE_PERMISSION, ", ".join(self.accounts_with_restore_permissions))
                    self.result["restore-access-accounts"] = [a.strip() for a in self.accounts_with_restore_permissions]
                except Exception as grant_ex:
                    raise_exception(ERR_SETTING_RESTORE_PERMISSION, self.accounts_with_restore_permissions, grant_ex)

        def set_tags_to_final_snapshot(snapshot, instance_tags):

            # tags on the snapshot
            tags = snapshot.get("Tags", {})

            tags.update(self.copied_instance_tagfilter.pairs_matching_any_filter(instance_tags))

            tags.update(
                self.build_tags_from_template(parameter_name=PARAM_SNAPSHOT_TAGS,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_DB_INSTANCE_ID: self.db_instance_id,
                                                  TAG_PLACEHOLDER_DB_NAME:
                                                      self.db_instance.get("DBName", "")
                                                      if self.db_instance.get("DBName", "") is not None else "",
                                                  TAG_PLACEHOLDER_ENGINE: self.db_instance.get("Engine", ""),
                                                  TAG_PLACEHOLDER_ENGINE_VERSION: self.db_instance.get("EngineVersion", ""),
                                                  TAG_PLACEHOLDER_DB_INSTANCE_CLASS: self.db_instance.get("DBInstanceClass", ""),
                                                  TAG_PLACEHOLDER_DB_SUBNET_GROUP: self.db_instance.get("DBSubnetGroupName", ""),
                                                  TAG_PLACEHOLDER_DB_SUBNETS: self.db_instance.get("DBSubnets", []),
                                                  TAG_PLACEHOLDER_DB_DOMAIN_MEMBERSHIPS:
                                                      self.db_instance.get("DomainMemberships", []),
                                                  TAG_PLACEHOLDER_OPTION_GROUPS: self.db_instance.get("OptionGroupMemberships", [])
                                              },
                                              restricted_value_set=True))

            if len(tags) > 0:
                try:
                    self._logger_.info(INF_SETTING_FINAL_SNAPSHOT_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                       snapshot["DBSnapshotArn"])
                    tagging.set_rds_tags(rds_client=self.rds_client,
                                         resource_arns=[snapshot["DBSnapshotArn"]],
                                         tags=tags,
                                         logger=self._logger_)

                    self._logger_.flush()

                except Exception as tag_ex:
                    raise_exception(ERR_SETTING_FINAL_SNAPSHOT_TAGS, snapshot["DBSnapshotArn"], tag_ex)

        def get_final_snapshot(snap_id):

            return rds.get(services.rds_service.DB_SNAPSHOTS,
                           region=self.db_instance["Region"],
                           tags=False,
                           DBSnapshotIdentifier=snap_id,
                           _expected_boto3_exceptions_=["DBSnapshotNotFoundFault"])

        def get_deleted_instance():

            try:
                return rds.get(services.rds_service.DB_INSTANCES,
                               region=self._region_,
                               DBInstanceIdentifier=self.db_instance_id,
                               _expected_boto3_exceptions_=["DBInstanceNotFoundFault"])
            except Exception as rds_ex:
                if type(rds_ex).__name__ == "DBInstanceNotFoundFault":
                    return None
                else:
                    raise rds_ex

        rds = services.create_service("rds", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("rds", context=self._context_))

        # if a snapshot was requested
        if self.create_snapshot:

            # id of the final snapshot
            snapshot_id = exec_result["snapshot-id"]
            self.result["db-snapshot-id"] = snapshot_id

            final_snapshot = get_final_snapshot(snapshot_id)

            # no snapshot yet
            if final_snapshot is None:

                self._logger_.info(INF_NO_SNAPSHOT_YET, snapshot_id)

                # test if this is because the delete_instance call not made because the instance needed to be started first
                if not exec_result.get("available", False):

                    # get status of started instance
                    self._logger_.info(INF_START_CHECKING_STATUS_OF_INSTANCE, self.db_instance_id)

                    rds_instance = get_deleted_instance()
                    if rds_instance is None:
                        raise_exception(ERR_STARTED_DB_INSTANCE_NO_LONGER_EXISTS, self.db_instance_id)

                    instance_status = rds_instance["DBInstanceStatus"]

                    # if database was in a stopping state stopping state restart if allowed by start_stopped_instance parameter
                    if instance_status == INSTANCE_STATUS_STOPPED:
                        if not self.start_stopped_instance:
                            raise_exception(ERR_INSTANCE_IS_STOPPED, self.db_instance_id, instance_status)
                        self._start_db_instance()
                        # and wait for it to become available
                        return None

                    # instance not available yet
                    if instance_status != INSTANCE_STATUS_AVAILABLE:
                        self._logger_.info(INF_WAITING_FOR_INSTANCE__AVAILABLE, self.db_instance_id, instance_status)
                        return None

                    # started instance is available
                    self._logger_.info(INF_INSTANCE_AVAILABLE, self.db_instance_id)
                    try:
                        # delete it with the requested snapshot
                        self._delete_instance(exec_result["snapshot-id"])
                        return None
                    except Exception as ex:
                        # instance was started for making a final snapshot before deleting, if deletion fails then stop instance
                        self._logger_.info(INF_STOP_STARTED_INSTANCE, self.db_instance_id)
                        self.rds_client.stop_db_instance_with_retries(DBInstanceIdentifier=self.db_instance_id)
                        raise ex

                return None

            # check status of final snapshot
            snapshot_status = final_snapshot['Status']

            # failed
            if snapshot_status == SNAPSHOT_STATUS_FAILED:
                raise_exception(ERR_FINAL_SNAPSHOT_FAILED, snapshot_id, self.db_instance_id)

            # creating but not done yet
            if snapshot_status == SNAPSHOT_STATUS_CREATING:
                progress = final_snapshot.get("PercentProgress", 0)
                self._logger_.info(INF_CREATING_FINAL_SNAPSHOT_PROGRESS, snapshot_id, self.db_instance_id, progress)
                return None

            # snapshot is available
            if snapshot_status == SNAPSHOT_STATUS_AVAILABLE:
                try:
                    set_tags_to_final_snapshot(final_snapshot, exec_result.get("instance-tags", {}))
                    grant_restore_permissions(snapshot_id)
                except Exception as ex:
                    raise ex

        # check for status of instance
        rds_instance = get_deleted_instance()

        # instance is still there
        if rds_instance is not None:
            instance_status = rds_instance["DBInstanceStatus"]
            self._logger_.info(INF_INSTANCE_STATUS, self.db_instance_id, instance_status)
            return None

        # no longer there because it was deleted successfully
        self._logger_.info(INF_INSTANCE_DELETED, self.db_instance_id)

        return self.result

    def _delete_instance(self, snapshot_name=None):

        # the task that is used to delete the instance needs to be deleted from the task list for the instance
        # if this is not done then an instance restored from the final snapshot might be deleted because the
        # tag that holds the action list is also restored from the snapshot
        def remove_delete_task_from_tasks_tag():
            # get current instance tags
            current_instance_tags = self.db_instance.get("Tags", {})
            # get tasks
            task_list_tag = os.getenv(handlers.ENV_AUTOMATOR_TAG_NAME, "")
            # find task list tag
            if task_list_tag in current_instance_tags:
                task_list = current_instance_tags[task_list_tag]
                tasks = tagging.split_task_list(task_list)
                # remove task that is deleting this instance
                if self._task_ in tasks:
                    tasks = [t for t in tasks if t != self._task_]
                if len(tasks) > 0:
                    # other tags left, update the task list tag
                    self.rds_client.add_tags_to_resource_with_retries(
                        ResourceName=self.db_instance_arn,
                        Tags=tag_key_value_list({task_list_tag: ",".join(tasks)}))
                else:
                    # no other tasks, delete the tag
                    self.rds_client.remove_tags_from_resource_with_retries(ResourceName=self.db_instance_arn,
                                                                           TagKeys=[task_list_tag])

        # store original set of tags in case the deletion fails
        instance_tags = self.db_instance.get("Tags", {})

        try:
            remove_delete_task_from_tasks_tag()

            args = {
                "DBInstanceIdentifier": self.db_instance_id,
            }

            if self.create_snapshot:
                args["FinalDBSnapshotIdentifier"] = snapshot_name
                self._logger_.info(INF_FINAL_SNAPSHOT, snapshot_name, self.db_instance_id)
            else:
                args["SkipFinalSnapshot"] = True

            args["_expected_boto3_exceptions_"] = ["InvalidDBInstanceStateFault", "DBInstanceNotFoundFault"]

            self._logger_.debug("calling delete_db_instance with arguments {}", safe_json(args, indent=3))
            resp = self.rds_client.delete_db_instance_with_retries(**args)
            self._logger_.debug("delete_db_instance response is {}", safe_json(resp, indent=3))
            self._logger_.flush()

        except Exception as ex:
            exception_name = type(ex).__name__
            error = getattr(ex, "response", {}).get("Error", {})
            if exception_name == "InvalidDBInstanceStateFault":
                if error.get("Code", "") == "InvalidDBInstanceState" and "is already being deleted" in error.get("Message", ""):
                    self._logger_.info(str(ex))
            elif exception_name == "DBInstanceNotFoundFault":
                self._logger_.info(str(ex))
            else:
                self.rds_client.add_tags_to_resource_with_retries(ResourceName=self.db_instance_arn,
                                                                  Tags=tag_key_value_list(instance_tags))
                raise Exception("Error deleting RDS instance {}, {}", self.db_instance_id, ex)

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_DELETING_INSTANCE_FOR_TASK, self.db_instance_id, self._task_)

        if self.create_snapshot:

            final_snapshot_name = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_NAME,
                                                               tag_variables={
                                                                   TAG_PLACEHOLDER_DB_INSTANCE_ID: self.db_instance_id
                                                               })
            if final_snapshot_name == "":
                dt = self._datetime_.utcnow()
                final_snapshot_name = SNAPSHOT_NAME.format(self.db_instance_id, dt.year, dt.month, dt.day, dt.hour, dt.minute)

            prefix = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_NAME_PREFIX,
                                                  tag_variables={
                                                      TAG_PLACEHOLDER_DB_INSTANCE_ID: self.db_instance_id
                                                  })

            final_snapshot_name = prefix + final_snapshot_name

            self.result["snapshot-id"] = final_snapshot_name
            self.result["instance-tags"] = self.db_instance.get("Tags", {})

            status = self.db_instance["DBInstanceStatus"]
            stopped = status == INSTANCE_STATUS_STOPPED
            available = status == INSTANCE_STATUS_AVAILABLE
            self.result["stopped-instance"] = stopped
            self.result["available"] = available

            # instance is stopped, if allowed start instance for final snapshot, otherwise raise error
            if stopped:
                if not self.start_stopped_instance:
                    raise_exception(ERR_INSTANCE_IS_STOPPED, self.db_instance_id, status)
                else:
                    self._start_db_instance()
            elif available:
                # instance was already running, delete with snapshot
                self._delete_instance(snapshot_name=final_snapshot_name)

        else:  # no snapshot requested, delete instance
            self._delete_instance()

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            DeletedDBInstances=1
        )

        return self.result

    def _start_db_instance(self):
        try:
            # start stopped instance and let completion handler delete the instance with a final snapshot
            self._logger_.info(INF_STARTING_STOPPED_INSTANCE, self.db_instance_id)
            self.rds_client.start_db_instance_with_retries(DBInstanceIdentifier=self.db_instance_id)

        except Exception as ex:
            raise_exception(ERR_STARTING_STOPPED_INSTANCE_FOR_SNAPSHOT, self.db_instance_id, ex)
