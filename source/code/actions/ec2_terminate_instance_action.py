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


import json
from os import getenv

import dateutil.parser

import handlers.ec2_tag_event_handler
import services.ec2_service
import tagging
from actions import *
from actions.action_base import ActionBase, date_time_provider
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from outputs import raise_exception
from tagging import tag_key_value_list
from tagging.tag_filter_set import TagFilterSet

EC2_STATE_TERMINATED = 48
EC2_STATE_SHUTTING_DOWN = 32
EC2_STATE_STOPPING = 64

TAG_PLACEHOLDER_IMAGE_ID = "image-id"
TAG_PLACEHOLDER_IMAGE_NAME = "image-name"
TAG_PLACEHOLDER_INSTANCE = "instance-id"
TAG_PLACEHOLDER_INSTANCE_AMI = "instance-ami"
TAG_PLACEHOLDER_INSTANCE_TYPE = "instance-type"
TAG_PLACEHOLDER_PRIVATE_IP = "instance-ip-private"
TAG_PLACEHOLDER_PUBLIC_IP = "instance-ip-public"
TAG_PLACEHOLDER_SUBNETS = "instance-subnets"

IMAGE_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"
IMAGE_DESCRIPTION = "Image created by task {} for terminated instance {}"

PARAM_ACCOUNTS_LAUNCH_ACCESS = "AccountsLaunchAccess"
PARAM_AMI_NAME_PREFIX = "ImageNamePrefix"
PARAM_AMI_TAGS = "ImageTags"
PARAM_COPIED_INSTANCE_TAGS = "CopiedInstanceTags"
PARAM_INSTANCE_TAGS = "InstanceTags"
PARAM_CREATE_IMAGE = "CreateImage"
PARAM_NAME = "ImageName"
PARAM_IMAGE_DESCRIPTION = "ImageDescription"

PARAM_DESC_AMI_NAME_PREFIX = "Prefix for image name."
PARAM_DESC_IMAGE_DESCRIPTION = "Description for image,leave blank for default description."
PARAM_DESC_NAME = "Name of the created image, leave blank for default image name"
PARAM_DESC_CREATE_IMAGE = "Create image before terminating the instance."
PARAM_DESC_INSTANCE_TAGS = "Tags to set on source EC2 instance after the images has been terminated successfully."
PARAM_DESC_AMI_TAGS = "Tags that will be added to created image. Use a list of tagname=tagvalue pairs."
PARAM_DESC_ACCOUNTS_LAUNCH_ACCESS = "List of valid AWS account ID that will be granted launch " \
                                    "permissions for the image."
PARAM_DESC_COPIED_INSTANCE_TAGS = "Enter a tag filter to copy tags from the instance to the AMI.\
                                   For example, use * to copy all tags from the instance to the image."

PARAM_LABEL_AMI_NAME_PREFIX = "Image name prefix"
PARAM_LABEL_AMI_TAGS = "Image tags"
PARAM_LABEL_COPIED_INSTANCE_TAGS = "Copied instance tags"
PARAM_LABEL_INSTANCE_TAGS = "Instance tags"
PARAM_LABEL_ACCOUNTS_LAUNCH_ACCESS = "Accounts with launch access"
PARAM_LABEL_CREATE_IMAGE = "Create image"
PARAM_LABEL_IMAGE_DESCRIPTION = "Image description"
PARAM_LABEL_NAME = "Image name"

GROUP_TITLE_INSTANCE_OPTIONS = "Instance options"
GROUP_TITLE_IMAGE_OPTIONS = "Image options"

INF_SET_INSTANCE_TAGS = "Set tags {} to terminated instance {}"
INF_SETTING_LAUNCH_PERMISSIONS = "Launch access granted to accounts"
INF_CREATE_IMAGE = "Creation of image {} for terminated instance {} started"
INF_SETTING_IMAGE_TAGS = "Set tags {} to image {}"
INF_START_IMAGE_TERMINATE_ACTION = "Terminating EC2 instance for task {}"
INF_IMAGE_NOT_CREATED_YET = "Image {} does has not been created yet"
INF_IMAGE_STATUS = "Image status is {}"
INF_NOT_COMPLETED = "Image creation for terminated instance not completed yet"
INF_ALREADY_TERMINATED = "Ec2 instance {} already terminated by Ops Automator stack {}, task {}, task id {}"
INF_TERMINATE_TIMEOUT = "Ec2 instance {} already terminated by Ops Automator stack {}, task {}, task id {}, but timed out"
INF_IMAGE_CREATION_COMPLETED = "Creation of image {} for terminated instance {} is completed"
INF_TERMINATION_COMPLETED = "Termination of instance {} completed"
INF_TERMINATION_COMPLETED_WITH_IMAGE = "Image {} for terminated instance {} is created and available"
INF_CREATING_LAST_IMAGE = "Creating image for terminated instance, instance will be terminated if images " \
                          "has been created successfully"
INF_WAIT_FOR_TERMINATE_STATE = "Waiting for instance to terminate, current state is {}"

ERR_SETTING_INSTANCE_TAGS = "Error setting tags to terminated instance {}, {}"
ERR_SETTING_LAUNCH_PERMISSIONS = "Error setting launch permissions for accounts {}, {}"
ERR_CREATING_IMAGE_START = "Error creating image for instance {}, image will not be terminated, {}"
ERR_SETTING_IMAGE_TAGS = "Error setting tags to image {}, {}"
ERR_CREATING_IMAGE = "Creation of image for terminated instance {} failed, reason is {}"
ERR_INVALID_DATE_IN_TERMINATION_MARKER = "{} in tag {} not a valid date {}"
ERR_SET_TAGS = "Can not set tags to stopped instance {}, {}"

INSTANCE_TERMINATION_MARKER_TAG = "ops-automator:Ec2TerminateInstance"
IMAGE_INSTANCE_TAG = "Automator:SourceInstanceId_{}"


class Ec2TerminateInstanceAction(ActionBase):
    """
    Implements action to create image for an EC2 instance
    """
    properties = {
        ACTION_TITLE: "EC2 Terminate Instance",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Terminates EC2 instance with optional snapshot",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "d8d7f3ae-1b5b-49ad-8135-4f35d8323ff5",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_SELECT_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION:
            "Reservations[*].Instances[].{State:State.Name,InstanceId:InstanceId, Tags:Tags, ImageId: ImageId, " +
            "PublicIpAddress:PublicIpAddress, PrivateIpAddress:PrivateIpAddress, InstanceType:InstanceType, " +
            "Subnets:NetworkInterfaces[].SubnetId  }" +
            "|[?contains(['stopped','running'],State)]",

        ACTION_EVENTS: {
            handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {
            PARAM_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_NAME,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_NAME
            },
            PARAM_IMAGE_DESCRIPTION: {
                PARAM_DESCRIPTION: PARAM_DESC_IMAGE_DESCRIPTION,
                PARAM_LABEL: PARAM_LABEL_IMAGE_DESCRIPTION,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_COPIED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_INSTANCE_TAGS
            },
            PARAM_AMI_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_AMI_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_AMI_TAGS
            },
            PARAM_AMI_NAME_PREFIX: {
                PARAM_DESCRIPTION: PARAM_DESC_AMI_NAME_PREFIX,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_AMI_NAME_PREFIX
            },
            PARAM_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_INSTANCE_TAGS
            },
            PARAM_CREATE_IMAGE: {
                PARAM_DESCRIPTION: PARAM_DESC_CREATE_IMAGE,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: True,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_CREATE_IMAGE
            },
            PARAM_ACCOUNTS_LAUNCH_ACCESS: {
                PARAM_DESCRIPTION: PARAM_DESC_ACCOUNTS_LAUNCH_ACCESS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_ACCOUNTS_LAUNCH_ACCESS
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_INSTANCE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_INSTANCE_TAGS
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_IMAGE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_CREATE_IMAGE,
                    PARAM_NAME,
                    PARAM_AMI_NAME_PREFIX,
                    PARAM_IMAGE_DESCRIPTION,
                    PARAM_COPIED_INSTANCE_TAGS,
                    PARAM_AMI_TAGS,
                    PARAM_ACCOUNTS_LAUNCH_ACCESS
                ]
            }
        ],

        ACTION_PERMISSIONS: [
            "ec2:CreateImage",
            "ec2:DescribeTags",
            "ec2:DescribeInstances",
            "ec2:CreateTags",
            "ec2:DeleteTags",
            "ec2:DescribeImages",
            "ec2:TerminateInstances",
            "ec2:ModifyImageAttribute"
        ],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.instance = self._resources_

        self.instance_id = self.instance["InstanceId"]
        self._ec2_client = None

        self.create_image = self.get(PARAM_CREATE_IMAGE, True)

        # tags from the Ec2 instance
        self.instance_tags = self.instance.get("Tags", {})
        # filter for tags copied from ec2 instance to image
        self.copied_instance_tagfilter = TagFilterSet(self.get(PARAM_COPIED_INSTANCE_TAGS, ""))

        self.accounts_with_launch_access = self.get(PARAM_ACCOUNTS_LAUNCH_ACCESS, [])

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "instance": self.instance_id,
            "task": self._task_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        instance = arguments[ACTION_PARAM_RESOURCES]
        instance_id = instance["InstanceId"]
        account = instance["AwsAccount"]
        region = instance["Region"]
        return "{}-{}-{}-{}".format(account, region, instance_id, log_stream_date())

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = [
                "create_image",
                "describe_tags",
                "describe_instances",
                "create_tags",
                "delete_tags",
                "describe_images",
                "terminate_instances",
                "modify_image_attribute"
            ]

            self._ec2_client = get_client_with_retries("ec2",
                                                       methods,
                                                       region=self.instance["Region"],
                                                       session=self._session_,
                                                       logger=self._logger_)

        return self._ec2_client

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):

        tags = resource.get("Tags", {})
        if INSTANCE_TERMINATION_MARKER_TAG in tags:
            terminate_data = json.loads(tags[INSTANCE_TERMINATION_MARKER_TAG])
            date_str = ""
            try:
                date_str = terminate_data.get("datetime", date_time_provider().utcnow().isoformat())
                dt = dateutil.parser.parse(date_str)
            except Exception as ex:
                logger.error(ERR_INVALID_DATE_IN_TERMINATION_MARKER, date_str, INSTANCE_TERMINATION_MARKER_TAG, ex)
                dt = date_time_provider().utcnow()

            if (int((date_time_provider().utcnow() - dt).total_seconds())) < \
                    (task.get(ACTION_PARAM_TIMEOUT, Ec2TerminateInstanceAction.properties[ACTION_COMPLETION_TIMEOUT_MINUTES]) * 60):
                logger.info(INF_ALREADY_TERMINATED, resource["InstanceId"], terminate_data["stack"], terminate_data["task"],
                            terminate_data["task-id"])
                return None
            else:
                logger.info(INF_TERMINATE_TIMEOUT, resource["InstanceId"], terminate_data["stack"], terminate_data["task"],
                            terminate_data["task-id"])
                return resource

        return resource

    def _create_terminated_instance_tags(self, image_id, image_name):

        tags = self.build_tags_from_template(parameter_name=PARAM_INSTANCE_TAGS,
                                             tag_variables={
                                                 TAG_PLACEHOLDER_IMAGE_ID: image_id,
                                                 TAG_PLACEHOLDER_IMAGE_NAME: image_name

                                             })
        if len(tags) > 0:
            try:
                tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                     resource_ids=[self.instance_id],
                                     tags=tags,
                                     logger=self._logger_)

                self._logger_.info(INF_SET_INSTANCE_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                   self.instance_id)
            except Exception as ex:
                raise_exception(ERR_SETTING_INSTANCE_TAGS, self.instance_id, ex)

    def _grant_launch_access(self, image_id):

        if self.accounts_with_launch_access is not None and len(self.accounts_with_launch_access) > 0:
            args = {
                "ImageId": image_id,
                "LaunchPermission": {
                    "Add": [
                        {
                            "UserId": a.strip()
                        } for a in self.accounts_with_launch_access]
                }
            }

            try:
                self.ec2_client.modify_image_attribute_with_retries(**args)
                self._logger_.info("Launch access granted to accounts", ", ".join(self.accounts_with_launch_access))
                self.result["launch-access-accounts"] = [a.strip() for a in self.accounts_with_launch_access]
            except Exception as ex:
                raise_exception(ERR_SETTING_LAUNCH_PERMISSIONS, self.accounts_with_launch_access, ex)

    def _delete_instance_marker_tag(self):
        self.ec2_client.delete_tags_with_retries(Resources=[self.instance_id],
                                                 Tags=tag_key_value_list({INSTANCE_TERMINATION_MARKER_TAG: ""}))

    def is_completed(self, execute_results):

        ec2 = services.create_service("ec2", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

        instance = ec2.get(services.ec2_service.INSTANCES,
                           region=self._region_,
                           InstanceIds=[self.instance_id])
        state = instance.get("State", {}).get("Code", 0) & 0xFF
        if state == EC2_STATE_TERMINATED:
            self._create_terminated_instance_tags(image_id=execute_results.get("image-id", ""),
                                                  image_name=execute_results.get("image-name", ""))

            self._logger_.info(INF_TERMINATION_COMPLETED, self.instance_id)
            return self.result
        if state in [EC2_STATE_SHUTTING_DOWN, EC2_STATE_STOPPING]:
            self._logger_.info(INF_WAIT_FOR_TERMINATE_STATE, instance.get("State", {}).get("Name"))
            return None

        if self.create_image:

            image_id = execute_results["image-id"]

            image = ec2.get(services.ec2_service.IMAGES,
                            Owners=["self"],
                            region=self._region_,
                            ImageIds=[image_id], tags=True)

            if image is None:
                self._logger_.info(INF_IMAGE_NOT_CREATED_YET, image_id)
                return None

            self._logger_.debug("Image data is {}", safe_json(image, indent=3))

            # get and test image state
            status = image["State"]
            self._logger_.info(INF_IMAGE_STATUS, status)

            if status == "pending":
                self._logger_.info(INF_NOT_COMPLETED)
                return None

            # abort if creation is not successful
            if status in ["failed", "error", "invalid"]:
                self._delete_instance_marker_tag()
                raise Exception(
                    ERR_CREATING_IMAGE.format(self.instance_id, image.get("StateReason", {}).get("Message", "")))

            if status == "available":
                self._logger_.info(INF_IMAGE_CREATION_COMPLETED, image_id, self.instance_id)
                # grant launch access to accounts
                self._grant_launch_access(image_id)

                self._terminate_instance()

                self._logger_.info(INF_TERMINATION_COMPLETED_WITH_IMAGE, image_id, self.instance_id)

        return None

    def _create_image(self):

        image_name = self.build_str_from_template(parameter_name=PARAM_NAME,
                                                  tag_variables=self._placeholder_variables())
        if image_name == "":
            dt = self._datetime_.utcnow()
            image_name = IMAGE_NAME.format(self.instance_id, dt.year, dt.month, dt.day, dt.hour, dt.minute)

        prefix = self.build_str_from_template(parameter_name=PARAM_AMI_NAME_PREFIX,
                                              tag_variables=self._placeholder_variables())

        image_name = prefix + image_name

        description = self.build_str_from_template(parameter_name=PARAM_DESCRIPTION,
                                                   tag_variables=self._placeholder_variables())
        if description == "":
            description = IMAGE_DESCRIPTION.format(self._task_, self.instance_id)

        args = {
            "InstanceId": self.instance_id,
            "NoReboot": False,
            "Name": image_name,
            "Description": description
        }

        try:
            self._logger_.debug("create_image arguments {}", args)
            resp = self.ec2_client.create_image_with_retries(**args)
            image_id = resp.get("ImageId", None)
            self._logger_.info(INF_CREATE_IMAGE, image_id, self.instance_id)
            self._create_image_tags(image_id)

            return image_id, image_name
        except Exception as ex:
            raise_exception(ERR_CREATING_IMAGE_START, self.instance_id, ex)

    def _create_image_tags(self, image_id):

        tags = self.copied_instance_tagfilter.pairs_matching_any_filter(self.instance_tags)

        tags.update(
            self.build_tags_from_template(parameter_name=PARAM_AMI_TAGS,
                                          tag_variables=self._placeholder_variables()))
        tags[IMAGE_INSTANCE_TAG.format(getenv(handlers.ENV_STACK_NAME, "").lower())] = self.instance_id
        if len(tags) > 0:
            try:
                tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                     resource_ids=[image_id],
                                     tags=tags,
                                     can_delete=False,
                                     logger=self._logger_)

                self._logger_.info(INF_SETTING_IMAGE_TAGS, ", ".join(["{}={}".format(t, tags[t]) for t in tags]),
                                   image_id)
            except Exception as ex:
                raise_exception(ERR_SETTING_IMAGE_TAGS, image_id, ex)

    def _placeholder_variables(self):
        variables = {
            TAG_PLACEHOLDER_INSTANCE: self.instance_id,
            TAG_PLACEHOLDER_INSTANCE_AMI: self.instance.get("ImageId", ""),
            TAG_PLACEHOLDER_INSTANCE_TYPE: self.instance.get("InstanceType", ""),
            TAG_PLACEHOLDER_PRIVATE_IP: self.instance.get("PrivateIpAddress", ""),
            TAG_PLACEHOLDER_PUBLIC_IP: self.instance.get("PublicIpAddress", ""),
            TAG_PLACEHOLDER_SUBNETS: self.instance.get("Subnets", [])
        }
        for t in variables:
            if variables[t] is None:
                variables[t] = ""
        return variables

    def _terminate_instance(self):
        self._logger_.info("Terminating instance {}", self.instance_id, )
        try:
            self.ec2_client.terminate_instances_with_retries(InstanceIds=[self.instance_id])
        except Exception as ex:
            self._delete_instance_marker_tag()
            raise Exception("Error terminating instance {}, {}", self.instance_id, ex)

    def execute(self):

        def set_in_termination_tag():
            self.ec2_client.create_tags_with_retries(Resources=[self.instance_id],
                                                     Tags=tag_key_value_list({
                                                         INSTANCE_TERMINATION_MARKER_TAG: safe_json({
                                                             "stack": os.getenv(handlers.ENV_STACK_NAME, "", ),
                                                             "task": self._task_,
                                                             "task-id": self._task_id_,
                                                             "datetime": self._datetime_.utcnow().isoformat()
                                                         })
                                                     }))

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_START_IMAGE_TERMINATE_ACTION, self.instance_id, self._task_)

        set_in_termination_tag()

        if self.create_image:
            self._logger_.info(INF_CREATING_LAST_IMAGE)
            image_id, image_name = self._create_image()
            self.result["image-id"] = image_id
            self.result["image-name"] = image_name
        else:
            self._terminate_instance()

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            TerminatedInstances=1
        )

        return self.result
