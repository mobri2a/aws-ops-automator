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
from botocore.exceptions import ClientError

import pytz
import services.ec2_service
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries
from outputs import raise_value_error, raise_exception

INF_IMAGE_SNAPSHOT_DELETED = "DELETED snapshot {} for image {}"

GROUP_TITLE_DELETE_OPTIONS = "Image delete options"

PARAM_DESC_RETENTION_COUNT = "Number of images to keep for an instance, use 0 to use retention days"
PARAM_DESC_RETENTION_DAYS = "Image retention period in days, use 0 to use retention count"

PARAM_LABEL_RETENTION_COUNT = "Retention count"
PARAM_LABEL_RETENTION_DAYS = "Retention days"

INFO_ACCOUNT_IMAGES = "{} snapshots for account {}"
INFO_KEEP_RETENTION_COUNT = "Retaining latest {} images for the Ec2 instance"
INFO_REGION = "Processing images in region {}"
INFO_RETENTION_DAYS = "Deleting Ec2 images older than {}"
INFO_SN_DELETE_RETENTION_COUNT = "Deleting image {}, because count for its instance is {}"
INFO_SN_RETENTION_DAYS = "Deleting image {} ({}) because it is older than retention period of {} days"
INFO_IMAGE_DEREGISTER = "De-registering image {} for instance {}"
INFO_NO_SOURCE_INSTANCE_WITH_RETENTION = "Original instance can not be retrieved for image {}, original volume is required for " \
                                         "use with Retention count parameter not equal to 0, image skipped"

ERR_RETENTION_PARAM_BOTH = "Only one of {} or {} parameters can be specified"
ERR_RETENTION_PARAM_NONE = "{} or {} parameter must be specified"
ERR_DELETING_IMAGE_SNAPSHOT = "Error deleting snapshots for image {}, {}"

WARN_NO_INSTANCE_TAG = "InstanceId for image {} cannot be retrieved from tag {}, deletion by retention count is not possible"

PARAM_RETENTION_DAYS = "RetentionDays"
PARAM_RETENTION_COUNT = "RetentionCount"


class Ec2DeleteImageAction(ActionBase):
    properties = {
        ACTION_TITLE: "EC2 Delete Image",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Deletes Amazon Machine Images (AMI) after retention period or count",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "aac9da60-d325-4ed5-ae30-2e11fe7a7e39",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.IMAGES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION: "Images[?State=='available'].{ImageId:ImageId,CreationDate:CreationDate,"
                                  "Tags:Tags, Snapshots:BlockDeviceMappings[].Ebs.SnapshotId}",

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_MIN_INTERVAL_MIN: 15,

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

        ACTION_KEEP_RESOURCE_TAGS: True,

        ACTION_SELECT_PARAMETERS: {'Owners': ["self"]},

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

        ACTION_PERMISSIONS: ["ec2:DescribeSnapshots",
                             "ec2:DeregisterImage",
                             "ec2:DeleteSnapshot"]

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.images = sorted(self._resources_)
        self.retention_days = int(self.get(PARAM_RETENTION_DAYS))
        self.retention_count = int(self.get(PARAM_RETENTION_COUNT))

        self._ec2_client = None
        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        retention_count = int(arguments["event"][ACTION_PARAMETERS].get(PARAM_RETENTION_COUNT, 0))
        if retention_count == 0:
            return "{}-{}-{}".format(account, region, log_stream_date())
        else:
            return "{}-{}-{}-{}".format(account,
                                        region, arguments[ACTION_PARAM_RESOURCES][0].get("ImageId", ""),
                                        log_stream_date())

    @staticmethod
    def _image_instance(img):
        return img.get("Tags", {}).get(marker_image_source_instance_tag(), "")

    # noinspection PyUnusedLocal
    @staticmethod
    def custom_aggregation(resources, parameters, logger):

        # sorts images by instance id  from tag
        images_sorted_by_instance_id = sorted(resources, key=lambda k: Ec2DeleteImageAction._image_instance(k))
        instance = Ec2DeleteImageAction._image_instance(images_sorted_by_instance_id[0]) if len(
            images_sorted_by_instance_id) > 0 else None
        images_for_instance = []
        for image in images_sorted_by_instance_id:
            if instance != Ec2DeleteImageAction._image_instance(image):
                yield images_for_instance
                instance = Ec2DeleteImageAction._image_instance(image)
                images_for_instance = [image]
            else:
                images_for_instance.append(image)
        yield images_for_instance

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):
        instance_from_tag = Ec2DeleteImageAction._image_instance(resource)
        if instance_from_tag not in [None, ""]:
            resource["SourceInstanceId"] = instance_from_tag
        else:
            if task.get("parameters", {}).get(PARAM_RETENTION_COUNT, 0) > 0:
                logger.info(INFO_NO_SOURCE_INSTANCE_WITH_RETENTION, resource["ImageId"])
                return None
        return resource

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

    def ec2_client(self, region):
        if (self._ec2_client is None) or (self._ec2_client.meta.region_name != region):
            self._ec2_client = get_client_with_retries("ec2",
                                                       [
                                                           "delete_snapshot",
                                                           "deregister_image"
                                                       ],
                                                       region=region,
                                                       context=self._context_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._ec2_client

    def execute(self):

        def get_creation_time(s):
            if isinstance(s["CreationDate"], datetime):
                return s["StartTime"]
            return dateutil.parser.parse(s["CreationDate"])

        def images_to_delete():

            def by_retention_days():

                delete_before_dt = self._datetime_.utcnow().replace(tzinfo=pytz.timezone("UTC")) - timedelta(
                    days=int(self.retention_days))
                self._logger_.info(INFO_RETENTION_DAYS, delete_before_dt)

                for img in sorted(self.images, key=lambda s: s["Region"]):
                    snapshot_dt = get_creation_time(img)
                    if snapshot_dt < delete_before_dt:
                        self._logger_.info(INFO_SN_RETENTION_DAYS, img["ImageId"], get_creation_time(img), self.retention_days)
                        yield img

            def by_retention_count():

                self._logger_.info(INFO_KEEP_RETENTION_COUNT, self.retention_count)
                sorted_snapshots = sorted(self.images,
                                          key=lambda s: (s["SourceInstanceId"], get_creation_time(s)),
                                          reverse=True)
                instance = None
                count_for_instance = 0
                for img in sorted_snapshots:
                    img_instance = Ec2DeleteImageAction._image_instance(img)

                    if img_instance is None:
                        self._logger_.warning(WARN_NO_INSTANCE_TAG, img["SourceInstanceId"], marker_image_source_instance_tag())
                        continue

                    if img_instance != instance:
                        instance = img_instance
                        count_for_instance = 0

                    count_for_instance += 1
                    if count_for_instance > self.retention_count:
                        self._logger_.info(INFO_SN_DELETE_RETENTION_COUNT, img["SourceInstanceId"], count_for_instance)
                        yield img

            return by_retention_days() if self.retention_days else by_retention_count()

        def deregister_image(img):
            img_id = img["ImageId"]
            try:
                self.ec2_client(img["Region"]).deregister_image_with_retries(ImageId=img_id)
                self._logger_.info(INFO_IMAGE_DEREGISTER, img_id, Ec2DeleteImageAction._image_instance(img))
                return True
            except ClientError as ex_client:
                if ex_client.response.get("Error", {}).get("Code", "") in ["InvalidAMIID.Unavailable", "InvalidAMIID.NotFound"]:
                    self._logger_.info("Image \"{}\" was not found or available and could not be deregistered", img_id)
                    return False
                else:
                    raise ex_client
            except Exception as e:
                raise Exception("Unable to deregister image {}, {}", img_id, e)

        def delete_image_snapshots(img):
            failed = []
            client = self.ec2_client(img["Region"])
            for snapshot_id in image.get("Snapshots", []):
                try:
                    client.delete_snapshot_with_retries(SnapshotId=snapshot_id)
                    self._logger_.info(INF_IMAGE_SNAPSHOT_DELETED, snapshot_id, img["ImageId"])
                except ClientError as ex_client:
                    if ex_client.response.get("Error", {}).get("Code", "") == "InvalidSnapshot.NotFound":
                        self._logger_.info("Snapshot \"{}\" was not found and could not be deleted", snapshot_id)
                        continue
                except Exception as ex:
                    failed.append((snapshot_id, str(ex)))

            if len(failed) > 0:
                raise_exception(ERR_DELETING_IMAGE_SNAPSHOT, img["ImageId"],
                                ", ".join(["{}:{}".format(f[0], f[1]) for f in failed]))

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        region = None
        deleted_count = 0

        self._logger_.info(INFO_ACCOUNT_IMAGES, len(self.images), self._account_)

        self._logger_.debug("Images : {}", self.images)

        for image in list(images_to_delete()):

            if image["Region"] != region:

                region = image["Region"]
                self._logger_.info(INFO_REGION, region)
                if "deleted" not in self.result:
                    self.result["deleted"] = {}
                self.result["deleted"][region] = []

            if deregister_image(image):
                self.result["deleted"][region].append(image)
                delete_image_snapshots(image)

            deleted_count += 1

        self.result.update({
            "images": len(self.images),
            "images-deleted": deleted_count,
            METRICS_DATA: build_action_metrics(self, DeletedImages=deleted_count)

        })

        return self.result
