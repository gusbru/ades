import json
import os
import sys
from urllib.parse import urlparse
from dataclasses import dataclass, field
from typing import Any, Optional

try:
    import zoo
except ImportError:
    class Zoo:
        SERVICE_SUCCEEDED = "SUCCEEDED"
        SERVICE_FAILED = "FAILED"
        def update_status(self, conf, progress):
            pass
    zoo = Zoo()

import boto3
from loguru import logger
from pystac import read_file
from pystac.stac_io import DefaultStacIO, StacIO
from botocore.client import Config
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException


logger.remove()
logger.add(sys.stderr, level="INFO")


class JobInformation:
    def __init__(self, conf: dict[str, Any], inputs: dict[str, Any]):
        self.conf = conf
        self.inputs = inputs
        self.tmp_path = conf["main"]["tmpPath"]
        self.process_identifier = conf["lenv"]["Identifier"]
        self.process_usid = conf["lenv"]["usid"]
        self.namespace = conf.get("zooServicesNamespace", {}).get("namespace", "")
        self.workspace_prefix = conf.get("eoepca", {}).get("workspace_prefix", "ws")
        self.job_workspace_suffix = conf.get("eoepca", {}).get(
            "job_workspace_suffix", "job"
        )
        self.input_parameters = self._parse_input_parameters()

    @property
    def workspace(self):
        return f"{self.workspace_prefix}-{self.namespace}"

    @property
    def job_workspace(self):
        return f"{self.workspace_prefix}-{self.namespace}-{self.job_workspace_suffix}"

    @property
    def working_dir(self):
        return os.path.join(
            self.tmp_path, f"{self.process_identifier}-{self.process_usid}"
        )

    def _parse_input_parameters(self):
        """
        Parse the input parameters from the request

        :param input_parameters: The input parameters from the request
        """
        json_request = self.conf.get("request", {}).get("jrequest", {})
        json_request = json.loads(json_request)
        logger.info(f"json_request from request: {json_request}")

        input_parameters = {}
        for key, value in json_request.get("inputs", {}).items():
            logger.info(f"key = {key}, value = {value}")
            if isinstance(value, dict) or isinstance(value, list):
                input_parameters[key] = json.dumps(value)
            else:
                input_parameters[key] = value

        return [{"name": k, "value": v} for k, v in input_parameters.items()]

    def __repr__(self) -> str:
        return f"""
        ************** Job Information **********************
        tmp_path = {self.tmp_path}
        process_identifier = {self.process_identifier}
        process_usid = {self.process_usid}
        workspace = {self.workspace}
        job_workspace = {self.job_workspace}
        working_dir = {self.working_dir}
        namespace = {self.namespace}
        input_parameters = {json.dumps(self.input_parameters, indent=2)}
        *****************************************************"""


@dataclass
class WorkflowConfig:
    conf: dict
    job_information: JobInformation


class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self):
        logger.info("CustomStacIO init")
        s3_region = os.environ.get('STAGEOUT_AWS_REGION')
        s3_endpoint = os.environ.get('STAGEOUT_AWS_SERVICEURL')
        s3_access_key_id = os.environ.get('STAGEOUT_AWS_ACCESS_KEY_ID')
        s3_access_key = os.environ.get('STAGEOUT_AWS_SECRET_ACCESS_KEY')
        
        logger.info(f"AWS_REGION = {s3_region}")
        logger.info(f"AWS_S3_ENDPOINT = {s3_endpoint}")
        logger.info(f"AWS_ACCESS_KEY_ID = {s3_access_key_id}")
        logger.info(f"AWS_SECRET_ACCESS_KEY = {s3_access_key}")

        self.session = boto3.Session(
            region_name=s3_region,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_access_key,
        )
        self.s3_client = self.session.client(
            service_name="s3",
            endpoint_url=s3_endpoint,
            verify=True,
            use_ssl=True,
            config=Config(s3={"addressing_style": "path", "signature_version": "s3v4"}),
        )

    def read_text(self, source, *args, **kwargs):
        parsed = urlparse(source)
        if parsed.scheme == "s3":
            return (
                self.s3_client.get_object(Bucket=parsed.netloc, Key=parsed.path[1:])[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
        else:
            return super().read_text(source, *args, **kwargs)

    def write_text(self, dest, txt, *args, **kwargs):
        parsed = urlparse(dest)
        if parsed.scheme == "s3":
            self.s3_client.put_object(
                Body=txt.encode("UTF-8"),
                Bucket=parsed.netloc,
                Key=parsed.path[1:],
                ContentType="application/geo+json",
            )
        else:
            super().write_text(dest, txt, *args, **kwargs)


class ArgoWorkflow:
    def __init__(self, workflow_config: WorkflowConfig):
        self.workflow_config = workflow_config
        self.conf = workflow_config.conf
        self.job_information = workflow_config.job_information
        self.job_namespace = self.job_information.job_workspace
        self.workflow_manifest: Optional[dict[str, Any]] = None
        self.feature_collection = json.dumps({}, indent=2)

        # Load the kube config from the default location
        logger.info("Loading kube config")
        # config.load_kube_config()
        config.load_config()

        # Create a new client
        logger.info("Creating a new K8s client")
        self.v1 = client.CoreV1Api()
        self.rbac_v1 = client.RbacAuthorizationV1Api()
        self.custom_api = client.CustomObjectsApi()

    def _save_template_job_namespace(self):
        logger.debug(
            f"template_manifest = {json.dumps(self.workflow_manifest, indent=2)}"
        )

        # TODO: try to load the template from the workspace namespace
        name = self.workflow_manifest["metadata"]["name"].lower()
        version = self.workflow_manifest["metadata"]["version"]
        self.workflow_manifest["metadata"]["name"] = f"{name}-{version}"
        self.workflow_manifest["metadata"] = {
            **self.workflow_manifest["metadata"],
            "namespace": self.job_namespace,
            "resourceVersion": version,
        }

        existing_template = self._load_workflow_template()

        if existing_template is not None:
            # TODO: should update?
            return

        # save workflow template if it does not exist
        try:
            # Create the template
            workflow_template_name = self.workflow_manifest["metadata"]["name"]
            logger.info(
                f"Creating workflow template {workflow_template_name} on namespace {self.job_namespace}"
            )

            self.custom_api.create_namespaced_custom_object(
                group="argoproj.io",
                version="v1alpha1",
                namespace=self.job_namespace,
                plural="workflowtemplates",
                body=self.workflow_manifest,
            )
            logger.info(
                f"Workflow template {workflow_template_name} created successfully"
            )
        except Exception as e:
            logger.error(f"Error saving template: {e}")
            raise e

    def _load_workflow_template(self):
        try:
            if self.workflow_manifest is None:
                raise ValueError("workflow_manifest is required")

            # Get the template
            return self.custom_api.get_namespaced_custom_object(
                group="argoproj.io",
                version="v1alpha1",
                namespace=self.job_namespace,
                plural="workflowtemplates",
                name=self.workflow_manifest["metadata"]["name"],
            )
        except ApiException as e:
            if e.status == 404:
                logger.info("Workflow template not found")
                return None
            else:
                raise e
        except Exception as e:
            logger.error(f"Error loading template: {e}")
            raise e

    # Submit the workflow
    def _submit_workflow(self):
        logger.info(f"Submitting workflow {self.job_information.process_usid}")

        if self.job_information.process_usid is None:
            raise ValueError("workflow_id is required")

        workflow_manifest = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Workflow",
            "metadata": {"name": f"{self.job_information.process_usid}".lower()},
            "spec": {
                "workflowTemplateRef": {
                    "name": self.workflow_manifest["metadata"]["name"],
                    "namespace": self.workflow_manifest["metadata"]["namespace"],
                },
                "arguments": {
                    "parameters": self.job_information.input_parameters  # Add parameters if provided
                },
            },
        }

        workflow = self.custom_api.create_namespaced_custom_object(
            group="argoproj.io",
            version="v1alpha1",
            namespace=self.job_namespace,
            plural="workflows",
            body=workflow_manifest,
        )

        return workflow

    def _update_status(self, progress: int, message: Optional[str] = None) -> None:
        """updates the execution progress (%) and provides an optional message"""
        if message:
            self.conf["lenv"]["message"] = message

        zoo.update_status(self.conf, progress)

    # Monitor the workflow execution
    def _monitor_workflow(self, workflow: dict):
        logger.info(f"Monitoring workflow {workflow['metadata']['name']}")

        if self.job_namespace is None:
            raise ValueError("job_namespace is required")

        w = watch.Watch()
        workflow_stream = w.stream(
            self.custom_api.list_namespaced_custom_object,
            group="argoproj.io",
            version="v1alpha1",
            namespace=self.job_namespace,
            plural="workflows",
            field_selector=f"metadata.name={workflow['metadata']['name']}",
        )

        phase_data = None
        self._update_status(10, "Workflow started")
        for data in workflow_stream:
            event_data = data["type"]
            workflow_name = data["object"]["metadata"]["name"]
            phase_data = data["object"].get("status", {}).get("phase")
            progress_data = data["object"].get("status", {}).get("progress")

            # Stop watching if the workflow has reached a terminal state
            if phase_data in ["Succeeded", "Failed", "Error"]:
                logger.info(
                    f"Workflow {workflow_name} has reached a terminal state: {phase_data}"
                )

                # need to save this on the logs?
                # logger.info(json.dumps(data, indent=2))
                self._update_status(100, phase_data)
                w.stop()

            # print(data)
            logger.info(
                f"Event: {event_data}, Workflow: {workflow_name}, Phase: {phase_data}, Progress: {progress_data}"
            )

        logger.info("Workflow monitoring complete")

        if phase_data == "Succeeded":
            return zoo.SERVICE_SUCCEEDED
        else:
            return zoo.SERVICE_FAILED

    def run(self, workflow_file: Optional[dict[str, Any]] = None):
        try:
            if workflow_file is not None:
                self.workflow_manifest = workflow_file
            else:
                # Load the workflow template
                logger.info("Loading workflow template")
                self.workflow_manifest = self._load_workflow_template()

            # Template workflow needs to be on the same namespace as the job
            self._save_template_job_namespace()

            workflow = self._submit_workflow()
            exit_status = self._monitor_workflow(workflow)

            self._save_feature_collection()
            self._save_workflow_logs()
            

            return exit_status
        except Exception as e:
            logger.error(f"Error running workflow: {e}")
            self._save_workflow_logs()
            return zoo.SERVICE_FAILED

    def get_collection(self):
        StacIO.set_default(CustomStacIO)
        collection_s3_path = f"s3://{self.job_information.workspace}/processing-results/{self.job_information.process_usid}/collection.json"
        logger.info(f"Getting collection at {collection_s3_path}")
        return read_file(collection_s3_path)
    
    def _get_pods_for_workflow(self):
        logger.info(f"Getting pods for workflow {self.job_information.process_usid}")
        pods = self.v1.list_namespaced_pod(namespace=self.job_namespace)
        return [pod for pod in pods.items if self.job_information.process_usid in pod.metadata.name]

    def _save_feature_collection(self):
        # get results
        collection = self.get_collection()
        self.feature_collection = json.dumps(
            collection.to_dict(transform_hrefs=False)
        )
    
    def _save_workflow_logs(self, log_filename="logs.log"):
        try:
            logger.info(
                f"Getting logs for workflow {self.job_information.process_usid} in namespace {self.job_namespace}"
            )
            # list pods for namespace
            pods = self._get_pods_for_workflow()

            logger.info(f"Saving logs to {log_filename}")
            with open(log_filename, "w") as f:
                for pod in pods:
                    try:
                        logger.info(f"Getting logs for pod {pod.metadata.name}")
                        f.write(f"{'='*80}\n")
                        f.write(f"Logs for pod {pod.metadata.name}:\n")
                        f.write(f"{'='*80}\n")
                        for container in pod.spec.containers:
                            f.write(f"Container {container.name}:\n")
                            f.write(
                                self.v1.read_namespaced_pod_log(
                                    name=pod.metadata.name,
                                    namespace=self.job_namespace,
                                    container=container.name,
                                )
                            )
                    except Exception as e:
                        logger.error(
                            f"Error getting logs for pod {pod.metadata.name}: {e}"
                        )
                        f.write(f"Error getting logs for pod {pod.metadata.name}: {e}")

                logger.info(f"Logs saved to {log_filename}")
                f.write(f"\n{'='*80}\n")

            #
            services_logs = {
                "url": os.path.join(
                    self.conf["main"]["tmpUrl"],
                    f"{self.job_information.process_identifier}-{self.job_information.process_usid}",
                    os.path.basename(log_filename),
                ),
                "title": f"Process execution log {os.path.basename(log_filename)}",
                "rel": "related",
            }

            if not self.conf.get("service_logs"):
                self.conf["service_logs"] = {}

            for key in services_logs.keys():
                self.conf["service_logs"][key] = services_logs[key]

            self.conf["service_logs"]["length"] = "1"

        except Exception as e:
            logger.error(f"Error getting logs: {e}")

    def delete_workflow(self):
        try:
            # delete the pods
            pods = self._get_pods_for_workflow()
            for pod in pods:
                self.v1.delete_namespaced_pod(name=pod.metadata.name, namespace=self.job_namespace)

            # delete configmap with env variables
            self.v1.delete_namespaced_config_map(
                name=f"env-variables-{self.job_information.process_usid}",
                namespace=self.job_namespace,
            )

            # delete the workflow
            self.custom_api.delete_namespaced_custom_object(
                group="argoproj.io",
                version="v1alpha1",
                namespace=self.job_namespace,
                plural="workflows",
                name=self.job_information.process_usid,
            )

            logger.info(f"Resources for {self.job_namespace}-{self.job_information.process_usid} deleted.")
        except Exception as e:
            logger.error(f"Error deleting Resources for {self.job_namespace}-{self.job_information.process_usid}: {e}")
