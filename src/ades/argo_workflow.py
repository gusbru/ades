import json
import os
import sys
from urllib.parse import urlparse
from dataclasses import dataclass, field
from typing import Any, Optional

import zoo
import boto3
from loguru import logger
from pystac import read_file
from pystac.stac_io import DefaultStacIO, StacIO
from botocore.client import Config
from kubernetes import client, config, watch


logger.remove()
logger.add(sys.stderr, level="INFO")


@dataclass
class StorageCredentials:
    access: str
    bucketname: str
    projectid: str
    secret: str
    endpoint: str
    region: str


@dataclass
class Endpoint:
    id: str
    url: str


@dataclass
class ContainerRegistry:
    username: str
    password: str


@dataclass
class WorkspaceCredentials:
    status: str
    endpoints: list[Endpoint]
    storage: StorageCredentials
    container_registry: ContainerRegistry


class JobInformation:
    def __init__(self, conf: dict[str, Any]):
        self.conf = conf
        self.tmp_path = conf["main"]["tmpPath"]
        self.process_identifier = conf["lenv"]["Identifier"]
        self.process_usid = conf["lenv"]["usid"]
        self.namespace = conf.get("zooServicesNamespace", {}).get("namespace", "")
        self.workspace_prefix = conf.get("eoepca", {}).get("workspace_prefix", "ws")
        self.input_parameters = self._parse_input_parameters()

    @property
    def workspace(self):
        return f"{self.workspace_prefix}-{self.namespace}"

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
        working_dir = {self.working_dir}
        input_parameters = {json.dumps(self.input_parameters, indent=2)}
        *****************************************************
        """


@dataclass
class WorkflowStorageCredentials:
    url: str
    access_key: str
    secret_key: str
    insecure: bool = True

    def __post_init__(self):
        parsed_url = urlparse(self.url)
        self.url = parsed_url.netloc
        storage_protocol = parsed_url.scheme
        self.insecure = storage_protocol == "http"


@dataclass
class WorkflowConfig:
    conf: dict
    job_information: JobInformation
    namespace: Optional[str] = field(default=None)
    workflow_template: Optional[str] = field(default=None)
    workflow_id: Optional[str] = field(default=None)
    workflow_parameters: list[dict] = field(default_factory=list)
    storage_credentials: Optional[WorkflowStorageCredentials] = field(default=None)

    def __post_init__(self):
        self.namespace = self.job_information.workspace
        self.workflow_id = self.job_information.process_usid
        self.workflow_parameters = self.job_information.input_parameters


class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self):
        logger.info("CustomStacIO init")
        logger.info(f"AWS_REGION = {os.environ.get('AWS_REGION')}")
        logger.info(f"AWS_S3_ENDPOINT = {os.environ.get('AWS_S3_ENDPOINT')}")
        logger.info(f"AWS_ACCESS_KEY_ID = {os.environ.get('AWS_ACCESS_KEY_ID')}")

        self.session = boto3.Session(
            region_name=os.environ.get("AWS_REGION"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
        self.s3_client = self.session.client(
            service_name="s3",
            endpoint_url=os.environ.get("AWS_S3_ENDPOINT"),
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
        self.job_namespace: Optional[str] = None
        self.workflow_manifest = None
        self.feature_collection = json.dumps({}, indent=2)
        self.job_information = workflow_config.job_information

        # Load the kube config from the default location
        logger.info("Loading kube config")
        # config.load_kube_config()
        config.load_config()

        # Create a new client
        logger.info("Creating a new K8s client")
        self.v1 = client.CoreV1Api()
        self.rbac_v1 = client.RbacAuthorizationV1Api()
        self.custom_api = client.CustomObjectsApi()

    def _create_job_namespace(self):
        # Create the namespace
        if self.workflow_config.workflow_id is None:
            raise ValueError("workflow_id is required")

        self.job_namespace = f"{self.workflow_config.namespace}-{self.workflow_config.workflow_id}".lower()
        logger.info(f"Creating namespace: {self.job_namespace}")
        namespace_body = client.V1Namespace(
            metadata=client.V1ObjectMeta(name=self.job_namespace)
        )
        self.v1.create_namespace(body=namespace_body)

    # Create storage secret on K8s
    def _create_job_secret(self):
        logger.info(f"Creating storage secrets for namespace: {self.job_namespace}")
        secret_body = client.V1Secret(
            metadata=client.V1ObjectMeta(
                name="storage-credentials"
            ),  # this name needs to match the configuration for the workflow-controller (workflow-controller-configmap)
            string_data={
                "access-key": self.workflow_config.storage_credentials.access_key,
                "secret-key": self.workflow_config.storage_credentials.secret_key,
            },
        )
        self.v1.create_namespaced_secret(namespace=self.job_namespace, body=secret_body)

    def _create_artifact_repository_configmap(self):
        logger.info(
            f"Creating artifact repository configmap for namespace: {self.job_namespace}"
        )
        # Define ConfigMap metadata
        metadata = client.V1ObjectMeta(
            name="artifact-repository",
            annotations={
                "workflows.argoproj.io/default-artifact-repository": "default-v1-s3-artifact-repository"
            },
        )

        # Define ConfigMap data
        data = {
            "default-v1-s3-artifact-repository": f"""
            archiveLogs: true
            s3:
                bucket: {self.workflow_config.namespace}
                endpoint: {self.workflow_config.storage_credentials.url}
                insecure: {str(self.workflow_config.storage_credentials.insecure).lower()}
                accessKeySecret:
                    name: storage-credentials
                    key: access-key
                secretKeySecret:
                    name: storage-credentials
                    key: secret-key
            """
        }

        # Create ConfigMap object
        config_map = client.V1ConfigMap(
            api_version="v1", kind="ConfigMap", metadata=metadata, data=data
        )

        # Create ConfigMap
        self.v1.create_namespaced_config_map(
            namespace=self.job_namespace, body=config_map
        )

    # Create the Role
    def _create_job_role(self):
        logger.info(f"Creating role for namespace: {self.job_namespace}")

        # artifactGC role
        artifact_gc_policy_rule = client.V1PolicyRule(
            api_groups=["argoproj.io"],
            resources=[
                "workflows",
                "workflows/finalizers",
                "workflowartifactgctasks",
                "workflowartifactgctasks/status",
            ],
            verbs=["get", "list", "watch", "create", "update", "patch", "delete"],
        )

        pods_policy_rule = client.V1PolicyRule(
            api_groups=[""],
            resources=["pods"],
            verbs=["get", "list", "watch", "create", "update", "patch", "delete"],
        )

        role_body = client.V1Role(
            metadata=client.V1ObjectMeta(name="pod-patcher"),
            rules=[pods_policy_rule, artifact_gc_policy_rule],
        )

        self.rbac_v1.create_namespaced_role(
            namespace=self.job_namespace, body=role_body
        )

    # Create the RoleBinding
    def _create_job_role_binding(self):
        logger.info(f"Creating role binding for namespace: {self.job_namespace}")
        role_binding_body = client.V1RoleBinding(
            metadata=client.V1ObjectMeta(name="pod-patcher-binding"),
            subjects=[
                {
                    "kind": "ServiceAccount",
                    "name": "default",
                    "namespace": self.job_namespace,
                }
            ],
            role_ref=client.V1RoleRef(
                api_group="rbac.authorization.k8s.io", kind="Role", name="pod-patcher"
            ),
        )
        self.rbac_v1.create_namespaced_role_binding(
            namespace=self.job_namespace, body=role_binding_body
        )

    def _create_job_information_configmap(self):
        logger.info(
            f"Creating job information configmap for namespace: {self.job_namespace}"
        )
        # Define ConfigMap metadata
        metadata = client.V1ObjectMeta(
            name="environment-variables", 
            labels={"workflows.argoproj.io/configmap-type": "Parameter"}
        )

        # Define ConfigMap data
        data = {
            "job_information": json.dumps({
                "WORKSPACE": self.job_information.workspace,
                "WORKING_DIR": self.job_information.working_dir,
                "PROCESS_IDENTIFIER": self.job_information.process_identifier,
                "PROCESS_USID": self.job_information.process_usid,
                "FEATURE_COLLECTION": self.feature_collection,
                "INPUT_PARAMETERS": json.dumps(self.job_information.input_parameters)
            })
        }

        # Create ConfigMap object
        config_map = client.V1ConfigMap(
            api_version="v1", kind="ConfigMap", metadata=metadata, data=data
        )

        # Create ConfigMap
        self.v1.create_namespaced_config_map(
            namespace=self.job_namespace, body=config_map
        )

    def _save_template_job_namespace(self):
        template_manifest = self.workflow_manifest
        logger.info(f"template_manifest = {json.dumps(template_manifest, indent=2)}")
        template_manifest["metadata"]["name"] = template_manifest["metadata"][
            "name"
        ].lower()
        template_manifest["metadata"]["namespace"] = self.job_namespace
        template_manifest["metadata"]["resourceVersion"] = None
        self.save_workflow_template(
            template_manifest=template_manifest, namespace=self.job_namespace
        )

    def load_workflow_template(self):
        try:
            if self.workflow_config.workflow_template is None:
                raise ValueError("workflow_template is required")

            # Get the template
            self.workflow_manifest = self.custom_api.get_namespaced_custom_object(
                group="argoproj.io",
                version="v1alpha1",
                namespace=self.workflow_config.namespace,
                plural="workflowtemplates",
                name=self.workflow_config.workflow_template,
            )

        except Exception as e:
            logger.error(f"Error loading template: {e}")
            raise e

    def save_workflow_template(
        self, template_manifest: dict = None, namespace: Optional[str] = None
    ):
        try:
            # Create the template
            namespace = namespace or self.workflow_config.namespace
            workflow_template_name = template_manifest["metadata"]["name"]
            logger.info(
                f"Creating workflow template {workflow_template_name} on namespace {namespace}"
            )

            self.custom_api.create_namespaced_custom_object(
                group="argoproj.io",
                version="v1alpha1",
                namespace=namespace,
                plural="workflowtemplates",
                body=template_manifest,
            )
            logger.info(
                f"Workflow template {workflow_template_name} created successfully"
            )
        except Exception as e:
            logger.error(f"Error saving template: {e}")
            raise e

    # Submit the workflow
    def _submit_workflow(self):
        if self.workflow_config.workflow_id is None:
            raise ValueError("workflow_id is required")

        workflow_manifest = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Workflow",
            "metadata": {"name": f"{self.workflow_config.workflow_id}".lower()},
            "spec": {
                "workflowTemplateRef": {
                    "name": self.workflow_manifest["metadata"]["name"],
                    "namespace": self.workflow_manifest["metadata"]["namespace"],
                },
                "arguments": {
                    "parameters": self.workflow_config.workflow_parameters  # Add parameters if provided
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

    def _update_status(self, progress: int, message: str = None) -> None:
        """updates the execution progress (%) and provides an optional message"""
        if message:
            self.conf["lenv"]["message"] = message

        zoo.update_status(self.conf, progress)

    # Monitor the workflow execution
    def monitor_workflow(self, workflow: dict):
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
                logger.info(json.dumps(data, indent=2))
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

    def run(self):
        # Load the workflow template
        logger.info(
            f"Loading workflow template: {self.workflow_config.workflow_template}"
        )
        self.load_workflow_template()

        # Create the namespace, access key, and secret key
        logger.info("Creating namespace, roles, and storage secrets")
        self._create_job_namespace()
        self._create_job_secret()
        self._create_artifact_repository_configmap()
        self._create_job_role()
        self._create_job_role_binding()
        self._create_job_information_configmap()

        # Template workflow needs to be on the same namespace as the job
        self._save_template_job_namespace()

        workflow = self._submit_workflow()
        exit_status = self.monitor_workflow(workflow)
        return exit_status

    def run_workflow_from_file(self, workflow_file: dict):
        self.workflow_manifest = workflow_file
        # Create the namespace, access key, and secret key
        logger.info("Creating namespace, roles, and storage secrets")
        self._create_job_namespace()
        self._create_job_secret()
        self._create_artifact_repository_configmap()
        self._create_job_role()
        self._create_job_role_binding()
        self._create_job_information_configmap()

        # Template workflow needs to be on the same namespace as the job
        self._save_template_job_namespace()

        workflow = self._submit_workflow()
        exit_status = self.monitor_workflow(workflow)
        return exit_status

    def save_workflow_logs(self, log_filename="logs.txt"):
        try:
            logger.info(
                f"Getting logs for workflow {self.workflow_config.workflow_id} in namespace {self.job_namespace}"
            )
            # list pods for namespace
            pods = self.v1.list_namespaced_pod(namespace=self.job_namespace)

            logger.info(f"Saving logs to {log_filename}")
            with open(log_filename, "w") as f:
                for pod in pods.items:
                    # only get logs from pods that belong to the workflow
                    if self.workflow_config.workflow_id not in pod.metadata.name:
                        continue

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

                logger.info(f"Logs saved to {log_filename}")
                f.write(f"\n{'='*80}\n")

            # get results
            StacIO.set_default(CustomStacIO)
            collection_s3_path = f"s3://{self.job_information.workspace}/processing-results/{self.job_information.process_usid}/collection.json"
            logger.info(f"Getting collection at {collection_s3_path}")
            collection = read_file(collection_s3_path)
            self.feature_collection = json.dumps(collection.to_dict())

            #
            servicesLogs = {
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

            for key in servicesLogs.keys():
                self.conf["service_logs"][key] = servicesLogs[key]

            self.conf["service_logs"]["length"] = "1"

        except Exception as e:
            logger.error(f"Error getting logs: {e}")
            raise e

    def delete_workflow(self):
        try:
            self.v1.delete_namespace(name=self.job_namespace)
            logger.info(f"Namespace {self.job_namespace} deleted.")
        except Exception as e:
            logger.error(f"Error deleting namespace {self.job_namespace}: {e}")
