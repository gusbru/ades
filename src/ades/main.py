import os
import json
from typing import Any

import zoo
from loguru import logger
import requests
import yaml

from .argo_workflow import (
    ArgoWorkflow,
    ContainerRegistry,
    JobInformation,
    WorkflowConfig,
    WorkflowStorageCredentials,
    WorkspaceCredentials,
    Endpoint,
    StorageCredentials,
)


class ADES:
    def __init__(self, conf, inputs, outputs):
        self.conf = conf
        self.inputs = inputs
        self.outputs = outputs

    def get_credentials(sefl, workspace: str) -> WorkspaceCredentials:
        os.environ.pop("HTTP_PROXY", None)
        logger.info("Getting credentials")
        response = requests.get(f"http://workspace-api.rm:8080/workspaces/{workspace}")
        response.raise_for_status()

        response_api = response.json()

        endpoints = [
            Endpoint(id=e["id"], url=e["url"]) for e in response_api["endpoints"]
        ]
        storage = StorageCredentials(
            access=response_api["storage"]["credentials"]["access"],
            bucketname=response_api["storage"]["credentials"]["bucketname"],
            projectid=response_api["storage"]["credentials"]["projectid"],
            secret=response_api["storage"]["credentials"]["secret"],
            endpoint=response_api["storage"]["credentials"]["endpoint"],
            region=response_api["storage"]["credentials"]["region"],
        )
        container_registry = ContainerRegistry(
            username=response_api["container_registry"]["username"],
            password=response_api["container_registry"]["password"],
        )

        # set environment variables
        os.environ["AWS_REGION"] = storage.region
        os.environ["AWS_S3_ENDPOINT"] = storage.endpoint
        os.environ["AWS_ACCESS_KEY_ID"] = storage.access
        os.environ["AWS_SECRET_ACCESS_KEY"] = storage.secret

        return WorkspaceCredentials(
            status=response_api["status"],
            endpoints=endpoints,
            storage=storage,
            container_registry=container_registry,
        )

    def load_workflow_template_from_file(self, file_name: str = "app-package.cwl") -> dict[str, Any]:
        logger.info(f"open file: {file_name}")

        # this will read files from /opt/zooservices_user/<namespace>/<service_name>/<file_name>
        zoo_service_namespace = self.conf["lenv"]["cwd"]
        zoo_service_name = self.conf["lenv"]["Identifier"]
        
        with open(
            os.path.join(
                zoo_service_namespace,
                zoo_service_name,
                file_name,
            ),
            "r",
        ) as stream:
            argo_template = yaml.safe_load(stream)

        return argo_template

    def register_catalog(self, job_information: JobInformation):
        os.environ.pop("HTTP_PROXY", None)
        workspace_api_endpoint = "http://workspace-api.rm:8080"
        stac_catalog = {
            "type": "stac-item",
            "url": f"s3://{job_information.workspace}/processing-results/{job_information.process_usid}",
        }
        logger.info(f"registering stac_catalog = {stac_catalog}")
        headers = {
            "Content-Type": "application/json",
        }
        r = requests.post(
            f"{workspace_api_endpoint}/workspaces/{job_information.workspace}/register",
            json=stac_catalog,
            headers=headers,
        )
        r.raise_for_status()
        logger.info(f"Register processing results response: {r.status_code}")

    def register_collection(self, job_information: JobInformation, collection: str):
        os.environ.pop("HTTP_PROXY", None)
        workspace_api_endpoint = "http://workspace-api.rm:8080"
        collection_dict = json.loads(collection)
        logger.info(f"registering collection = {collection}")
        headers = {
            "Content-Type": "application/json",
        }
        r = requests.post(
            f"{workspace_api_endpoint}/workspaces/{job_information.workspace}/register-json",
            json=collection_dict,
            headers=headers,
        )
        r.raise_for_status()
        logger.info(f"Register processing results response: {r.status_code}")


    def prepare_work_directory(self, job_information: JobInformation):
        os.makedirs(
            job_information.working_dir,
            mode=0o777,
            exist_ok=True,
        )
        os.chdir(job_information.working_dir)

    def execute_runner(self):
        try:
            logger.info("Starting execute runner")
            argo_template = self.load_workflow_template_from_file()

            job_information = JobInformation(conf=self.conf, inputs=self.inputs)
            logger.info(job_information)

            self.prepare_work_directory(job_information)

            logger.info("Starting execute runner")

            # run workflow on Argo
            # from API
            logger.info(
                f"preparing job on workspace {job_information.workspace} with process (workflow) {job_information.process_usid}"
            )

            # get Storage credentials from workspace-api.
            # TODO: Use the default storage credentials for the global workspace
            workspace_credentials = self.get_credentials(job_information.workspace)

            #############################################################
            workflow_config = WorkflowConfig(
                conf=self.conf,
                job_information=job_information,
                storage_credentials=WorkflowStorageCredentials(
                    url=workspace_credentials.storage.endpoint,
                    access_key=workspace_credentials.storage.access,
                    secret_key=workspace_credentials.storage.secret,
                ),
            )

            # run the workflow
            logger.info("Running workflow")
            argo_workflow = ArgoWorkflow(workflow_config=workflow_config)
            exit_status = argo_workflow.run_workflow_from_file(argo_template)

            # if there is a collection_id on the input, add the processed item into that collection
            if exit_status == zoo.SERVICE_SUCCEEDED:
                collection = argo_workflow.feature_collection
                logger.info("Registering collection")
                self.register_collection(job_information, collection)
                
                # Register Catalog
                # TODO: consider more use cases
                logger.info("Registering catalog")
                self.register_catalog(job_information)

                logger.info(
                    f"Setting Collection into output key {list(self.outputs.keys())[0]}"
                )
                self.outputs[list(self.outputs.keys())[0]]["value"] = (
                    argo_workflow.feature_collection
                )
                logger.info(f"outputs = {json.dumps(self.outputs, indent=4)}")

            else:
                error_message = zoo._("Execution failed")
                logger.error(f"Execution failed: {error_message}")
                self.conf["lenv"]["message"] = error_message
                exit_status = zoo.SERVICE_FAILED

            # Clean up the namespace
            # argo_workflow.delete_workflow()

            return exit_status

        except Exception as e:
            logger.error("ERROR in processing execution template...")
            stack = str(e)
            logger.error(stack)
            self.conf["lenv"]["message"] = zoo._(
                f"Exception during execution...\n{stack}\n"
            )
            return zoo.SERVICE_FAILED
