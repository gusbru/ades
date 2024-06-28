import os
import json
from typing import Any

import zoo
from loguru import logger
import requests
import yaml

from .argo_workflow import (
    ArgoWorkflow,
    JobInformation,
    WorkflowConfig,
)


def check_file_exists(file_path: str) -> bool:
    return os.path.isfile(file_path)


class ADES:
    job_information: JobInformation

    def __init__(self, conf, inputs, outputs):
        self.conf = conf
        self.inputs = inputs
        self.outputs = outputs

    def register_catalog(self):
        os.environ.pop("HTTP_PROXY", None)
        workspace_api_endpoint = "http://workspace-api.rm:8080"
        stac_catalog = {
            "type": "stac-item",
            "url": f"s3://{self.job_information.workspace}/processing-results/{self.job_information.process_usid}",
        }
        logger.info(f"registering stac_catalog = {stac_catalog}")
        headers = {
            "Content-Type": "application/json",
        }
        r = requests.post(
            f"{workspace_api_endpoint}/workspaces/{self.job_information.workspace}/register",
            json=stac_catalog,
            headers=headers,
        )
        r.raise_for_status()
        logger.info(f"Register processing results response: {r.status_code}")

    def register_collection(self, collection: str):
        os.environ.pop("HTTP_PROXY", None)
        workspace_api_endpoint = "http://workspace-api.rm:8080"
        collection_dict = json.loads(collection)
        logger.info(f"registering collection = {collection}")
        headers = {
            "Content-Type": "application/json",
        }
        r = requests.post(
            f"{workspace_api_endpoint}/workspaces/{self.job_information.workspace}/register-json",
            json=collection_dict,
            headers=headers,
        )
        r.raise_for_status()
        logger.info(f"Register processing results response: {r.status_code}")

    def _prepare_work_directory(self):
        logger.info("Preparing work directory")
        os.makedirs(
            self.job_information.working_dir,
            mode=0o777,
            exist_ok=True,
        )
        os.chdir(self.job_information.working_dir)

    def execute_runner(self):
        try:
            logger.info("Starting execute runner")

            self.job_information = JobInformation(conf=self.conf, inputs=self.inputs)
            logger.info(self.job_information)

            self._prepare_work_directory()

            logger.info("Starting execute runner")

            # run workflow on Argo
            # from API
            logger.info(f"Preparing job on workspace: {self.job_information.workspace}")
            logger.info(f"Job process (workflow): {self.job_information.process_usid}")

            # get Storage credentials from workspace-api.
            # TODO: Use the default storage credentials for the global workspace
            # workspace_credentials = get_credentials(self.job_information.workspace)

            #############################################################
            workflow_config = WorkflowConfig(
                conf=self.conf,
                job_information=self.job_information,
            )

            # run the workflow
            logger.info("Running workflow")
            argo_workflow = ArgoWorkflow(workflow_config=workflow_config)
            exit_status = argo_workflow.run()

            # if there is a collection_id on the input, add the processed item into that collection
            if exit_status == zoo.SERVICE_SUCCEEDED:
                collection = argo_workflow.feature_collection
                logger.info("Registering collection")
                self.register_collection(collection)

                # Register Catalog
                # TODO: consider more use cases
                logger.info("Registering catalog")
                self.register_catalog()

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
            if os.environ.get("NAMESPACE_CLEANUP") is not None:
                logger.info("Cleaning up namespace")
                argo_workflow.delete_workflow()

            return exit_status

        except Exception as e:
            logger.error("ERROR in processing execution template...")
            stack = str(e)
            logger.error(stack)
            self.conf["lenv"]["message"] = zoo._(
                f"Exception during execution...\n{stack}\n"
            )
            return zoo.SERVICE_FAILED
