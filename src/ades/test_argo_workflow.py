import json
from typing import Any

from loguru import logger
import yaml


from argo_workflow import (
    ArgoWorkflow,
    JobInformation,
    WorkflowConfig,
)

def main():
    # load conf from file
    with open('src/ades/conf.json', 'r') as f:
        conf = json.load(f)

    # load inputs from file
    with open('src/ades/inputs.json', 'r') as f:
        inputs = json.load(f)

    with open("src/ades/argo_template.yaml", "r") as stream: 
        argo_template = yaml.safe_load(stream)

    job_information = JobInformation(conf=conf, inputs=inputs)
    
    workflow_config = WorkflowConfig(
        conf=conf,
        job_information=job_information,
    )

    # run the workflow
    logger.info("Running workflow")
    argo_workflow = ArgoWorkflow(workflow_config=workflow_config)
    exit_status = argo_workflow.run(workflow_file=argo_template)
    logger.info(f"Workflow finished with exit status: {exit_status}")
    argo_workflow.delete_workflow()

if __name__ == '__main__':
    main()