import logging
import json
import os
import glob
import shutil

from pygeoapi.process.base import BaseProcessor
from processes.podman_processor import PodmanProcessor

PROCESS_METADATA = {
    "version": "1.0.0",
    "id": "tool_simulation_evaluation",
    "title": {
        "en": "Simulation Evaluation",
        "de": "Simulation Evaluation"
    },
    "description": {
        "en": "Evaluates simulation time series against observations and generates evaluation outputs.",
        "de": "Vergleicht Simulationen mit Beobachtungen und erzeugt Auswertungs-Ausgaben."
    },
    "keywords": ["simulation", "evaluation", "timeseries", "camels"],
    "links": [{
        "type": "text/html",
        "rel": "about",
        "title": "information",
        "href": "https://github.com/VForWaTer/simulation_evaluation/releases/tag/v1.0.0",
        "hreflang": "en-US"
    }],
    "inputs": {
        "index_column": {
            "title": "Index column",
            "schema": {"type": "string", "default": "date"},
            "minOccurs": 0,
            "maxOccurs": 1
        },
        "observation_column": {
            "title": "Observation column",
            "schema": {"type": "string", "default": "discharge_spec_obs"},
            "minOccurs": 1,
            "maxOccurs": 1
        },
        "simulation_column": {
            "title": "Simulation column",
            "schema": {"type": "string", "default": "discharge_spec_sim_lstm"},
            "minOccurs": 1,
            "maxOccurs": 1
        },
        "location_column": {
            "title": "Location column",
            "schema": {"type": "string", "default": "catchment_id"},
            "minOccurs": 1,
            "maxOccurs": 1
        },
        "simulation_data": {
            "title": "Simulation data path or wildcard",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1
        },
        "observation_data": {
            "title": "Observation data path or wildcard",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1
        },

    },
    "outputs": {
        "result": {
            "title": "Output directory",
            "schema": {"type": "object", "contentMediaType": "application/json"}
        }
    },
    "example": {
        "inputs": {
            "index_column": "date",
            "observation_column": "discharge_spec_obs",
            "simulation_column": "discharge_spec_sim_lstm",
            "simulation_data": "/in/CAMELS_DE_discharge_sim_DE*.csv",
            "observation_data": "/in/CAMELS_DE_discharge_obs_DE*.csv"
        }
    }
}


class SimulationEvaluationProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data, path=None, extra_mounts=None):
        mimetype = "application/json"

        if path is None:
            path = f"simulation_{os.urandom(5).hex()}"

        user = data.get("User-Info", "default")

        index_column = data.get("index_column", "date")
        observation_column = data.get("observation_column", "discharge_spec_obs")
        simulation_column = data.get("simulation_column", "discharge_spec_sim_lstm")
        location_column = data.get("location_column", "catchment_id")

        simulation_data = data.get("simulation_data")
        observation_data = data.get("observation_data")

        if not simulation_data:
            raise ValueError("Missing required input: simulation_data")
        if not observation_data:
            raise ValueError("Missing required input: observation_data")

        secrets = PodmanProcessor.get_secrets()

        host_path_in = f'{secrets["GEOAPI_PATH"]}/in/{user}/{path}'
        host_path_out = f'{secrets["GEOAPI_PATH"]}/out/{user}/{path}'
        server_path_in = f'{secrets["DATA_PATH"]}/in/{user}/{path}'
        server_path_out = f'{secrets["DATA_PATH"]}/out/{user}/{path}'

        os.makedirs(host_path_in, exist_ok=True)
        os.makedirs(host_path_out, exist_ok=True)
        
        input_json_path = os.path.join(host_path_in, "input.json")
        if os.path.exists(input_json_path):
            os.remove(input_json_path)
        # If host paths are passed in, copy matching files into this tool's /in
        # and rewrite JSON paths to container-visible paths.

        # def stage_input(input_value: str, subfolder: str):
        #     if input_value.startswith("/in/"):
        #         return input_value

        #     matches = sorted(glob.glob(input_value))
        #     if not matches:
        #         raise FileNotFoundError(f"No files match input pattern: {input_value}")

        #     target_dir = os.path.join(host_path_in, subfolder)
        #     os.makedirs(target_dir, exist_ok=True)

        #     for src in matches:
        #         dst = os.path.join(target_dir, os.path.basename(src))
        #         if os.path.abspath(src) == os.path.abspath(dst):
        #             continue
        #         shutil.copy2(src, dst)

        #     return f"/in/{subfolder}/*.csv"
        


        # simulation_data_in_container = stage_input(simulation_data, "sim")
        # observation_data_in_container = stage_input(observation_data, "obs")

        tool_input = {
            "simulation_evaluation": {
                "parameters": {
                    "index_column": index_column,
                    "observation_column": observation_column,
                    "simulation_column": simulation_column,
                    "location_column": location_column
                },
                "data": {
                    "simulation_data": "/in/sim/*.csv",
                    "observation_data": "/in/obs/*.csv"
                }
            }
        }

        with open(os.path.join(host_path_in, "input.json"), "w", encoding="utf-8") as f:
            json.dump(tool_input, f, indent=4)

        image_name = "ghcr.io/vforwater/simulation_evaluation:v3.0.0"

#        image_name = "ghcr.io/safabouguezzi/simulation_evaluation:latest"
#        image_name = "ghcr.io/balazsbis/simulation_evaluation:2.0"
#        image_name = "ghcr.io/vforwater/simulation_evaluation:latest"
#        image_name = "ghcr.io/vforwater/simulation_evaluation:v1.0.0"
        container_name = f"simulation_eval_{os.urandom(5).hex()}"

        mounts = [
            {"type": "bind", "source": server_path_in, "target": "/in", "read_only": True},
            {"type": "bind", "source": server_path_out, "target": "/out"}
        ]
        if extra_mounts:
            mounts.extend(extra_mounts)

        environment = {"TOOL_RUN": "simulation_evaluation"}
        network_mode = "host"
        # command = ["python", "/src/run.py"]

        command = [
    "bash", "-lc",
    """exec 2>&1
set -x
echo "=== LS /in ==="
ls -lah /in
echo "=== LS /in/obs ==="
ls -lah /in/obs
echo "=== LS /in/sim ==="
ls -lah /in/sim
echo "=== RUN TOOL ==="
python /src/run.py 2>&1
rc=$?
echo "RC=$rc"
exit $rc
"""
        ]

        error = "none"
        status = "failed"
        tool_logs = "No logs available"

        try:
            client = PodmanProcessor.connect(secrets["PODMAN_URI"])
            container = PodmanProcessor.pull_run_image(
                client=client,
                image_name=image_name,
                container_name=container_name,
                environment=environment,
                mounts=mounts,
                network_mode=network_mode,
                command=command
            )
            container.reload()
            status = getattr(container, "status", "unknown")
            try:
                tool_logs = "".join(log.decode("utf-8") for log in container.logs())
            except Exception:
                pass
        except Exception as e:
            logging.error("Error running simulation_evaluation: %s", e)
            error = str(e)

        return mimetype, {
            "value": "completed" if error == "none" and status == "exited" else "failed",
            "dir": server_path_out,
            "container_status": status,
            "error": error,
            "tool_logs": tool_logs
        }

    def __repr__(self):
        return "<SimulationEvaluationProcessor>"
