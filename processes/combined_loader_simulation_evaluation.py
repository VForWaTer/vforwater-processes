import logging
import json
import os
import glob
import shutil
import urllib.parse

from pygeoapi.process.base import BaseProcessor
from processes.tool_vforwater_loader import VforwaterLoaderProcessor
from processes.tool_simulation_evaluation import SimulationEvaluationProcessor
from processes.podman_processor import PodmanProcessor


PROCESS_METADATA = {
    "version": "1.0.0",
    "id": "combined_loader_simulation_evaluation",
    "name": "ChainedLoaderSimulationEvaluation",
    "title": {
        "en": "Evaluation Tool",
        "de": "Simulation Evaluation Tool"
    },
    "description": {
        "en": "Evaluation tool - A containerized tool for evaluating hydrological simulations against observations across multiple catchments. It computes standard performance metrics and produces an interactive HTML report with time series plots and statistical summaries.",
        "de": "Lädt Zeitreihendaten mit dem V-FOR-WaTer-Loader und wertet Simulationsergebnisse mit dem simulation_evaluation-Container aus."
    },
    "keywords": ["loader", "simulation", "evaluation", "timeseries", "camels", "chain"],
    "jobControlOptions": ["async-execute"],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'Loader GitHub',
        'href': 'https://github.com/VForWaTer/tool_vforwater_loader',
        'hreflang': 'en-US'
         },
        {
            "type": "text/html",
            "rel": "about",
            "title": "Evaluation tool GitHub",
            "href": "https://github.com/VForWaTer/simulation_evaluation"
        }],
    "inputs": {
        "timeseries_ids": {
            "title": "List of Timeseries",
            "schema": {"type": "timeseries", "required": "false"},
            "minOccurs": 0,
            "maxOccurs": 0
        },
        "raster_ids": {
            "title": "List of Raster data",
            "schema": {"type": "raster", "required": "false"},
            "minOccurs": 0,
            "maxOccurs": 0
        },
        "reference_area": {
            "title": "Coordinates",
            "schema": {"type": "geometry", "format": "GEOJSON", "required": "false"},
            "minOccurs": 0,
            "maxOccurs": 1
        },
        "start_date": {
            "title": "Start Date",
            "schema": {"type": "dateTime", "format": "string", "required": "true"},
            "minOccurs": 1,
            "maxOccurs": 1
        },
        "end_date": {
            "title": "End Date",
            "schema": {"type": "dateTime", "format": "string", "required": "true"},
            "minOccurs": 1,
            "maxOccurs": 1
        },
        "cell_touches": {
            "title": "Cell Touches",
            "schema": {"type": "boolean", "default": True},
            "minOccurs": 0,
            "maxOccurs": 1
        },
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
        "model_file": {
            "title": "Model CSV file",
            "description": "Upload the model CSV file from your computer. The file is stored temporarily for execution and deleted afterwards.",
            "schema": {
                "type": "string",
                "contentMediaType": "text/csv"
            },
            "keywords": ["upload"],
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
            "timeseries_ids": ["110000"],
            "start_date": "2008-01-01T00:00:00Z",
            "end_date": "2015-12-31T00:00:00Z",
            "index_column": "date",
            "observation_column": "discharge_spec_obs",
            "simulation_column": "discharge_spec_sim_lstm",
            "location_column": "catchment_id"
        }
    }
}


class CombinedLoaderSimulationEvaluationProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        mimetype = "application/json"

        user_folder = data.get("User-Info", "default")
        shared_id = data.get("shared_id")
        if not shared_id:
            raise ValueError("Missing shared_id from upload step")

        secrets = PodmanProcessor.get_secrets()

        host_in_path = os.path.join(secrets["GEOAPI_PATH"], "in", user_folder, shared_id)
        host_out_path = os.path.join(secrets["GEOAPI_PATH"], "out", user_folder, shared_id)
        server_in_path = os.path.join(secrets["DATA_PATH"], "in", user_folder, shared_id)
        server_out_path = os.path.join(secrets["DATA_PATH"], "out", user_folder, shared_id)

        os.makedirs(server_in_path, exist_ok=True)
        os.makedirs(server_out_path, exist_ok=True)
        os.makedirs(host_in_path, exist_ok=True)
        os.makedirs(host_out_path, exist_ok=True)

        obs_dir = os.path.join(
            secrets["GEOAPI_PATH"],
            "in",
            user_folder,
            shared_id,
            "simulation",
            "obs"
        )
        sim_dir = os.path.join(
            secrets["GEOAPI_PATH"],
            "in",
            user_folder,
            shared_id,
            "simulation",
            "sim"
        )

        os.makedirs(obs_dir, exist_ok=True)
        os.makedirs(sim_dir, exist_ok=True)

        def run_path(*parts: str) -> str:
            return "/".join([shared_id, *[p for p in parts if p]])

        # 1) Run loader
        loader = VforwaterLoaderProcessor({"name": "vforwater_loader"})
        _, loader_output = loader.execute(data, path=run_path("loader"))

        if isinstance(loader_output, str):
            loader_output = json.loads(loader_output)

        loader_out_dir_server = loader_output.get("dir")
        if not loader_out_dir_server:
            raise RuntimeError("Loader returned no output dir")

        loader_out_dir_host = loader_out_dir_server.replace(
            secrets["DATA_PATH"],
            secrets["GEOAPI_PATH"]
        )

        datasets_host_path = os.path.join(loader_out_dir_host, "datasets")
        csv_files = sorted(glob.glob(os.path.join(datasets_host_path, "discharge_*.csv")))

        if not csv_files:
            csv_files = sorted(glob.glob(os.path.join(datasets_host_path, "*.csv")))

        if not csv_files:
            raise RuntimeError(f"No CSV file found in loader datasets folder: {datasets_host_path}")

        logging.info("Found %d loader CSV file(s): %s", len(csv_files), csv_files)

        # 2) Copy loader output CSV(s) to simulation/sim
        for src in csv_files:
            dst = os.path.join(obs_dir, os.path.basename(src))
            shutil.copy2(src, dst)

        # 3) Build observation/simulation patterns
        observation_pattern = os.path.join(obs_dir, "*.csv")
        simulation_pattern = os.path.join(sim_dir, "*.csv")

        logging.info("observation_pattern: %s", observation_pattern)
        logging.info("simulation_pattern: %s", simulation_pattern)

        if not glob.glob(observation_pattern):
            raise RuntimeError(f"No uploaded simulation CSV found in: {sim_dir}")

        if not glob.glob(simulation_pattern):
            raise RuntimeError(f"No loader observation CSV found in: {obs_dir}")

        obs_files = sorted(glob.glob(observation_pattern))
        sim_files = sorted(glob.glob(simulation_pattern))

        obs_name = os.path.basename(obs_files[0])
        sim_name = os.path.basename(sim_files[0])

        obs_id = os.path.splitext(obs_name)[0].split("_")[-1]
        sim_id = os.path.splitext(sim_name)[0].split("_")[-1]

        if obs_id != sim_id:
            raise RuntimeError(
                f"Observation and simulation catchment IDs do not match: "
                f"{obs_id} vs {sim_id}"
            )

        # 4) Run simulation evaluation
        simulation = SimulationEvaluationProcessor({"name": "simulation_evaluation"})

        sim_data = dict(data)
        sim_data["observation_data"] = observation_pattern
        sim_data["simulation_data"] = simulation_pattern

        _, sim_output = simulation.execute(
            sim_data,
            path=run_path("simulation")
        )

        if isinstance(sim_output, str):
            sim_output = json.loads(sim_output)




        html_files = []
        html_dir_host = os.path.join(host_out_path, "simulation")

        if os.path.isdir(html_dir_host):
            for fname in sorted(os.listdir(html_dir_host)):
                full = os.path.join(html_dir_host, fname)
                if not os.path.isfile(full):
                    continue

                if fname.lower().endswith(".html"):
                    rel = os.path.relpath(full, host_out_path).replace("\\", "/")
                    html_files.append(rel)

        else:
            logging.info("ℹ No 'simulation' directory found at: %s", host_out_path)

        return mimetype, {
            "value": sim_output.get("value", "failed"),
            "dir": server_out_path , #sim_output.get("dir"),
            "loader_dir": loader_out_dir_server,
            "simulation_dir": sim_output.get("dir"),
            "container_status": sim_output.get("container_status", "unknown"),
            "error": sim_output.get("error", "none"),
            'plots': html_files,
            "tool_logs": sim_output.get("tool_logs", "")
        }

    def __repr__(self):
        return "<CombinedLoaderSimulationEvaluationProcessor>"
