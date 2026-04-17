import logging
import json
import time
import shutil
import os
import uuid
import glob
from pygeoapi.process.base import BaseProcessor
from processes.tool_vforwater_loader import VforwaterLoaderProcessor
from processes.tool_whiteboxgis_v_0_9_2 import WhiteboxGISProcessorV092
from processes.podman_processor import PodmanProcessor
from processes.tool_catflow import CatflowProcessor

PROCESS_METADATA = {
    'version': '1.1.0',
    'id': 'combined_loader_whitebox',
    'name': 'ChainedLoaderWhitebox',
    'title': {
        'en': 'DEM2CAT _ Automated Preprocessing Toolchain for CATFLOW',
        'de': 'DEM2CAT'
    },
    'description': {
        'en': "From DEM to CATFLOW _ transformation tool: DEM2CAT is a streamlined, containerized workflow designed to automate the transformation of raw topographic and spatial datasets into model-ready hillslope geometries for the CATFLOW hydrological model. Built within the V-FOR-WaTer ecosystem and aligned with FAIR data principles, DEM2CAT minimizes manual effort, enhances reproducibility, and promotes transparent environmental modeling workflows.",
        'de': 'Lädt Datensätze aus dem Metakatalog und führt Hillslope-Analyse mit WhiteboxGIS durch.'
    },
    'keywords': ['loader', 'whitebox', 'chain', 'raster', 'hydrology'],
    'jobControlOptions': ['async-execute'],
    'inputs': {
        'timeseries_ids': {
            'title': 'List of Timeseries',
            'schema': {'type': 'timeseries', 'required': 'false'},
            'minOccurs': 0,
            'maxOccurs': 0
        },
        'raster_ids': {
            'title': 'List of Raster data',
            'schema': {'type': 'raster', 'required': 'false'},
            'minOccurs': 0,
            'maxOccurs': 0
        },
        'reference_area': {
            'title': 'Coordinates',
            'schema': {'type': 'geometry', 'format': 'GEOJSON', 'required': 'false'},
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'start_date': {
            'title': 'Start Date',
            'schema': {'type': 'dateTime', 'format': 'string', 'required': 'true'},
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'end_date': {
            'title': 'End Date',
            'schema': {'type': 'dateTime', 'format': 'string', 'required': 'true'},
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'stream_threshold_low': {
            'title': 'Stream Threshold Low',
            'schema': {'type': 'number', 'default': 10000.0},
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'stream_threshold_high': {
            'title': 'Stream Threshold High',
            'schema': {'type': 'number', 'default': 20000.0},
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'to_file': {
            'title': 'Save to File',
            'schema': {'type': 'boolean', 'default': True},
            'minOccurs': 0,
            'maxOccurs': 1
        },

        'target_epsg': {
            'title': 'Target EPSG (metric)',
            'schema': {'type': 'integer', 'default': 25832},
            'minOccurs': 0,
            'maxOccurs': 1
        },

        'cell_size': {
            'title': 'Cell size (m)',
            'schema': {'type': 'number', 'default': 30.0},
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'resampling': {
            'title': 'Resampling',
            'schema': {'type': 'string', 'enum': ['nearest', 'bilinear', 'cubic'], 'default': 'bilinear'},
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'source_epsg': {
            'title': 'Source EPSG (optional)',
            'schema': {'type': 'integer', 'default': 4326},
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'hillslope_id': {'title': 'Hillslope ID', 'schema': {'type': 'integer', 'default': -1}},
        'no_flow_area': {'title': 'No Flow Area', 'schema': {'type': 'number', 'default': 0.30}},
        'min_cells': {'title': 'Minimum Cells', 'schema': {'type': 'integer', 'default': 10}},
        'hill_type': {
            'title': 'Hillslope Type',
            'schema': {
                'type': 'string',
                'enum': ['constant', 'cake', 'variable'],
                'default': 'constant'
            }
        },
        'depth': {'title': 'Soil Depth', 'schema': {'type': 'number', 'default': 2.1}}
    },
    'outputs': {
        'result': {
            'title': 'Chained Output Directory',
            'schema': {'type': 'object', 'contentMediaType': 'application/json'}
        }
    },
    'example': {
        'inputs': {
            'dataset_ids': [1, 2, 19],
            'start_date': '2000-01-01T12:00:00+01',
            'end_date': '2014-01-01T12:00:00+01',
            'reference_area': {
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[8.9771580802, 47.2703623267],
                                    [13.83504270839, 47.2703623267],
                                    [13.8350427083, 50.5644529365],
                                    [8.9771580802, 50.5644529365],
                                    [8.9771580802, 47.2703623267]]]
                },
                'properties': 'Bayern'
            }
        }
    }
}

class CombinedLoaderWhiteboxProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        mimetype = 'application/json'
        user_folder = data.get("User-Info", "default")
        shared_id = f'combined_{os.urandom(5).hex()}'

        # Setup host paths
        base_path = '/data/geoapi_data'
        #host_in_path = os.path.join(base_path, 'in', user_folder, shared_id)
        #host_out_path = os.path.join(base_path, 'out', user_folder, shared_id)

        secrets = PodmanProcessor.get_secrets()
        host_in_path = f'{secrets["GEOAPI_PATH"]}/in/{user_folder}/{shared_id}'  # path in container (mounted in '/data/geoapi' auf server)
        host_out_path = f'{secrets["GEOAPI_PATH"]}/out/{user_folder}/{shared_id}'

        # Setup container paths (bound by Podman)
        container_in_path = '/in'
        container_out_path = '/out'

        server_path_in = f'{secrets["DATA_PATH"]}/in/{user_folder}/{shared_id}'  # path in container (mounted in '/data/geoapi' auf server)
        server_path_out = f'{secrets["DATA_PATH"]}/out/{user_folder}/{shared_id}'  # was out_dir

        # mounts = [{'type': 'bind', 'source': '/data', 'target': '/data', 'read_only': True},
        #           {'type': 'bind', 'source': server_path_in, 'target': container_in_path, 'read_only': True},
        #           {'type': 'bind', 'source': server_path_out, 'target': container_out_path}]  # mal entfernen in pull run
        # logging.info(f'use mounts: {mounts}')
        
        os.makedirs(host_in_path, exist_ok=True)
        os.makedirs(host_out_path, exist_ok=True)

        def run_path(*parts: str) -> str:
            # allows paths like: combined_x/whitebox/merge
            return "/".join([shared_id, *[p for p in parts if p]])
        
        loader = VforwaterLoaderProcessor({"name": "vforwater_loader"})
        _, loader_output = loader.execute(data, path=run_path("loader"))
        if isinstance(loader_output, str):
            loader_output = json.loads(loader_output)

        logging.info("📤 loader_output path: %s", loader_output)
        print("📤 loader_output path: %s", loader_output)
        loader_out_dir_server = loader_output.get("dir")  # DATA_PATH/out/<user>/<run_id>/loader
        if not loader_out_dir_server:
            raise RuntimeError("Loader returned no output dir")
        
        logging.info("📤 loader_out_dir_server path: %s", loader_out_dir_server)
        print("📤 loader_out_dir_server path: %s", loader_out_dir_server)   


        # loader_out_dir_host for local filesystem operations (if needed)
        loader_out_dir_host = loader_out_dir_server.replace(secrets["DATA_PATH"], secrets["GEOAPI_PATH"])

        expected_raster_folder_host = os.path.join(
            loader_out_dir_host, "datasets", "elevation_1100"
        )


        expected_raster_folder = os.path.join(loader_output.get("dir"), 'datasets', 'elevation_1100')
        logging.info("📤 Using expected_raster_folder path: %s", expected_raster_folder)
        # /data/geoapi_data/out/user1580_safa/testfolder/datasets/elevation_1100
        print("📤 Using expected_raster_folder path: %s", expected_raster_folder)

        # expected_raster_folder_host = os.path.join(host_out_path, 'datasets', 'elevation_1100')
        logging.info("📤 Using expected_raster_folder path: %s", expected_raster_folder_host)
        #/home/geoapi/out/user1580_safa/testfolder/datasets/elevation_1100
        print("📤 Using expected_raster_folder path: %s", expected_raster_folder_host)

        # import glob
        # waited = 0
        raster_candidates = []
        # while waited < 15:
        #     raster_candidates = glob.glob(os.path.join(expected_raster_folder, '*.tif'))
        #     if raster_candidates:
        #         break
        #     time.sleep(1)
        #     waited += 1

        # if not raster_candidates:
        #     raise RuntimeError(f"❌ No raster files found in {expected_raster_folder} after waiting.")

        # for tif in raster_candidates:
        #     shutil.copy(tif, host_in_path)
        # Poll until the loader’s rasters become visible (race-proof)






        timeout_s = 30
        interval_s = 0.5
        elapsed = 0.0

        raster_candidates = []
        while elapsed < timeout_s:
            if os.path.isdir(expected_raster_folder_host):
                raster_candidates = glob.glob(os.path.join(expected_raster_folder_host, '*.tif'))
                if raster_candidates:
                    break
            time.sleep(interval_s)
            elapsed += interval_s

        logging.info("🕒 Waited %.1fs for rasters; found %d file(s) in %s",
                    elapsed, len(raster_candidates), expected_raster_folder_host)

        if not raster_candidates:
            # Extra diagnostics to help if it happens again
            parent = os.path.dirname(expected_raster_folder_host)
            listing = os.listdir(parent) if os.path.isdir(parent) else "<parent missing>"
            raise RuntimeError(f"No raster .tif files found in {expected_raster_folder_host} "
                            f"(parent contents: {listing})")

        # (Optional) small settle delay
        time.sleep(0.2)

        # --- Prepare WHITEBOX input folder (whitebox/in) ---
        whitebox_in_host = f'{secrets["GEOAPI_PATH"]}/in/{user_folder}/{run_path("whitebox")}'
        os.makedirs(whitebox_in_host, exist_ok=True)

        for tif in raster_candidates:
            shutil.copy2(tif, whitebox_in_host)

        logging.info("✅ Copied %d raster(s) into whitebox input: %s", len(raster_candidates), whitebox_in_host)

        whitebox = WhiteboxGISProcessorV092({"name": "tool_whiteboxgis_v_0_9_2"})


        # Decide merge-or-skip based on number of TIFFs from the loader
        if len(raster_candidates) == 0:
            raise RuntimeError(f"No raster .tif files found in {expected_raster_folder}")

        # Decide merge-or-skip
        if len(raster_candidates) == 1:
            dem_host_path = os.path.join(whitebox_in_host, os.path.basename(raster_candidates[0]))
            logging.info("Single DEM detected; skipping merge. Using: %s", dem_host_path)
            merge_output = {
                "dir": f'{secrets["DATA_PATH"]}/out/{user_folder}/{run_path("whitebox")}',
                "tool_logs": "merge skipped (only one input)",
                "container_status": "skipped",
                "value": "skipped",
                "error": "none",
            }
        else:
            # merge reads from folder of tifs
            logging.info("whitebox_in_host: %s", whitebox_in_host)
            merge_input = {
                "tool_name": "merge_tifs",
                "raster_file": whitebox_in_host,  # host folder containing .tif
                "method": data.get("method", "nn"),
                "to_file": True,
                "User-Info": user_folder,
            }
            _, merge_output = whitebox.execute(merge_input, path=run_path("whitebox", "merge"))

            # merged DEM ends up as dem.tif in WHITEBOX merge out folder
            merge_out_host = merge_output["dir"].replace(secrets["DATA_PATH"], secrets["GEOAPI_PATH"])
            logging.info("merge_out_host: %s", merge_out_host)

            dem_host_path = os.path.join(merge_out_host, "dem.tif")
            logging.info("dem_host_path: %s", dem_host_path)



        target_epsg = int(data.get('target_epsg', 25832))
        cell_size = float(data.get('cell_size', 30.0))
        resampling = str(data.get('resampling', 'bilinear'))
        source_epsg = int(data.get('source_epsg', 4326))

        reproject_input = {
            'tool_name': 'reproject_to_metric',
            'raster_file': dem_host_path,   # merged DEM (host path)
            'target_epsg': target_epsg,
            'cell_size': cell_size,
            'resampling': resampling,
            'source_epsg': source_epsg,
            'to_file': True,
            'User-Info': user_folder
        }

        _, reproject_output = whitebox.execute(reproject_input, path=run_path("whitebox", "reproject"))
        logging.info(f"🗺 reproject_output dir: {reproject_output['dir']}")

        reproject_out_host = reproject_output["dir"].replace(secrets["DATA_PATH"], secrets["GEOAPI_PATH"])
        dem_reprojected_host = os.path.join(reproject_out_host, "dem_reprojected.tif")
        logging.info(f"🗺 reproject_out_host dir: {reproject_out_host}")
        logging.info(f"🗺 dem_reprojected_host dir: {dem_reprojected_host}")

        # --- Hillslope (WHITEBOX/hillslope) ---
        # hillslope_input = {
        #     "tool_name": "hillslope_generator",
        #     "raster_file": dem_reprojected_host,
        #     "stream_threshold": float(data.get("stream_threshold", 10000.0)),
        #     "to_file": data.get("to_file", True),
        #     "User-Info": user_folder,
        # }
        # _, hillslope_output = whitebox.execute(hillslope_input, path=run_path("whitebox", "hillslope"))

        # --- Hillslope run #1 (LOW threshold) ---
        hillslope_low_input = {
            "tool_name": "hillslope_generator",
            "raster_file": dem_reprojected_host,
            "stream_threshold": float(data.get("stream_threshold_low")),  # <-- low
            "to_file": True,
            "User-Info": user_folder,
        }
        _, hillslope_low_output = whitebox.execute(
            hillslope_low_input,
            path=run_path("whitebox", "hillslope_low")
        )

        # --- Hillslope run #2 (HIGH threshold) ---
        hillslope_high_input = {
            "tool_name": "hillslope_generator",
            "raster_file": dem_reprojected_host,
            "stream_threshold": float(data.get("stream_threshold_high")),  # <-- high
            "to_file": True,
            "User-Info": user_folder,
        }
        _, hillslope_high_output = whitebox.execute(
            hillslope_high_input,
            path=run_path("whitebox", "hillslope_high")
        )

        catflow_in_host = f'{secrets["GEOAPI_PATH"]}/in/{user_folder}/{run_path("catflow")}'
        os.makedirs(catflow_in_host, exist_ok=True)

        low_out_host  = hillslope_low_output["dir"].replace(secrets["DATA_PATH"], secrets["GEOAPI_PATH"])
        high_out_host = hillslope_high_output["dir"].replace(secrets["DATA_PATH"], secrets["GEOAPI_PATH"])

        # Copy ALL .tif files from LOW run into catflow input
        for src in glob.glob(os.path.join(low_out_host, "*.tif")):
            shutil.copy2(src, os.path.join(catflow_in_host, os.path.basename(src)))

        # Overwrite hillslopes.tif from HIGH run
        high_hills = os.path.join(high_out_host, "hillslopes.tif")
        if not os.path.isfile(high_hills):
            raise RuntimeError(f"Expected hillslopes.tif not found in {high_out_host}")
        shutil.copy2(high_hills, os.path.join(catflow_in_host, "hillslopes.tif"))


        # --- CATFLOW in its own folder ---
        # catflow_in_host = hillslope_output["dir"].replace(secrets["DATA_PATH"], secrets["GEOAPI_PATH"])
        logging.info(f"🗺 catflow_in_host dir: {catflow_in_host}")

        catflow = CatflowProcessor({"name": "tool_catflow_v_0_1"})
        catflow_input = {
            "input_dir": catflow_in_host,
            "hillslope_id": data.get("hillslope_id", -1),
            "no_flow_area": data.get("no_flow_area", 0.30),
            "min_cells": data.get("min_cells", 10),
            "hill_type": data.get("hill_type", "constant"),
            "depth": data.get("depth", 2.1),
            "User-Info": user_folder,
        }
        _, catflow_output = catflow.execute(catflow_input, path=run_path("catflow","run"))





        logging.info("📤 catflow_output : %s", catflow_output)
        # --- Collect plot files and split PDFs vs images ---

        plots_pdfs = []
        preview_images = []

        # shared OUT root (HOST path) -> used to build relative paths for the portal
        shared_out_host = f'{secrets["GEOAPI_PATH"]}/out/{user_folder}/{shared_id}'

        # catflow run dir (HOST path) -> where plots are actually created
        catflow_run_host = catflow_output['dir'].replace(secrets["DATA_PATH"], secrets["GEOAPI_PATH"])
        plots_dir_host = os.path.join(catflow_run_host, 'plots')

        if os.path.isdir(plots_dir_host):
            for fname in sorted(os.listdir(plots_dir_host)):
                full = os.path.join(plots_dir_host, fname)
                if not os.path.isfile(full):
                    continue

                # IMPORTANT: path must be relative to shared_out_host because you return dir=shared_out_dir
                rel = os.path.relpath(full, shared_out_host).replace("\\", "/")
                lower = fname.lower()

                if lower.endswith('.pdf'):
                    plots_pdfs.append(rel)
                elif lower.endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg')):
                    preview_images.append(rel)
        else:
            logging.info("ℹ No plots directory found at: %s", plots_dir_host)

        return mimetype, {
            "name" : "DEM2CAT",
            'container_status': catflow_output.get('container_status'),
            'value': catflow_output.get('value'),
            'dir': server_path_out,
            'shared_dir': shared_id,

            'loader_output_dir': loader_output.get('dir'),
            "whitebox_dir": f'{secrets["DATA_PATH"]}/out/{user_folder}/{run_path("whitebox")}',
            "catflow_dir": catflow_output.get("dir"),

            'merge_output_dir': merge_output.get('dir'),
            'reproject_output_dir': reproject_output.get('dir') if len(raster_candidates) > 1 else None,

            "hillslope_low_output_dir": hillslope_low_output.get("dir"),
            "hillslope_high_output_dir": hillslope_high_output.get("dir"),

            'whitebox_logs_merge': merge_output.get('tool_logs', 'n/a'),
            'whitebox_logs_reproject': reproject_output.get('tool_logs', 'n/a') if len(raster_candidates) > 1 else 'n/a',

            "whitebox_logs_hillslope_low": hillslope_low_output.get("tool_logs", "n/a"),
            "whitebox_logs_hillslope_high": hillslope_high_output.get("tool_logs", "n/a"),

            'catflow_logs': catflow_output.get('tool_logs', 'n/a'),
       
            "stream_threshold_low": float(data.get("stream_threshold_low")),
            "stream_threshold_high": float(data.get("stream_threshold_high")),

            'plots': plots_pdfs,
#            'preview_images': catflow_output.get('preview_images', []),
            'preview_images': preview_images,

            'error': catflow_output.get('error'),
        }


    def __repr__(self):
        return '<CombinedLoaderWhiteboxProcessor>'