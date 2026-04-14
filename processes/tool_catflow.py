import logging
import json
import shutil
import os
from pygeoapi.process.base import BaseProcessor
from processes.podman_processor import PodmanProcessor
#from pdf2image import convert_from_path, pdfinfo_from_path

import subprocess
import sys
import multiprocessing

# Attempt to install pdf2image + pillow if missing
try:
    from pdf2image import convert_from_path, pdfinfo_from_path
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pdf2image", "pillow"])
    from pdf2image import convert_from_path



PROCESS_METADATA = {
    'version': '0.1.1',
    'id': 'tool_catflow',
    'title': {
        'en': 'Catflow Tool',
        'de': 'Catflow Werkzeug'
    },
    'description': {
        'en': 'EExecutes the make_representative_hillslope process of the Catflow hydrological model. Supports optional soil raster.',
        'de': 'Führt den Prozess make_representative_hillslope des hydrologischen Modells Catflow aus. Unterstützt optionalen Boden-Raster.'
    },
    'keywords': ['catflow', 'hydrology', 'representative hillslope', 'raster', 'watershed'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'Information',
        'href': 'https://github.com/VForWaTer/tool_catflow',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'input_dir': {
            'title': 'Input Folder',
            'description': 'Path to a folder containing input.json and all required input files.',
            'schema': {
                'type': 'string',
                'example': '/in/test_catflow_inputs'
            }
        }
    },
    'outputs': {
        'result': {
            'title': 'Output directory',
            'description': 'Directory with output files',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json',
            },
            'example': {
                'value': 'completed',
                'container_status': 'exited'
            }
        }
    },
    'example': {
        'inputs': {
            'input_dir': '/in/test_catflow_inputs'
        }
    }
}

class CatflowProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data, path=None):
        mimetype = 'application/json'
        if path is None:
            path = f'catflow_{os.urandom(5).hex()}'
        user = data.get('User-Info', 'default')

        input_dir = data.get('input_dir')

        if not input_dir or not os.path.isdir(input_dir):
            raise ValueError("Missing or invalid 'input_dir' path")

        logging.info(f"📤 input_folder from catflow: {input_dir}")

        secrets = PodmanProcessor.get_secrets()
        host_path_in = f'{secrets["GEOAPI_PATH"]}/in/{user}/{path}'
        host_path_out = f'{secrets["GEOAPI_PATH"]}/out/{user}/{path}'
        server_path_out = f'{secrets["DATA_PATH"]}/out/{user}/{path}'
        server_path_in = f'{secrets["DATA_PATH"]}/in/{user}/{path}'

        os.makedirs(host_path_in, exist_ok=True)
        os.makedirs(host_path_out, exist_ok=True)
        os.makedirs(server_path_in, exist_ok=True)
        os.makedirs(server_path_out, exist_ok=True)

        logging.info(f"📤 Catflow input directory: {input_dir}")

        # Read parameters
        hillslope_id = data.get('hillslope_id', -1)
        no_flow_area = data.get('no_flow_area', 0.30)
        min_cells = data.get('min_cells', 10)
        hill_type = data.get('hill_type', 'constant')
        depth = data.get('depth', 2.1)

        min_area = data.get('min_area', 10000)         # m²
        freedom = data.get('freedom', 10)              # spline DoF
        constant_width = data.get('constant_width', True)

        # Assemble expected input file paths
        file_map = {
            'flow_accumulation': 'flow_accumulation.tif',
            'hillslopes': 'hillslopes.tif',
            'elev2river': 'elevation.tif',
            'dist2river': 'distance.tif',
            'filled_dem': 'fill_DEM.tif',
            'aspect': 'aspect.tif',
            'river_id': 'streams.tif',
            # optional:
            'soil': 'soils.tif'
        }
        input_data_paths = {k: os.path.join('/in', v) for k, v in file_map.items()}

        input_dict = {
            "make_representative_hillslope": {
                "parameters": {
                    "hillslope_id": hillslope_id,
                    "no_flow_area": no_flow_area,
                    "min_cells": min_cells,
                    "hill_type": hill_type,
                    "depth": depth,
                    "min_area": min_area,
                    "freedom": freedom,
                    "constant_width": constant_width
                },
                "data": {k: v for k, v in input_data_paths.items() if k != 'soil'}
            },
            "define_run_printouts": {
                "parameters": {
                    "start.time": "01.01.2008 00:00:00",
                    "end.time": "31.12.2015 00:00:00",
                    "interval": 12,
                    "time.unit": "hourly",
                    "flag": 1
                },
                "data": {}
            },
            "write_multipliers": {
                "parameters": {"fac_kst": 1},
                "data": {"geometry": "/in/rep_hill.geo"}
            }
        }

        # If optional soil file exists in the provided input folder, add it to JSON
        soil_src = os.path.join(input_dir, file_map['soil'])
        if os.path.exists(soil_src):
            input_dict["make_representative_hillslope"]["data"]["soil"] = input_data_paths['soil']

#        secrets = PodmanProcessor.get_secrets()
#        host_path_in = f'{secrets["GEOAPI_PATH"]}/in/{user}/{path}'
#        host_path_out = f'{secrets["GEOAPI_PATH"]}/out/{user}/{path}'
#        server_path_out = f'{secrets["DATA_PATH"]}/out/{user}/{path}'

#        os.makedirs(host_path_in, exist_ok=True)
#        os.makedirs(host_path_out, exist_ok=True)



        # Copy all required files to input dir (skip optional soil if absent)
        for key, src_name in file_map.items():
            src_path = os.path.join(input_dir, src_name)
            dst_path = os.path.join(host_path_in, src_name)
            if (key == 'soil') and (not os.path.exists(src_path)):
                logging.info("ℹ Optional soil raster not found, skipping: %s", src_path)
                continue
            shutil.copy(src_path, dst_path)


        #  copy rep_hill.geo if exists
#        rep_hill_path = os.path.join(input_dir, 'rep_hill.geo')
#        if os.path.exists(rep_hill_path):
#            shutil.copy(rep_hill_path, os.path.join(host_path_in, 'rep_hill.geo'))

        # Write input.json
        with open(os.path.join(host_path_in, 'input.json'), 'w') as f:
            json.dump(input_dict, f, indent=4)

        os.system(f'cp -r {input_dir}/* {host_path_in}/')
        logging.info("✅ Copied input folder contents to %s", host_path_in)

        image_name = 'ghcr.io/vforwater/tbr_catflow:v0.9.5.1'
        container_name = f'catflow_tool_{os.urandom(5).hex()}'
        container_in = '/in'
        container_out = '/out'
        mounts = [
            {'type': 'bind', 'source': server_path_in, 'target': container_in, 'read_only': True},
            {'type': 'bind', 'source': server_path_out, 'target': container_out}
        ]

        environment = {'TOOL_RUN': 'make_representative_hillslope'}
        network_mode = 'host'
        command = ["Rscript", "/src/run.R"]

        error = 'none'
        try:
            client = PodmanProcessor.connect(secrets['PODMAN_URI'])
            container = PodmanProcessor.pull_run_image(
                client=client, image_name=image_name,
                container_name=container_name, environment=environment,
                mounts=mounts, network_mode=network_mode,
                command=command)
        except Exception as e:
            logging.error(f'Error running Podman: {e}')
            error = str(e)
            container = None

        status = 'failed'
        tool_logs = 'No logs available'
        if container:
            try:
                container.reload()
                status = container.status
                tool_logs = ''.join(log.decode('utf-8') for log in container.logs())
            except Exception as e:
                logging.error(f'Error getting logs/status: {e}')
                error = f'{error} | Log/Status error: {e}'

        plots_dir = os.path.join(host_path_out, 'plots')
        preview_dir = os.path.join(host_path_out, 'plots_preview')
        os.makedirs(preview_dir, exist_ok=True)
        pdfs = []
        preview_images = []


# #        import shutil
#        logging.info("🔍 pdftoppm found at:", shutil.which("pdftoppm"))
#        logging.info("📁 Full PATH:", os.environ.get("PATH"))
#        poppler_path = shutil.which("pdftoppm")
#        if poppler_path:
#            poppler_dir = os.path.dirname(poppler_path)
#        else:
#            poppler_dir = None  # Or raise a helpful error


#        def safe_convert(pdf_path, output_path):
#            try:
#                pages = convert_from_path(pdf_path, dpi=150, first_page=1, last_page=1, poppler_path="/usr/bin")
#                if pages:
#                    pages[0].save(output_path, 'PNG')
#                    return True
#            except Exception as e:
#                logging.error(f"❌ Error converting {os.path.basename(pdf_path)} to image: {e}")
#            return False

#        def with_timeout(pdf_path, output_path, timeout=60):
#            proc = multiprocessing.Process(target=safe_convert, args=(pdf_path, output_path))
#            proc.start()
#            proc.join(timeout)
#            if proc.is_alive():
#                logging.warning(f"⏳ Conversion timeout for: {pdf_path}")
#                proc.terminate()
#                proc.join()

#        if os.path.exists(plots_dir):
#            for fname in os.listdir(plots_dir):
#                if fname.endswith('.pdf'):
#                    pdfs.append(fname)
#                    pdf_path = os.path.join(plots_dir, fname)
#                    img_path = os.path.join(preview_dir, f"{os.path.splitext(fname)[0]}_page1.png")
#                    with_timeout(pdf_path, img_path, timeout=60)
#                    if os.path.exists(img_path):
#                        preview_images.append(f'plots_preview/{os.path.basename(img_path)}')

        outputs = {
            'container_status': status,
            'value': 'completed' if status == 'exited' else 'failed',
            'dir': server_path_out,
            'error': error,
            'tool_logs': tool_logs,
#            'plots': pdfs,
#            'preview_images': preview_images
        }

        return mimetype, outputs

    def __repr__(self):
        return '<CatflowProcessor>'
