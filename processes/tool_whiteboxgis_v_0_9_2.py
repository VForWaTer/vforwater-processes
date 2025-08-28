import logging
import json
import os
from pygeoapi.process.base import BaseProcessor
from processes.podman_processor import PodmanProcessor

PROCESS_METADATA = {
    'version': '0.9.2.1',
    'id': 'tool_whiteboxgis_v_0_9_2',
    'title': {
        'en': 'Whitebox GIS Tool V0.9.2',
        'de': 'Whitebox GIS Werkzeug'
    },
    'description': {
        'en': 'Runs Whitebox GIS operations like hillslope generation or terrain analysis.',
        'de': 'Führt Whitebox GIS-Operationen wie Hangauswertung oder Terrainanalyse durch.'
    },
    'keywords': ['whitebox', 'gis', 'raster', 'terrain', 'hydrology'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'Information',
        'href': 'https://github.com/VForWaTer/tool_whiteboxgis',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'tool_name': {
            'title': 'Whitebox Tool Name',
            'description': 'Tool to run inside the container (e.g., "hillslope_generator")',
            'schema': {
                'type': 'string',
                'default': 'hillslope_generator',
                'example': 'hillslope_generator'
            }
        },
        'method': {
            'title': 'Merging method',
            'description': "The method to use for merging the TIFF files. Options include 'nn', 'cc' and 'bilinear'",
            'schema': {
                'type': 'string',
                'default': 'nn',
                'example': 'nn'
            }
        },        
        'stream_threshold': {
            'title': 'Stream Threshold',
            'description': 'Threshold value for stream extraction',
            'schema': {
                'type': 'number',
                'default': 100.0,
                'example': 100.0
            }
        },
        'to_file': {
            'title': 'Write Output to File',
            'description': 'Whether the tool should save output to file',
            'schema': {
                'type': 'boolean',
                'default': True,
                'example': True
            }
        },
        'raster_file': {
            'title': 'Input raster file path or folder',
            'description': 'Path to the input DEM GeoTIFF file or folder of files.',
            'schema': {
                'type': 'string',
                'example': '/in/dem.tif'
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
            'tool_name': 'hillslope_generator',
            'raster_file': '/in/dem.tif',
            'stream_threshold': 100.0,
            'to_file': True
        }
    }
}

class WhiteboxGISProcessorV092(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data, path=None):
        mimetype = 'application/json'
        if path is None:
            path = f'whitebox_{os.urandom(5).hex()}'
        user = data.get('User-Info', 'default')

        raster_path = data.get('raster_file')
        logging.info(f"📤 raster_path from whiteboxgis: {raster_path}")
        tool_name = data.get('tool_name', 'hillslope_generator')
        logging.info(f"📤 tool_name from whiteboxgis: {tool_name}")

        stream_threshold = data.get('stream_threshold', 100.0)
        method = data.get('method', 'nn')
        to_file = data.get('to_file', True)
        logging.info(f"📤 to_file from whiteboxgis: {to_file}")

        if not raster_path:
            raise ValueError("Missing required 'raster_file'")

        secrets = PodmanProcessor.get_secrets()
        host_path_in = f'{secrets["GEOAPI_PATH"]}/in/{user}/{path}'
        host_path_out = f'{secrets["GEOAPI_PATH"]}/out/{user}/{path}'
        server_path_out = f'{secrets["DATA_PATH"]}/out/{user}/{path}'

        os.makedirs(host_path_in, exist_ok=True)
        os.makedirs(host_path_out, exist_ok=True)

        if tool_name == "hillslope_generator":
            input_dict = {
                tool_name: {
                    "parameters": {
                        "stream_threshold": stream_threshold,
                        "toFile": to_file
                    },
                    "data": {
                        "dem": "/in/dem.tif"
                    }
                }
            }
            # Copy DEM file
            target_raster_path = os.path.join(host_path_in, "dem.tif")
            os.system(f'cp "{raster_path}" "{target_raster_path}"')
            logging.info("✅ copied %d data to %s", len(raster_path), target_raster_path)

        elif tool_name == "merge_tifs":
            input_dict = {
                tool_name: {
                    "parameters": {
                        "method": method,
                        "toFile": to_file
                    },
                    "data": {
                        "in_file": "/in"
                    }
                }
            }
            # Copy entire folder of rasters
            os.system(f'cp {raster_path}/*.tif {host_path_in}/')
            logging.info("✅ to copy %d data to %s", len(raster_path), host_path_in)

        else:
            raise ValueError(f"Unsupported tool_name: {tool_name}")

        try:
            with open(f'{host_path_in}/input.json', 'w') as f:
                json.dump(input_dict, f, indent=4)
        except Exception as e:
            raise RuntimeError(f"Error writing input.json: {e}")

        image_name = 'ghcr.io/vforwater/tbr_whitebox:v0.9.2.1'
        container_name = f'whiteboxgis_tool_{os.urandom(5).hex()}'
        container_in = '/in'
        container_out = '/out'
        mounts = [
            {'type': 'bind', 'source': f'/data/geoapi_data/in/{user}/{path}', 'target': container_in, 'read_only': True},
            {'type': 'bind', 'source': f'/data/geoapi_data/out/{user}/{path}', 'target': container_out}
        ]

        environment = {'TOOL_RUN': tool_name}
        network_mode = 'host'
        command = ["python", "/src/run.py"]

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

        outputs = {
            'container_status': status,
            'value': 'completed' if status == 'exited' else 'failed',
            'dir': server_path_out,
            'error': error,
            'tool_logs': tool_logs
        }

        return mimetype, outputs

    def __repr__(self):
        return '<WhiteboxGISProcessorV092>'
