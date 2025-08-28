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
        'stream_threshold': {
            'title': 'Stream Threshold',
            'schema': {'type': 'number', 'default': 100.0},
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'to_file': {
            'title': 'Save to File',
            'schema': {'type': 'boolean', 'default': True},
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


        loader = VforwaterLoaderProcessor({"name": "vforwater_loader"})
        _, loader_output = loader.execute(data, path=shared_id)
        if isinstance(loader_output, str):
            loader_output = json.loads(loader_output)


        logging.info("📥 Using input path: %s", host_in_path)
        print("📥 Using input path: %s", host_in_path)
        logging.info("📤 Using output path: %s", host_out_path)
        print("📤 Using output path: %s", host_out_path)

        logging.info("📥 Using server input path: %s", server_path_in)
        print("📥 Using server input path: %s", server_path_in)
        logging.info("📤 Using server output path: %s", server_path_out)
        print("📤 Using server output path: %s", server_path_out)


        expected_raster_folder = os.path.join(loader_output.get("dir"), 'datasets', 'elevation_1100')
        logging.info("📤 Using expected_raster_folder path: %s", expected_raster_folder)
        # /data/geoapi_data/out/user1580_safa/testfolder/datasets/elevation_1100
        print("📤 Using expected_raster_folder path: %s", expected_raster_folder)

        expected_raster_folder_host = os.path.join(host_out_path, 'datasets', 'elevation_1100')
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
        raster_candidates = glob.glob(os.path.join(expected_raster_folder, '*.tif'))
        logging.info("✅ to copy %d data to %s", len(raster_candidates), host_in_path)
        # copied 0 to /home/geoapi/in/user1580_safa/testfolder
        print("✅ to copy %d data to %s", len(raster_candidates), host_in_path)

        for tif in raster_candidates:
            shutil.copy(tif, host_in_path)
        #    if len( os.listdir(raster_candidates)) == 1            
         #       hillslop_in = shutil.copy(tif,  os.path.join(host_in_path, 'dem.tif')
        #logging.info(f" hillslop input %s", hillslop_in)
        
        # logging.info("✅ Copied %d rasters to %s", len(raster_candidates), host_in_path)
        logging.info("✅ Copied %d data to %s", len(raster_candidates), host_in_path)

        whitebox = WhiteboxGISProcessorV092({"name": "tool_whiteboxgis_v_0_9_2"})

        merge_input = {
            'tool_name': 'merge_tifs',
            'raster_file': expected_raster_folder_host,
            'method': data.get('method', 'nn'),
            'to_file': True,
            'User-Info': user_folder
        }

        _, merge_output = whitebox.execute(merge_input, path=shared_id)
        logging.info(f"🔍 merge_output dir: {merge_output['dir']}")
        # INFO - 🔍 merge_output dir: /data/geoapi_data/out/user1580_safa/testfolder
        dem_path = os.path.join(merge_output['dir'], 'dem.tif')



        #if not os.path.exists(dem_path):
        #    raise RuntimeError(f"❌ Merged DEM not found at: {dem_path}")
        dem_container_dir = merge_output['dir']
        logging.info(f"🔍 dem_container_dir: {dem_container_dir}")
        # INFO - 🔍 dem_container_dir: /data/geoapi_data/out/user1580_safa/testfolder

        dem_host_dir = dem_container_dir.replace('/data/geoapi_data', '/home/geoapi')
        logging.info(f"🔍 dem_host_dir: {dem_host_dir}")
        #INFO - 🔍 dem_host_dir: /home/geoapi/out/user1580_safa/testfolder
        # expected_dem_dir = dem_container_dir.replace( '/data/geoapi_data', '/home/geoapi')
        expected_dem_file =  os.path.join(dem_host_dir, 'dem.tif')
#        expected_dem_file =  os.path.join(dem_host_dir, 'elevation_1100_part_1.tif')


        logging.info(f"🔍 expected_dem_dir: {dem_host_dir}")

        #expected_dem_file_in =  shutil.copy(expected_dem_file, host_in_path)
        #logging.info(f"🔍 expected_dem_file_in: {expected_dem_file_in}")

        logging.info(f"🔍 expected_dem_file: {expected_dem_file}")

        # dem_path = os.path.join(dem_host_dir, 'dem.tif')
        # logging.info(f"🔍 dem_path: {dem_path}")
        # INFO - 🔍 dem_path: /data/geoapi_data/out/user1580_safa/testfolder/merged.tif

#        logging.info(f"🔍  merge_output['dir'] after merge: {os.listdir(merge_output['dir'])}")
 #       raster_candidates_merged = glob.glob(os.path.join(merge_output['dir'], '*.tif'))
 #       logging.info("✅ to copy %d data to %s", len(raster_candidates_merged),raster_candidates_merged, host_in_path)
        logging.info("📤 Using expected_raster_folder_host  path.. input of merge: %s", expected_raster_folder_host)
#        logging.info(f"🔍  merge_output['dir'] after merge: {os.listdir('/data/geoapi_data/in/user1580_safa/combined_194e56925a')}")

        hillslope_input = {
            'tool_name': 'hillslope_generator',
            'raster_file': expected_dem_file,
            'stream_threshold': float(data.get('stream_threshold', 100.0)),
            'to_file': data.get('to_file', True),
            'User-Info': user_folder
        }

        _, hillslope_output = whitebox.execute(hillslope_input, path=shared_id)




        # Run Catflow tool with hillslope output directory as input
        #catflow = CatflowProcessor({"name": "tool_catflow_v_0_1"})

        catlflow_host_dir = hillslope_output['dir'].replace('/data/geoapi_data', '/home/geoapi')
        logging.info("📤 Using catlflow_host_dir input  path..: %s", catlflow_host_dir)



        # Validate and rename files for Catflow
        expected_files = {
            'flow_accumulation.tif', 'hillslopes.tif', 'elevation.tif', 'distance.tif',
            'fill_DEM.tif', 'aspect.tif', 'streams.tif'
        }
#        available_files = set(os.listdir(hillslope_output['dir']))
#        logging.info("📤 check available_files for catflow input  path..: %s", available_files)
        logging.info("📤 check expected_files for catflow input  path..: %s", expected_files)

#        missing = expected_files - available_files
#        if missing:
#            raise RuntimeError(f"Missing files for Catflow: {', '.join(missing)}")

        # Run Catflow
        catflow = CatflowProcessor({"name": "tool_catflow_v_0_1"})
        catflow_input = {
            'input_dir': catlflow_host_dir,
            'hillslope_id': data.get('hillslope_id', -1),
            'no_flow_area': data.get('no_flow_area', 0.30),
            'min_cells': data.get('min_cells', 10),
            'hill_type': data.get('hill_type', 'constant'),
            'depth': data.get('depth', 2.1),
            'User-Info': user_folder
        }
        _, catflow_output = catflow.execute(catflow_input, path=shared_id)

        return mimetype, {
            'container_status': catflow_output.get('container_status'),
            'value': catflow_output.get('value'),
            'dir': catflow_output.get('dir'),
            'shared_dir': shared_id,
            'loader_output_dir': loader_output.get('dir'),
            'merge_output_dir': merge_output.get('dir'),
            'whitebox_logs_merge': merge_output.get('tool_logs', 'n/a'),
            'whitebox_logs_hillslope': hillslope_output.get('tool_logs', 'n/a'),
            'catflow_logs': catflow_output.get('tool_logs', 'n/a'),
            'plots': catflow_output.get('plots', []),
#            'preview_images': catflow_output.get('preview_images', []),
            'preview_images':  ["./test_preview-1.png" ],
            'error': catflow_output.get('error'),
            'name': 'DEM2CAT'
        }


#        return mimetype, {
#            'container_status': hillslope_output.get('container_status'),
#            'value': hillslope_output.get('value'),
#            'dir': hillslope_output.get('dir'),
#            'shared_dir': shared_id,
#            'loader_output_dir': loader_output.get('dir'),
#            'merge_output_dir': merge_output.get('dir'),
#            'whitebox_logs_merge': merge_output.get('tool_logs', 'n/a'),
#            'whitebox_logs_hillslope': hillslope_output.get('tool_logs', 'n/a'),
#            'error': hillslope_output.get('error'),
#            'name': 'combined_loader_whitebox'
#        }
    
        # return mimetype, {
        #     'container_status': merge_output.get('container_status'),
        #     'value': merge_output.get('value'),
        #     'dir': merge_output.get('dir'),
        #     'shared_dir': shared_id,
        #     'loader_output_dir': loader_output.get('dir'),
        #     'merge_output_dir': merge_output.get('dir'),
        #     'whitebox_logs_merge': merge_output.get('tool_logs', 'n/a'),
        #     # 'whitebox_logs_hillslope': hillslope_output.get('tool_logs', 'n/a'),
        #     'error_merge': merge_output.get('error'),
        #     # 'error_hillslope': hillslope_output.get('error'),
        #     'name': 'combined_loader_whitebox'
        # }

    def __repr__(self):
        return '<CombinedLoaderWhiteboxProcessor>'
