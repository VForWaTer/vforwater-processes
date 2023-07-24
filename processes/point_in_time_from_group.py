from datetime import datetime
import logging
import json
from pathlib import Path

import numpy as np
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import os
import pandas as pd
import polars as pl
from pyproj import CRS, Transformer, Proj, transform
from toolbox_runner import list_tools
from toolbox_runner.run import get_remote_image_list

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'point_in_time_from_group',
    'title': {
        'en': 'Point In Time From Group',
        'de': 'Zeitpunkt aus einer Gruppe'
    },
    'description': {
        'en': 'Extract a single point in time from a group of timeseries. '
              'Creates a second dataset with metric coords (srid 3857).',
        'de': 'Einen einzelnen Zeitpunkt aus einer Gruppe von Zeitreihen extrahieren. '
              'Es wird ein zweiter Datensatz mit metrischen Koordinaten erstellt (srid 3857).',
    },
    'keywords': ['extract', 'timeseries', 'group', 'HTML', 'JSON'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'timeseries': {  # TODO
            'title': 'Timeseries',
            'description': 'Array of IDs of group data of timeseries with an overlap in time.',
            'schema': {
                'type': 'array',
                'required': 'true'
            },
            'minOccurs': 1,  # expect the data is needed
            'maxOccurs': 1,
            # 'metadata': None,  # TODO how to use?
            # 'keywords': ['full name', 'personal']
        },
        'timestamp': {
            'title': 'Point in time',
            'description': 'The point in time to extract. Undefined timesteps are linearly interpolated from their'
                           'neighbours.',
            'schema': {
                'type': 'dateTime',
                'required': 'true'
            },
            'minOccurs': 1,  # expect the data is needed
            'maxOccurs': 1,
        },
        'interpolation': {
            'title': 'Interpolation method',
            'description': 'Method used to interpolate values if the exact point in time is not available in a '
                           'dataset.',
            'schema': {
                'type': 'string',
                'enum': ['Linear', 'Nearest Neighbour', 'None (dismiss value)'],
                'default': 'Linear',
            }
        }
    },
    'outputs': {
        'res': {
            'title': 'result path',
            'description': 'output path',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            # 'df': 'World',
            'coordinates': 'coords',  # how to example data
            'values': 'vals',  # how to example data
            'maxlag': 'median',  # adjust to possible input
            'model': 'exponential',  # adjust to possible input
            'bin_func': 'scott'  # adjust to possible input
        }
    }
}


class PointInTimeFromGroup(BaseProcessor):
    """PointInTimeFromGroup Processor"""

    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.variogram.VariogramProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        print('In excecute')
        print('Data: ', data)
        fullpaths = {}  # Path(f'{PROCESSES_IN_DIR}/{folder}/')

        dataset_values = []
        dataset_precisions = []
        x_coords = []
        y_coords = []
        sources = []

        in_dir_base = '/home/geoapi/in/'

        mimetype = 'application/json'

        # interpolation_map = {'Next Neighbour': 'nn', 'Linear': 'linear', 'None (dismiss value)': None}
        interpolation_map = {'Next Neighbour': 'nn', 'Nearest Neighbour': 'nn', 'Linear': 'linear',
                             'None (dismiss value)': None}
        # collect inputs
        datasets = data.get('timeseries')
        print('datasets: ', datasets)
        time_string = data.get('timestamp')
        print('timestring: ', time_string)
        time_obj = datetime.strptime(time_string, '%Y-%m-%dT%H:%M')
        pd_time_obj = pd.to_datetime(time_string, format='%Y-%m-%dT%H:%M')
        test_time_obj = datetime.strptime("1902-01-01T01:10", '%Y-%m-%dT%H:%M')
        print('time obj: ', time_obj)
        print('interpolation 1: ', data.get('interpolation'))
        interpolation = interpolation_map[data.get('interpolation')]
        print('interpolation: ', interpolation)

        for i in datasets:
            fullpaths[i] = Path(f'{in_dir_base}/{i}/')
            print('fullpaths: ', fullpaths)
            try:
                # data = pd.read_csv(Path(f'{in_dir_base}/{i}/dataframe.csv'))
                data = pl.read_csv(Path(f'{in_dir_base}/{i}/dataframe.csv'), try_parse_dates=True).to_pandas()
                print('data["tstamp"]: ', data["tstamp"])
                # data["tstamp"] = data["tstamp"].astype('datetime64[ns]')  # this should be faster than "to_datetime"
                # print('data["tstamp"] 2: ', data["tstamp"])

            except Exception as e:
                print('data e: ', e)

            # if data.loc[data['tstamp'].searchsorted(test_time_obj)]['tstamp'] == test_time_obj:
            if data.loc[data['tstamp'].searchsorted(time_obj)]['tstamp'] == time_obj:
                dataset_values.append(data.loc[data['tstamp'].searchsorted(time_obj)]['value'])
                dataset_precisions.append(data.loc[data['tstamp'].searchsorted(time_obj)]['precision'])
            elif interpolation is None:
                pass
            elif interpolation == "nn":  # come here if you have to interpolate
                if data.loc[data['tstamp'].searchsorted(time_obj)]['tstamp'] - time_obj < \
                    data.loc[data['tstamp'].searchsorted(time_obj) - 1]['tstamp'] - time_obj:
                    nearest_row = data.loc[data['tstamp'].searchsorted(time_obj)]
                else:
                    nearest_row = data.loc[data['tstamp'].searchsorted(time_obj)]
                dataset_values.append(nearest_row['value'])
                dataset_precisions.append(nearest_row['precision'])
            elif interpolation == "linear":
                lower_tstamp = data.loc[data['tstamp'].searchsorted(time_obj) - 1]
                greater_tstamp = data.loc[data['tstamp'].searchsorted(time_obj)]
                interval = greater_tstamp - lower_tstamp
                df = pd.DataFrame([lower_tstamp,
                                   pd.Series(data={'tstamp': pd_time_obj, 'value': np.nan, 'precision': np.nan},
                                             index=['tstamp', 'value', 'precision']),
                                   greater_tstamp])
                # df = pd.DataFrame([lower_tstamp, ('tstamp', time_obj, greater_tstamp])
                # low_dist =
            else:
                raise ProcessorExecuteError('interpolation method not allowed')

            try:
                f = open(f'{in_dir_base}/{i}/dataframe.json')
                metadata = json.load(f)

                coords = metadata['coordinates']
                srid = metadata['srid']
                type = metadata['type']

                from_crs = CRS(f"epsg:{srid}")  # or whatever you want
                transformer = Transformer.from_crs(from_crs, CRS("epsg:3857"))

                new_coords = transformer.transform(coords[1], coords[0])
                print('new_coords: ', new_coords)

                x_coords.append(new_coords[0])
                y_coords.append(new_coords[1])

                sources.append(metadata)

            except Exception as e:
                print('meta e: ', e)

            # from_crs = CRS("epsg:4326")  # or whatever you want
            # transformer = Transformer.from_crs(from_crs, CRS("epsg:3857"))

        # here you could check if required files are given and check format
        if coords is None or datasets is None or time_string is None:
            raise ProcessorExecuteError('Cannot process without a dataset')

        # path = os.path.dirname(values)

        # in_dir = '/home/geoapi/in/' + path
        out_dir = f"/home/geoapi/out/{self.metadata['id']}"  # new in dir for next process

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        metadata = {
            "variogram": {  # "data": "/in/dataframe.csv"
                "source": "",
                "coords": f"/in/{coords}",
                "values": f"/in/{values}",
                "n_lags": n_lags,
                "use_nugget": use_nugget,
                "fit_range": fit_range,
                "fit_sill": fit_sill,
                "fit_nugget": fit_nugget,
                "fit_sigma": fit_sigma,
            }
        }

        with open(in_dir + '/parameters.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)

        # df.to_csv(in_dir+'dataframe.csv')s

        res = 'completed'

        # tools = list_tools('ghcr', as_dict=True)
        # prof = tools.get('profile')
        # dataset = pd.read_csv(df)
        # res = prof.run(result_path='out/', data=dataset)

        outputs = {
            'id': 'res',
            'value': res,
            'dir': out_dir
        }

        return mimetype, outputs

    def __repr__(self):
        return '<VariogramProcessor> completed!'
