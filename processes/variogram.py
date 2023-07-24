import logging
import json

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import os
import pandas as pd
from toolbox_runner import list_tools
from toolbox_runner.run import get_remote_image_list

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.3',
    'id': 'variogram',
    'title': {
        'en': 'Variogram fitting',
        'de': 'Variogram Anpassung'
    },
    'description': {
        'en': 'Estimate an empirical variogram and fit a model.',
        'de': 'Schätzung eines empirischen Variogramms und Anpassung eines Modells.',
    },
    'keywords': ['variogram', 'HTML', 'JSON'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'coordinates': {  # TODO
            'title': 'Coordinates',
            'description': 'Array of shape (m, n). Will be used as m observation points of n-dimensions. This '
                           'variogram can be calculated on 1 - n dimensional coordinates. In case a 1-dimensional '
                           'array is passed, a second array of same length containing only zeros will be stacked to '
                           'the passed one. For very large datasets, you can set maxlag to only calculate distances '
                           'within the maximum lag in a sparse matrix. Alternatively you can supply a MetricSpace '
                           '(optionally with a max_dist set for the same effect). This is useful if you’re creating '
                           'many different variograms for different measured parameters that are all measured at the '
                           'same set of coordinates, as distances will only be calculated once, instead of once per '
                           'variogram.'
                           '[Pass either a (N, D) shaped numpy array, or a .mat file containing the matrix of '
                           'observation location coordinates].'
                           'e.g.: a numpy.ndarray like array([[181072, 333611], [181025, 333558], ... ])',
            'schema': {
                'type': 'ndarray, MetricSpace',
                'required': 'true'
            },
            'minOccurs': 1,  # expect the data is needed
            'maxOccurs': 1,
            # 'metadata': None,  # TODO how to use?
            # 'keywords': ['full name', 'personal']
        },
        'values': {
            'title': 'Values',
            'description': 'Array of values observed at the given coordinates. The length of the values array has to '
                           'match the m dimension of the coordinates array. Will be used to calculate the dependent '
                           'variable of the variogram. If the values are of shape (n_samples, 2), a cross-variogram '
                           'will be calculated. This assumes the main variable and the co-variable to be co-located '
                           'under Markov-model 1 assumptions, meaning the variable need to be conditionally '
                           'independent.'
                           '[Pass either a (N, 1) shaped numpy array or a .mat file containing the matrix of '
                           'observations].'
                           'e.g.: a numpy.ndarray like array([299, 277, ... ]), same lengths as coods',
            'schema': {
                'type': 'ndarray',
                'required': 'true'
            },
            'minOccurs': 1,  # expect the data is needed
            'maxOccurs': 1,
        },
        'n_lags': {
            'title': 'n lags',
            'description': 'Specify the number of lag classes to be defined by the binning function.',
            'schema': {
                'type': 'number',
                'format': 'integer',
                'minimum': 3,
                'default': 10,
                'required': 'true'
            },
        },
        'bin_func': {
            'title': 'bin Function',
            'description': 'The binning function used to find lag class edges. All methods '
                           'calculate bin edges on the interval [0, maxlag[. Possible values are: '
                           '’even’ (default) finds n_lags same width bins '
                           '’uniform’ forms n_lags bins of same data count '
                           'Freedman-Diaconis estimator to find n_lags '
                           # '’fd’ applies Freedman-Diaconis estimator to find n_lags '
                           'Sturge’s rule to find n_lags.'
                           # '’sturges’ applies Sturge’s rule to find n_lags.'
                           'Scott’s rule to find n_lags '
                           # '’scott’ applies Scott’s rule to find n_lags '
                           'Doane’s extension to Sturge’s rule to find n_lags '
                           # '’doane’ applies Doane’s extension to Sturge’s rule to find n_lags '
                           'the square-root of distance as n_lags. '
                           # '’sqrt’ uses the square-root of distance as n_lags. '
                           'KMeans clustering to well supported bins'
                           # '’kmeans’ uses KMeans clustering to well supported bins'
                           'hierarchical clustering to find minimum-variance clusters. '
                           # '’ward’ uses hierarchical clustering to find minimum-variance clusters. '
                           'More details are given in the documentation for set_bin_func.',
            'schema': {
                'type': 'string',  # 'enum'
                'enum': ['Even', 'Uniform', 'Freedman-Diaconis estimator', 'Sturge’s rule', 'Scott’s rule',
                         'Doane’s extension', 'Square-root of distance', 'KMeans clustering',
                         'Hierarchical clustering'],
                'default': 'Even',
                'required': 'true'
            },
        },
        'model': {
            'title': 'Model',
            'description': 'The theoretical variogram function to be used to describe the '
                           'experimental variogram. Can be one of: ’Spherical’ (default), ’Exponential’, ’Gaussian’, '
                           '’Cubic’, ’Stable model’, ’Matérn model’, ’Nugget effect variogram’',
                           # 'experimental variogram. Can be one of: spherical [Spherical, default], '
                           # 'exponential [Exponential], gaussian [Gaussian], cubic [Cubic], stable [Stable model], '
                           # 'matern [Matérn model], nugget [nugget effect variogram]',
            'schema': {
                'type': 'string',  # 'enum'
                'enum': ['Spherical', 'Exponential', 'Gaussian', 'Cubic', 'Stable model', 'Matérn model',
                         'Nugget effect variogram'],
                'default': 'Spherical',
                'required': 'true'
            },
        },
        'estimator': {
            'title': 'Estimator',
            'description': 'The semi-variance estimator to be used. Possible values are: '
                           '’Matheron estimator’ (default), ’Cressie-Hawkins’, ’Dowd-Estimator’, ’Genton’, ’MinMax’, '
                           '’Scaler’, ’Shannon Entropy’. If a callable is passed, it has to accept an array of '
                           'absolute differences, aligned to the 1D distance matrix (flattened upper triangle) and '
                           'return a scalar, that converges towards small values for similarity (high covariance).',
                           # 'String identifying the semi-variance estimator to be used. Defaults to the Matheron '
                           # 'estimator. Possible values are: matheron [Matheron, default] cressie, [Cressie-Hawkins],'
                           # 'dowd [Dowd-Estimator], genton [Genton], minmax [MinMax Scaler], entropy [Shannon Entropy],'
                           # 'If a callable is passed, it has to accept an array of absolute differences, aligned to '
                           # 'the 1D distance matrix (flattened upper triangle) and return a scalar, that converges '
                           # 'towards small values for similarity (high covariance).',
            'schema': {
                'type': 'string',  # 'enum'
                'enum': ['Matheron estimator', 'Cressie-Hawkins', 'Dowd-Estimator',
                         'Genton', 'MinMax Scaler',
                         'Shannon Entropy'
                         ],
                'default': 'Matheron estimator',
                'required': 'true'
            },
        },
        'maxlag': {  # TODO: define a regex
            'title': 'Maxlag',
            'description': 'Can be "median", "mean", a number < 1 for a ratio of maximum separating distance or a '
                           'number > 1 for an absolute distance.'
                           'Can specify the maximum lag distance directly by giving a value larger than 1. The '
                           'binning function will not find any lag class with an edge larger than maxlag. '
                           'If 0 < maxlag < 1, then maxlag is relative and maxlag * max(Variogram.distance) will be '
                           'used. In case maxlag is a string it has to be one of ‘median’, ‘mean’. Then the median or '
                           'mean of all Variogram.distance will be used. Note maxlag=0.5 will use half the maximum '
                           'separating distance, this is not the same as ‘median’, which is the median of all '
                           'separating distances',
            'schema': {
                'type': 'float, string',  # 'string'
                'enum': ['median', 'mean']
                # optional: true
            },
        },
        'fit_method': {
            'title': 'Fit Method',
            'description': 'Method to be used for fitting the theoretical variogram function '
                           'to the experimental. If None is passed, the fit does not run. More info is given in the '
                           'Variogram.fit docs. Can be one of:'
                           '’Levenberg-Marquardt algorithm’ for unconstrained problems. This is the faster algorithm, '
                           'yet is the fitting of a variogram not unconstrianed.'
                           '’Trust Region Reflective’ function (default) for non-linear constrained problems. The '
                           'class will set the boundaries itself.'
                           '’Maximum-Likelihood estimation’ With the current implementation only the Nelder-Mead '
                           'solver for unconstrained problems is implemented. This will estimate the variogram '
                           'parameters from a Gaussian parameter space by minimizing the negative log-likelihood.'
                           '’Manual fitting’ You can set the range, sill and nugget either directly to the '
                           'fit function, or as fit_ prefixed keyword arguments on Variogram instantiation.',
                           # '’lm’: Levenberg-Marquardt algorithm for unconstrained problems. This is the faster '
                           # 'algorithm, yet is the fitting of a variogram not unconstrianed.'
                           # '’trf’: Trust Region Reflective function for non-linear constrained problems. The class '
                           # 'will set the boundaries itself. This is the default function.'
                           # '’ml’: Maximum-Likelihood estimation. With the current implementation only the Nelder-Mead '
                           # 'solver for unconstrained problems is implemented. This will estimate the variogram '
                           # 'parameters from a Gaussian parameter space by minimizing the negative log-likelihood.'
                           # '’manual’: Manual fitting. You can set the range, sill and nugget either directly to the '
                           # 'fit function, or as fit_ prefixed keyword arguments on Variogram instantiation.',
            'schema': {
                'type': 'string',  # 'enum'
                'enum': ['Levenberg-Marquardt algorithm', 'Trust Region Reflective', 'Maximum-Likelihood estimation',
                         'Manual fitting'],  # TODO: need action for manual, how to define this?
                'default': 'Trust Region Reflective',
                'required': 'true'
            },
            # 'minOccurs': 1,  # expect the fit_method is mandatory. TODO: mandadory!
        },
        'use_nugget': {
            'title': 'Use nugget',
            'description': 'Defaults to False. If True, a nugget effect will be added to all Variogram models as a '
                           'third (or fourth) fitting parameter. A nugget is essentially the y-axis interception of '
                           'the theoretical variogram function.',
            'schema': {
                'type': 'boolean',
                'default': 'false',  # False sets the nugget parameter to 0
                'required': 'true'
            },
        },
        'fit_range': {  # TODO: indicator to connect to fit_method
            'title': 'Fit range',
            'description': 'The variogram effective range. Only valid if fit_method="manual".',
            'schema': {
                'type': 'number',
                'format': 'float',
                # optional: true
            },
        },
        'fit_sill': {  # TODO: indicator to connect to fit_method
            'title': 'Fit sill',
            'description': 'The variogram sill. Only valid if fit_method="manual".',
            'schema': {
                'type': 'number',
                'format': 'float'
                # optional: true
            },
        },
        'fit_nugget': {  # TODO: indicator to connect to fit_method
            'title': 'Fit nugget',
            'description': 'The variogram nugget. Only valid if fit_method="manual".',
            'schema': {
                'type': 'number',
                'format': 'float'
                # optional: true
            },
        },
        'fit_sigma': {
            'title': 'Fit sigma',
            'description': 'The sigma is used as measure of uncertainty during variogram fit. If '
                           'fit_sigma is an array, it has to hold n_lags elements, giving the uncertainty for all '
                           'lags classes. If fit_sigma is None (default), it will give no weight to any lag. Higher '
                           'values indicate higher uncertainty and will lower the influence of the corresponding '
                           'lag class for the fit. If `Fit sigma` is a string, a pre-defined function of separating '
                           'distance will be used to fill the array. Can be one of: '
                           '’linear loss with distance’. Small bins will have higher impact. '
                           '’Exponential decrease’: The weights decrease by a e-function of distance '
                           '’Square Root of distance decrease’: The weights decrease by the square root of distance '
                           '’Squared distance decrease’: The weights decrease by the squared distance. '
                           'More info is given in the Variogram.fit_sigma documentation.',
            'schema': {
                'type': 'ndarray, string',  # 'enum'
                'enum': ['None', 'Linear loss with distance', 'Exponential decrease',
                         'Square Root of distance decrease', 'Squared distance decrease'],
                'default': 'None'
            },
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


class VariogramProcessor(BaseProcessor):
    """variogram Processor"""

    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.variogram.VariogramProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):

        mimetype = 'application/json'

        # load all images (podman images!)
        images = get_remote_image_list()

        # dictionaries to map inputs to options of tool
        bin_func_map = {'Even': 'even', 'Uniform': 'uniform', 'Freedman-Diaconis estimator': 'fd',
                        'Sturge’s rule': 'sturges', 'Scott’s rule': 'scott', 'Doane’s extension': 'doane',
                        'Square-root of distance': 'sqrt', 'KMeans clustering': 'kmeans',
                        'Hierarchical clustering': 'ward'}
        model_map = {'Spherical': 'spherical', 'Exponential': 'exponential', 'Gaussian': 'gaussian', 'Cubic': 'cubic',
                     'Stable model': 'stable', 'Matérn model': 'matern', 'Nugget effect variogram': 'nugget'}
        estimator_map = {'Matheron estimator': 'matheron', 'Cressie-Hawkins': 'cressie', 'Dowd-Estimator': 'dowd',
                         'Genton': 'genton', 'MinMax Scaler': 'minmax', 'Shannon Entropy': 'entropy'}
        fit_method_map = {'Levenberg-Marquardt algorithm': 'lm', 'Trust Region Reflective': 'trf',
                          'Maximum-Likelihood estimation': 'ml', 'Manual fitting': 'manual'}
        fit_sigma_map = {'Linear loss with distance': 'linear', 'Exponential decrease': 'esp',
                         'Square Root of distance decrease': 'sqrt', 'Squared distance decrease': 'sq', 'None': 'None'}

        # collect inputs
        coords = data.get('coordinates')  # path/name to numpy.ndarray
        values = data.get('values')  # path/name to numpy.ndarray
        n_lags = data.get('n_lags')  # integer
        bin_func = bin_func_map[data.get('bin_func')]  # string
        model = model_map[data.get('model')]  # string
        estimator = estimator_map[data.get('estimator')]  # string
        maxlag = data.get('maxlag')  # float or string ['median', 'mean']
        fit_method = fit_method_map[data.get('fit_method')]  # string
        use_nugget = data.get('use_nugget')  # boolean
        fit_range = data.get('fit_range')  # float
        fit_sill = data.get('fit_sill')  # float
        fit_nugget = data.get('fit_nugget')  # float
        fit_sigma = data.get('fit_sigma')  # # string or array
        # fit_sigma = fit_sigma_map[data.get('fit_sigma')]  # float or array

        # here you could check if required files are given and check format
        if coords is None or values is None or n_lags is None or bin_func is None or model is None \
            or estimator is None or fit_method is None or use_nugget is None:
            raise ProcessorExecuteError('Cannot process without a dataset')

        path = os.path.dirname(values)

        in_dir = '/home/geoapi/in/' + path
        out_dir = '/home/geoapi/out/' + path

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        metadata = {
            "variogram": {  # "data": "/in/dataframe.csv"
                "coords": f"/in/{coords}",
                "values": f"/in/{values}",
                "n_lags": n_lags,
                "bin_func": bin_func,
                "model": model,
                "estimator": estimator,
                "maxlag": maxlag,
                "fit_method": fit_method,
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

        for image in images:
            if 'skgstat' in image:
                # os.system(f"docker run --rm -t --network=host -v {in_dir}:/in -v {out_dir}:/out -e TOOL_RUN=variogram {image}")
                os.system(f"podman run -t --rm -it --network=host -v {in_dir}:/in -v {out_dir}:/out -e TOOL_RUN=variogram {image}")
            else:
                print('Error in processes - variogram.py. Cannot load docker image.')

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
