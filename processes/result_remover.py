# =================================================================
#
# Authors: Marcus Strobl <marcus.strobl@kit.edu>
#
# Copyright (c) 2024 Marcus Strobl
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging
import shutil

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from processes.podman_processor import PodmanProcessor

# TODO: figure out how to remove jobs from process db
#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.0.1',
    'id': 'result_remover',
    'title': {
        'en': 'Remove results from disk when user clicks on delete.',
        'de': 'Entfernen von Ergebnissen wenn der Nutzer auf löschen klickt.'
    },
    'description': {
        'en': 'This tool specializes in deleting data created with our V-FOR-WaTer tools by creating an in and out '
              'folder. These are also the folders that are deleted. A more general tool may also be required.',
        'de': 'Dieses Tool ist speziell auf das Löschen von Daten spezialisiert, die mit unseren V-FOR-WaTer Tools '
              'erstellt wurden, indem ein in- und out Ordner erstellt wurde. Dies sind auch die Ordner die gelöscht '
              'werden. Evtl. wird noch ein allgemeineres Tool benötigt.',
    },
    'keywords': ['data remover'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://github.com/VForWaTer/vforwater-processes',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'input_folders': {
            'title': 'List of input folders',
            'description': 'List of input folders to delete.',
            'schema': {
                'type': 'string',  # the tool needs an array of strings
                'required': 'true'
            },
            'minOccurs': 1,  # > 0 => expect the data is needed
            'maxOccurs': 0,  # 0 => no limit expects an array
        },
        'output_folders': {
            'title': 'List of output folders',
            'description': 'List of output folders to delete.',
            'schema': {
                'type': 'string',  # the tool needs an array of strings
                'required': 'true'
            },
            'minOccurs': 1,  # > 0 => expect the data is needed
            'maxOccurs': 0,  # 0 => no limit expects an array
        },
        'job_list': {
            'title': 'List of jobs',
            'description': 'List of jobs to delete.',
            'schema': {
                'type': 'string',  # the tool needs an array of strings
                'required': 'false'
            },
            'minOccurs': 0,  # > 0 => expect the data is needed
            'maxOccurs': 0,  # 0 => no limit expects an array
        },
    },
    'outputs': {
        'res': {
            'title': 'list of deletes',
            'description': 'dictionaries with lists of successful and unsuccessful deletes.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'input_folders': ["/base/data_fold/in/userX/vfw_tool_9b04", "/base/data_fold/in/userX/vfw_tool_9b09", ],
            'output_folders': ["/base/data_fold/out/userX/vfw_tool_9b04", "/base/data_fold/out/userX/vfw_tool_9b09", ],
            'job_list': ['domain:port/jobs/e58b69ce-fbf6-11ee-ae0d-520fc74c9cff', 'domain:port/jobs/058bd9ce-ccf6-11ee-ae0d-520fc74c9cea'],
        }
    }
}


class ResultRemoverProcessor(BaseProcessor):
    """Result Remover Processor"""

    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.ResultRemoverProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        mimetype = 'application/json'

        input_folders = data.get('input_folders', [])
        output_folders = data.get('output_folders', [])
        job_list = data.get('job_list', [])
        # TODO: looks like there is no remove job implemented in pygeoapi. Add a remove job function when available.
        #  Or write your own when there is nothing else to do...
        #  Or even better: When owslib has the removal implemented, hopefully it cares about everything it doesn't even
        #  need this result remover process.

        removed = []
        not_removed = []
        error = []

        def __remove(folder_list, folder_location, removed_list, not_removed_list, error_list):
            """
            Actual function to delete from hard disc.

            Parameters
            ----------
            folder_list : list
                List of folder paths.
            folder_location : str
                Location of folder to be removed.
            removed_list : list
                List to store the paths of removed folders.
            not_removed_list : list
                List to store the paths of folders that were not removed.
            error_list : list
                List to store the error messages for folders that were not removed.

            Returns
            -------
            tuple
                A tuple containing the updated removed_list, not_removed_list, and error_list.

            """
            for i in folder_list:
                try:
                    shutil.rmtree(i)
                    logging.info(f'Removed {folder_location} folder {i}')
                    removed_list.append(i)
                except Exception as e:
                    not_removed_list.append(i)
                    error_list.append(e)
                    logging.warning(f'Unable to remove {folder_location} folder {i}. Error: {e}')

            return removed_list, not_removed_list, error_list

        removed, not_removed, error = __remove(input_folders, 'input', removed, not_removed, error)
        removed, not_removed, error = __remove(output_folders, 'output', removed, not_removed, error)

        outputs = {
            'container_status': 'finished',
            'geoapi_status': 'completed',
            'value': {'removed': removed, 'not_removed': not_removed},
            'error:': error,
        }

        logging.info(f'Finished execution of result remover. return {outputs}')
        return mimetype, outputs

    def __repr__(self):
        return '<ResultRemover> completed!'
