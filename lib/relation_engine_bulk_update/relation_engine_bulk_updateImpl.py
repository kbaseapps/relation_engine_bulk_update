# -*- coding: utf-8 -*-
#BEGIN_HEADER
import logging
import os

from installed_clients.KBaseReportClient import KBaseReport
from installed_clients.WorkspaceClient import Workspace
from relation_engine_bulk_update.type_collections import update_type_collections
#END_HEADER


class relation_engine_bulk_update:
    '''
    Module Name:
    relation_engine_bulk_update

    Module Description:
    A KBase module: relation_engine_bulk_update
    '''

    ######## WARNING FOR GEVENT USERS ####### noqa
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    ######################################### noqa
    VERSION = "0.0.1"
    GIT_URL = ""
    GIT_COMMIT_HASH = ""

    #BEGIN_CLASS_HEADER
    # Class variables and functions can be defined in this block
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
# Any configuration parameters that are important should be parsed and
# saved in the constructor.
        self.callback_url = os.environ['SDK_CALLBACK_URL']
        self.shared_folder = config['scratch']
        self.re_api_url = config['re-api-url']
        self.ws = Workspace(config['workspace-url'])
        self.kb_report = KBaseReport(self.callback_url)
        logging.basicConfig(level=logging.INFO)
        #END_CONSTRUCTOR
        pass


    def update_type_collections(self, ctx, params):
        """
        This example function accepts any number of parameters and returns results in a KBaseReport
        :param params: instance of mapping from String to unspecified object
        :returns: instance of type "ReportResults" -> structure: parameter
           "report_name" of String, parameter "report_ref" of String
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN update_type_collections
        message = update_type_collections(self.ws, self.re_api_url)
        report_info = self.kb_report.create(
            {'report': {'objects_created': [],
                        'text_message': message},
             'workspace_name': params['workspace_name']})
        output = {
            'report_name': report_info['name'],
            'report_ref': report_info['ref'],
        }
        #END update_type_collections

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method update_type_collections return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]
    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]
