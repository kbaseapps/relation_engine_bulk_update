# -*- coding: utf-8 -*-
import os
import time
import ujson
import unittest
from configparser import ConfigParser

from relation_engine_bulk_update.relation_engine_bulk_updateImpl import relation_engine_bulk_update
from relation_engine_bulk_update.relation_engine_bulk_updateServer import MethodContext
from relation_engine_bulk_update.authclient import KBaseAuth as _KBaseAuth

from installed_clients.WorkspaceClient import Workspace


class relation_engine_bulk_updateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        token = os.environ.get('KB_AUTH_TOKEN', None)
        config_file = os.environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('relation_engine_bulk_update'):
            cls.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        authServiceUrl = cls.cfg['auth-service-url']
        auth_client = _KBaseAuth(authServiceUrl)
        user_id = auth_client.get_user(token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.ctx = MethodContext(None)
        cls.ctx.update({'token': token,
                        'user_id': user_id,
                        'provenance': [
                            {'service': 'relation_engine_bulk_update',
                             'method': 'please_never_use_it_in_production',
                             'method_params': []
                             }],
                        'authenticated': 1})
        cls.wsURL = cls.cfg['workspace-url']
        cls.wsClient = Workspace(cls.wsURL)
        cls.serviceImpl = relation_engine_bulk_update(cls.cfg)
        cls.scratch = cls.cfg['scratch']
        cls.callback_url = os.environ['SDK_CALLBACK_URL']

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    def getWsClient(self):
        return self.__class__.wsClient

    def getWsId(self):
        if not hasattr(self.__class__, 'wsName'):
            suffix = int(time.time() * 1000)
            wsName = "test_relation_engine_bulk_update_" + str(suffix)
            ret = self.getWsClient().create_workspace({'workspace': wsName})  # noqa
            self.__class__.wsName = wsName
            self.__class__.wsId = ret[0]
        return self.__class__.wsId


    def getImpl(self):
        return self.__class__.serviceImpl

    def getContext(self):
        return self.__class__.ctx

    # NOTE: According to Python unittest naming rules test method names should start from 'test'. # noqa
    @unittest.skip
    def test_update_type_collections(self):
        ret = self.getImpl().update_type_collections(self.getContext(),
                                                     {'workspace_id': self.getWsId()})

    @unittest.skip
    def test_update_ws_prov(self):
        ret = self.getImpl().update_ws_provenance(self.getContext(), {
            'workspace_id': self.getWsId(),
            'list_ws_params': ujson.dumps({
                'owners': ['jjeffryes'],
                'after': '2018-11-18T20:26:25+0000'
            })
        })

    def test_update_module_collections(self):
        ret = self.getImpl().update_sdk_module_collections(self.getContext(),
                                                           {'workspace_id': self.getWsId()})
