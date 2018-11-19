import logging
import time

from relation_engine_bulk_update.loader import RELoader

WS_OBJ_DELIMITER = ":"


def _timestamp_to_epoch(timestamp):
    return int(time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")))


def update_ws_object_collections(ws_client, re_api_url, token, params):
    """Update all workspaces matching the supplied parameters"""
    if params.get('showDeleted'):
        raise ValueError('This option makes it impossible to determine if the workspace is '
                         'deleted or not. Use "showOnlyDeleted" to crawl deleted workspaces.')
    deleted_ws = params.get('showOnlyDeleted', False)

    loader = RELoader([
        # vertices
        "users",
        "workspaces",
        "ws_objects",
        "ws_object_versions",
        "sdk_module_versions",
        "sdk_module_method_versions",
        # edges
        "is_version_of",
        "contains",
        "is_latest_version_of",
        "is_owner_of",
        "is_instance_of",
        "was_copied_from",
        "refers_to",
        "was_created_using",
        "ws_object_was_created_with_method",
    ])
    for workspace in ws_client.list_workspace_info(params):
        ws_id = workspace[0]
        public = workspace[6] == 'r'
        owner = workspace[2]
        logging.info(f"Processing workspace {ws_id}")

        loader.add('workspaces', {'_key': str(ws_id), 'name': workspace[1],
                                  'mod_epoch': _timestamp_to_epoch(workspace[3]),
                                  'public': public, 'deleted': deleted_ws})

        loader.add('users', {'_key': owner})

        loader.add('is_owner_of', {'_from': f'users/{owner}',
                                   '_to': f'workspaces/{ws_id}'})

        for obj_info in ws_client.list_objects({'ids': [ws_id], 'showHidden': 1}):
            obj_key = WS_OBJ_DELIMITER.join([str(ws_id), str(obj_info[0])])
            latest_version = WS_OBJ_DELIMITER.join([obj_key, str(obj_info[4])])

            loader.add('ws_objects', {'_key': obj_key,
                                      'workspace_id': ws_id,
                                      'object_id': obj_info[0],
                                      'deleted': False})

            loader.add('contains', {'_from': f'workspaces/{ws_id}',
                                    '_to': f'ws_objects/{obj_key}'})

            loader.add('is_latest_version_of',
                       {'_from': f'ws_object_versions/{latest_version}',
                        '_to': f'ws_objects/{obj_key}'})

            _parse_ws_objects(ws_client, loader, obj_info, obj_key)

    return loader.save_all_to_re(re_api_url, token)


def _parse_ws_objects(ws_client, loader, obj_info, obj_key):
    """Parse all versions of a specified object"""
    objects = [{"ref": f"{obj_info[6]}/{obj_info[0]}/{ver}"}
               for ver in list(range(1, obj_info[4]+1))]

    for obj_vers in ws_client.get_objects2({'objects': objects,  'no_data': 1})['data']:
        info = obj_vers['info']
        if obj_vers.get('provenance'):
            prov = obj_vers['provenance'][0]  # only one provenance item per version in practice
        else:
            prov = {}
        ws_id = info[6]
        obj_id = obj_info[0]
        ver = info[4]
        ver_key = WS_OBJ_DELIMITER.join([str(ws_id), str(obj_id), str(ver)])

        loader.add('ws_object_versions', {'_key': ver_key,
                                          'workspace_id': ws_id,
                                          'object_id': obj_id,
                                          'version': ver,
                                          'name': info[1],
                                          'hash': info[8],
                                          'size': info[9],
                                          'epoch': obj_vers['epoch'],
                                          'deleted': False})

        loader.add('is_instance_of', {'_from': f'ws_object_versions/{ver_key}',
                                      '_to': f'type_versions/{info[2]}'})

        loader.add('is_version_of', {'_from': f'ws_object_versions/{ver_key}',
                                     '_to': f'ws_objects/{obj_key}'})

        loader.add('is_owner_of', {'_from': f'users/{obj_vers["creator"]}',
                                   '_to': f'ws_object_versions/{ver_key}'})

        for ref in obj_vers.get("refs", []):
            ref_key = ref.replace("/", WS_OBJ_DELIMITER)
            loader.add('refers_to', {'_from': f'ws_object_versions/{ver_key}',
                                     '_to': f'ws_object_versions/{ref_key}'})

        if obj_vers.get("copied"):
            copy_key = obj_vers["copied"].replace("/", WS_OBJ_DELIMITER)
            loader.add('was_copied_from', {'_from': f'ws_object_versions/{ver_key}',
                                           '_to': f'ws_object_versions/{copy_key}'})

        if prov.get('method_params'):
            _proc_creating_method(loader, prov, ver_key)

        for ref in prov.get("input_ws_objects", []):
            input_key = ref.replace("/", WS_OBJ_DELIMITER)
            loader.add('was_created_using', {'_from': f'ws_object_versions/{ver_key}',
                                             '_to': f'ws_object_versions/{input_key}'})

        for action in prov.get("subactions", []):
            # I think these are the results of test images?
            if action['commit'] == 'local-docker-image':
                action['commit'] = 'UNDEFINED'
                action['ver'] = '0.0.0'
            mod_ver_key = f"{action['name']}:{action['commit']}"
            loader.add('sdk_module_versions', {'_key': mod_ver_key,
                                               'name': action['name'],
                                               'commit': action['commit'],
                                               'ver': action['ver'].split("-")[0],
                                               'code_url': action['code_url'],
                                               })

            loader.add('was_created_using', {'_from': f'ws_object_versions/{ver_key}',
                                             '_to': f'sdk_module_versions/{mod_ver_key}'})


def _proc_creating_method(loader, prov, ver_key):
    if prov.get("subactions"):
        mod_ver = prov["subactions"][0]['commit']
    else:
        mod_ver = "UNDEFINED"
    if mod_ver == 'local-docker-image':  # I think these are the results of test images?
        mod_ver = "UNDEFINED"
        prov['service_ver'] = '0.0.0'
    app_key = f"{prov['service']}:{mod_ver}.{prov['method']}"

    loader.add('sdk_module_method_versions', {'_key': app_key,
                                              'module_name': prov['service'],
                                              'method_name': prov['method'],
                                              'commit': mod_ver,
                                              'ver': prov.get('service_ver', '0.0.0'),
                                              'code_url': 'UNKNOWN',
                                              })

    if len(prov['method_params']) > 1:
        method_params = {i: x for i, x in enumerate(prov['method_params'])}
    else:
        method_params = prov['method_params'][0]

    loader.add('ws_object_was_created_with_method',
               {'_from': f'ws_object_versions/{ver_key}',
                '_to': f'sdk_module_method_versions/{app_key}',
                'method_params': method_params})
