from relation_engine_bulk_update.loader import RELoader


def update_type_collections(ws_client, re_api_url, token):
    loader = RELoader([
        # vertices
        "users",
        "type_modules",
        "types",
        "type_versions",
        # edges
        "is_version_of",
        "contains",
        "is_latest_version_of",
        "is_owner_of",
    ])
    for module in ws_client.list_all_types({}):
        mod_info = ws_client.get_module_info({"mod": module})
        loader.add('type_modules', {'_key': module})
        for owner in mod_info['owners']:
            loader.add('users', {'_key': owner})
            loader.add('is_owner_of', {'_from': f'users/{owner}',
                                       '_to': f'type_modules/{module}'})
        for ws_type in ws_client.get_all_type_info(module):
            type_id = ws_type['type_def'].split("-")[0]
            loader.add('types', {'_key': type_id})
            loader.add('contains', {'_from': f'type_modules/{module}',
                                    '_to': f'types/{type_id}'})
            loader.add('is_latest_version_of', {'_from': f'type_versions/{ws_type["type_def"]}',
                                                '_to': f'types/{type_id}'})

            for type_version in ws_type['type_vers']:
                loader.add('type_versions', {'_key': type_version})
                loader.add('is_version_of', {'_from': f'type_versions/{type_version}',
                                             '_to': f'types/{type_id}'})

    return loader.save_all_to_re(re_api_url, token)
