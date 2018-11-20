import logging

from relation_engine_bulk_update.loader import RELoader


def update_sdk_module_collections(catalog, re_api_url, token):
    loader = RELoader([
        # vertices
        "sdk_modules",
        "sdk_module_versions",
        "sdk_module_method_versions",
        "users",
        # edges
        "is_version_of",
        "contains",
        "is_owner_of",
    ])
    for module in catalog.list_basic_module_info({'include_unreleased': 1}):
        mod_key = module['module_name']
        logging.info(mod_key)
        loader.add('sdk_modules', {'_key': mod_key,
                                   'language': module['language'],
                                   'dynamic_service': bool(module['dynamic_service'])})

        for owner in module['owners']:
            loader.add('users', {'_key': owner})
            loader.add('is_owner_of', {'_from': f'users/{owner}',
                                       '_to': f'sdk_modules/{mod_key}'})

        for tag in ['dev', 'beta', 'release']:
            if not module[tag]:
                continue
            git_hash = module[tag]['git_commit_hash']
            mod_version_key = f'{module["module_name"]}:{git_hash}'
            version_detail = catalog.get_module_version({'module_name': mod_key,
                                                         'git_commit_hash': git_hash})
            loader.add('sdk_module_versions', {'_key': mod_version_key,
                                               'name': mod_key,
                                               'commit': git_hash,
                                               'ver': version_detail['version'],
                                               'code_url': version_detail['git_url']
                                               })

            # TODO: remove prior is_version_of links
            loader.add('is_version_of', {'_from': f'sdk_modules/{module["module_name"]}',
                                         '_to': f'sdk_module_versions/{mod_version_key}',
                                         'tag': tag})

    return loader.save_all_to_re(re_api_url, token)
