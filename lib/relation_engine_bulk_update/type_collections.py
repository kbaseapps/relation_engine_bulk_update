import logging
from collections import OrderedDict

import requests


def _load_collection(re_api_url, token, collection, documents, on_duplicate="update"):
    url = re_api_url + "/documents"
    headers = {"Authorization": token}
    payload = {"collection": collection, "on_duplicate": on_duplicate}
    r = requests.put(url, data="\n".join(documents), headers=headers, params=payload,)
    r.raise_for_status()
    results = r.json()
    logging.info(results)
    if results['error']:
        raise RuntimeError(f"Error saving {collection} to Relation Engine API")
    return (f"{results['created']} documents were added and {results['updated']} documents were "
            f"modified in {collection} collection. {results['errors']} documents had upload errors.")


def update_type_collections(ws_client, re_api_url, token):
    message = []
    collections = OrderedDict([
        # vertices
        ("users", set()),
        ("type_modules", set()),
        ("types", set()),
        ("type_versions", set()),
        # edges
        ("is_version_of", set()),
        ("contains", set()),
        ("is_owner_of", set()),
        ("is_latest_version_of", set()),
    ])
    for module in ws_client.list_all_types({}):
        mod_info = ws_client.get_module_info({"mod": module})
        collections['type_modules'].add('{"_key": "%s"}' % module)
        for user in mod_info['owners']:
            collections['users'].add('{"_key": "%s"}' % user)
            collections['is_owner_of'].add('{"_from": "users/%s", "_to": "type_modules/%s"}'
                                           % (user, module))
        for ws_type in ws_client.get_all_type_info(module):
            type_id = ws_type['type_def'].split("-")[0]
            collections['types'].add('{"_key": "%s"}' % type_id)
            collections['contains'].add('{"_from": "type_modules/%s", "_to": "types/%s"}'
                                        % (module, type_id))
            collections['is_latest_version_of'].add(
                '{"_from": "type_versions/%s", "_to": "types/%s"}'
                % (ws_type['type_def'], type_id))
            for type_version in ws_type['type_vers']:
                collections['type_versions'].add('{"_key": "%s"}' % type_version)
                collections['is_version_of'].add('{"_from": "type_versions/%s", "_to": "types/%s"}'
                                                 % (type_version, type_id))

    for collect, docs in collections.items():
        logging.info(f"Uploading {collect} documents to RE API")
        message.append(_load_collection(re_api_url, token, collect, docs))

    return "\n".join(message)
