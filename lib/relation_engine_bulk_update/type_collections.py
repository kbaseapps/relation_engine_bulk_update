import logging
from collections import OrderedDict

import requests


def _load_collection(re_api_url, collection, documents, on_duplicate="update"):
    url = re_api_url + "/documents"
    payload = {"collection": collection, "on_duplicate": on_duplicate}
    r = requests.put(url, data=documents, params=payload)
    r.raise_for_status()
    results = r.json()
    logging.info(results)
    if results['error']:
        raise RuntimeError(f"Error saving {collection} to Relation Engine API")
    return (f"{results['created']} documents were added and {updated} documents were modified "
            f"in {collection} collection. {results['errors']} documents had upload errors.")


def update_type_collections(ws_client, re_api_url):
    message = []
    collections = OrderedDict([
        # vertices
        ("users", set()),
        ("type_modules", set()),
        ("types", set()),
        ("type_versions", set()),
        # edges
        ("version_of", set()),
        ("contains", set()),
        ("owns", set()),
        ("latest_version", set()),
    ])
    for module in ws_client.list_all_types({}):
        mod_info = ws_client.get_module_info({"mod": module})
        collections['type_modules'].add('{"_key": "%s"}' % module)
        for user in mod_info['owners']:
            collections['users'].add('{"_key": "%s"}' % user)
            collections['owns'].add('{"_from": "%s", "_to": "%s"}' % (user, module))
        for ws_type in ws_client.get_all_type_info(module):
            type_id = ws_type['type_def'].split("-")[0]
            collections['types'].add('{"_key": "%s"}' % type_id)
            collections['contains'].add('{"_from": "%s", "_to": "%s"}' % (module, type_id))
            collections['latest_version'].add('{"_from": "%s", "_to": "%s"}' % (type_id, ws_type['type_def']))
            for type_version in ws_type['type_vers']:
                collections['type_versions'].add('{"_key": "%s"}' % type_version)
                collections['version_of'].add('{"_from": "%s", "_to": "%s"}'
                                              % (type_version, type_id))

    for collect, docs in collections.items():
        logging.info(f"Uploading {collect} documents to RE API")
        print(collect)
        print(len(docs))
        # TODO: enable this when RE API is up
        #message.append(_load_collection(re_api_url, collect, docs))

    return "\n".join(message)
