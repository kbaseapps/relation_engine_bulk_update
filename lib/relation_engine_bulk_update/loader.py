import logging
from collections import OrderedDict

import requests
import ujson


class RELoader:
    """Helper class to load docs into RE API"""
    def __init__(self, collections):
        """Supply a list of collections that will be loaded"""
        self.collections = OrderedDict([(key, set()) for key in collections])

    def __str__(self):
        return "RELoader\n" + "\n".join((f"{k}: {len(v)} documents"
                                         for k, v in self.collections.items()))

    def add(self, collection, dictionary):
        """Add a dictionary to the loader as a json document"""
        self.collections[collection].add(ujson.dumps(dictionary))

    def save_all_to_re(self, re_url, token, on_duplicate="update"):
        """Save all loaded documents to the RelationEngineAPI"""
        return "\n".join([self.save_collection_to_re(collection, re_url, token, on_duplicate)
                          for collection in self.collections])

    def save_collection_to_re(self, collection, re_url, token, on_duplicate="update"):
        """Save the loaded documents in a specified collection to the RelationEngineAPI"""
        documents = self.collections[collection]
        url = re_url + "/documents"
        headers = {"Authorization": token}
        payload = {"collection": collection, "on_duplicate": on_duplicate}
        r = requests.put(url, data="\n".join(documents), headers=headers, params=payload, )
        logging.info(f"{collection}: {r.text}")
        results = r.json()
        if results['error']:
            raise RuntimeError(f"Error saving {collection} to Relation Engine API")
        return (f"{results['created']} documents were added and {results['updated']} documents "
                f"were modified in {collection} collection. {results['errors']} documents had "
                f"upload errors.")


