import logging
import time
import collections

from installed_clients.baseclient import ServerError
from relation_engine_bulk_update.loader import RELoader

WS_OBJ_DELIMITER = ":"
GENOME_WORKSPACE = "ReferenceDataManager"


def _timestamp_to_epoch(timestamp):
    return int(time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")))


def _tuples_to_dict(tuple_list):
    aggregator = collections.defaultdict(list)
    for k, v in tuple_list:
        aggregator[k].append(v)
    return dict(aggregator)


def _convert_location(loc):
    location_keys = ['contig', 'strand', 'start', 'length']
    return {k: v for k, v in zip(location_keys, loc)}


def _parse_gene(feature):
    copy_keys = ['protein_translation', 'protein_translation_length', 'note', 'functions',
                 'functional_descriptions', 'flags', 'warnings', 'dna_sequence',
                 'dna_sequence_length']
    parsed = {k: feature[k] for k in copy_keys if k in feature}
    parsed['_key'] = feature['id']
    parsed['md5_hash'] = feature['md5']
    parsed['type'] = feature.get('type', 'gene')
    parsed['location'] = [_convert_location(l) for l in feature['location']]
    try:
        if feature.get('aliases'):
            parsed['aliases'] = _tuples_to_dict(feature['aliases'])
        if feature.get('db_xrefs'):
            parsed['db_xrefs'] = _tuples_to_dict(feature['db_xrefs'])
    except ValueError:
        pass
    return parsed


def _parse_genome(genome):
    copy_keys = ['dna_size', 'scientific_name', 'domain', 'feature_counts', 'contig_ids', 'notes'
                 'contig_lengths', 'source', 'source_id', 'release', 'gc_content', 'is_suspect']
    parsed = {k: genome[k] for k in copy_keys if k in genome}
    parsed['_key'] = genome['source_id']
    parsed['taxonomy'] = genome['taxonomy'].split("; ")
    if "contig_ids" in parsed:
        parsed['num_contigs'] = len(parsed['contig_ids'])
    return parsed


def update_ncbi_genomes(ws_client, re_api_url, token, params):
    """Update all genomes matching the supplied parameters. Only doing genes for now"""

    for genome_name in params['genomes']:
        loader = RELoader([
            # vertices
            "ncbi_genome",
            "ncbi_gene",
            # edges
            "ncbi_gene_within_genome",
        ])
        logging.info(f"Importing {genome_name}")
        genome_keys = ['features', 'dna_size', 'scientific_name', 'domain', 'feature_counts',
                       'contig_ids', 'contig_lengths', 'source', 'source_id', 'release',
                       'gc_content', 'is_suspect', 'notes', 'taxonomy']
        try:
            genome = ws_client.get_objects2({'objects': [{'ref': f'{GENOME_WORKSPACE}/{genome_name}',
                                                          'included': genome_keys}]}
                                            )['data'][0]['data']
        except ServerError:
            logging.warning(f"{genome_name} was not found in refdata workspace. Skipping")
            continue
        parsed_genome = _parse_genome(genome)
        loader.add('ncbi_genome', parsed_genome)

        for feature in genome['features']:
            parsed_gene = _parse_gene(feature)
            loader.add('ncbi_gene', parsed_gene)
            loader.add('ncbi_gene_within_genome', {'_from': f'ncbi_gene/{parsed_gene["_key"]}',
                                                   '_to': f'ncbi_genome/{parsed_genome["_key"]}'})
        loader.save_all_to_re(re_api_url, token)

    return "Done"
