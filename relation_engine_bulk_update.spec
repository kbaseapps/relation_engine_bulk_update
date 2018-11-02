/*
A KBase module: relation_engine_bulk_update
*/

module relation_engine_bulk_update {
    typedef structure {
        string report_name;
        string report_ref;
    } ReportResults;

    /*
        This example function accepts any number of parameters and returns results in a KBaseReport
    */
    funcdef update_type_collections(mapping<string,UnspecifiedObject> params) returns (ReportResults output) authentication required;

};
