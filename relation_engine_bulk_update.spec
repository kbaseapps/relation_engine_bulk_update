/*
A KBase module: relation_engine_bulk_update
*/

module relation_engine_bulk_update {
    typedef structure {
        string report_name;
        string report_ref;
    } ReportResults;

    /*
        Updates type mappings. Currently only requires a ws_id for the report
    */
    funcdef update_type_collections(mapping<string,UnspecifiedObject> params) returns (ReportResults output) authentication required;
    /*
        Updates sdk module mappings. Currently only requires a ws_id for the report
    */
    funcdef update_sdk_module_collections(mapping<string,UnspecifiedObject> params) returns (ReportResults output) authentication required;
    /*
        Updates the provenance relationships for workspace objects. Currently only requires a ws_id for the report
    */
    funcdef update_ws_provenance(mapping<string,UnspecifiedObject> params) returns (ReportResults output) authentication required;

};
