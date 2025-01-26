###############
### SCHEMAS ###
###############
SCHEMA_PRE_CLASS_RAILWAY = {
    "ID": "object",
    "IDX": "object",
    "date": "datetime64[ns]",
    "pre_class_ia": "object",
    "pre_class_date_inc": "datetime64[ns]",
    "pre_class_region": "object",
    "pre_class_location": "object",
    "pre_class_gps": "object",
    "pre_class_dmg_eqp": "object",
    "pre_class_inc_type": "object",
    "pre_class_coll_with": "object",
    "pre_class_nb_loco_dmg": "int64",
    "pre_class_nb_relay_dmg": "int64",
    "pre_class_prtsn_grp": "object",
    "pre_class_prtsn_arr": "bool",
    "pre_class_prtsn_names": "object",
    "pre_class_prtsn_age": "object",
    "pre_class_app_laws": "object",
    "url": "object",
    "pre_class_comments": "object",
}

SCHEMA_PRE_CLASS_ARREST = {
    "ID": "object",
    "IDX": "object",
    "date": "datetime64[ns]",
    "pre_class_ia": "object",
    "pre_class_region": "object",
    "pre_class_location": "object",
    "pre_class_arrest_date": "datetime64[ns]",
    "pre_class_arrest_reason": "object",
    "pre_class_app_laws": "object",
    "pre_class_sentence_years": "int64",
    "pre_class_sentence_date": "datetime64[ns]",
    "pre_class_person_name": "object",
    "pre_class_person_age": "int64",
    "url": "object",
    "pre_class_comments": "object",
}

SCHEMA_PRE_CLASS_SABOTAGE = {
    "ID": "object",
    "IDX": "object",
    "date": "datetime64[ns]",
    "pre_class_ia": "object",
    "pre_class_date_inc": "datetime64[ns]",
    "pre_class_region": "object",
    "pre_class_location": "object",
    "pre_class_gps": "object",
    "pre_class_dmg_eqp": "object",
    "pre_class_inc_type": "object",
    "pre_class_prtsn_grp": "object",
    "pre_class_prtsn_arr": "bool",
    "pre_class_prtsn_names": "object",
    "pre_class_prtsn_age": "int64",
    "pre_class_app_laws": "object",
    "url": "object",
    "pre_class_comments": "object",
}

####################################
####################################
####################################

SCHEMA_EXCEL_RAILWAY = {
    "ID": "object",
    "IDX": "object",
    "class_date_inc": "datetime64[ns]",
    "class_region": "object",
    "class_location": "object",
    "class_gps": "object",
    "class_dmg_eqp": "object",
    "class_inc_type": "object",
    "class_sabotage_success": "object",
    "class_coll_with": "object",
    "class_nb_loco_dmg": "int64",
    "class_nb_relay_dmg": "int64",
    "class_prtsn_grp": "object",
    "class_prtsn_arr": "bool",
    "class_prtsn_names": "object",
    "class_prtsn_age": "object",
    "class_app_laws": "object",
    "class_sources": "object",
    "class_comments": "object",
}

SCHEMA_EXCEL_ARREST = {
    "ID": "object",
    "IDX": "object",
    "class_region": "object",
    "class_location": "object",
    "class_arrest_date": "datetime64[ns]",
    "class_arrest_reason": "object",
    "class_app_laws": "object",
    "class_sentence_years": "int64",
    "class_sentence_date": "datetime64[ns]",
    "class_person_name": "object",
    "class_person_age": "int64",
    "class_sources": "object",
    "class_comments": "object",
}

SCHEMA_EXCEL_SABOTAGE = {
    "ID": "object",
    "IDX": "object",
    "class_region": "object",
    "class_location": "object",
    "class_date_sab": "datetime64[ns]",
    "class_arrest_reason": "object",
    "class_app_laws": "object",
    "class_sentence_years": "int64",
    "class_sentence_date": "datetime64[ns]",
    "class_person_name": "object",
    "class_person_age": "int64",
    "class_sources": "object",
    "class_comments": "object",
}
