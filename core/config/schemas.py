###############
### SCHEMAS ###
###############
SCHEMA_QUALIF_RAILWAY = {
    "ID": "object",
    "IDX": "object",
    "date": "datetime64[ns]",
    "qualif_ia": "object",
    "qualif_date_inc": "datetime64[ns]",
    "qualif_region": "object",
    "qualif_location": "object",
    "qualif_gps": "object",
    "qualif_dmg_eqp": "object",
    "qualif_inc_type": "object",
    "qualif_coll_with": "object",
    "qualif_nb_loco_dmg": "int64",
    "qualif_nb_relay_dmg": "int64",
    "qualif_prtsn_grp": "object",
    "qualif_prtsn_arr": "bool",
    "qualif_prtsn_names": "object",
    "qualif_prtsn_age": "object",
    "qualif_app_laws": "object",
    "url": "object",
    "qualif_comments": "object",
}

SCHEMA_QUALIF_ARREST = {
    "ID": "object",
    "IDX": "object",
    "date": "datetime64[ns]",
    "qualif_ia": "object",
    "qualif_region": "object",
    "qualif_location": "object",
    "qualif_arrest_date": "datetime64[ns]",
    "qualif_arrest_reason": "object",
    "qualif_app_laws": "object",
    "qualif_media_post": "object",
    "qualif_sentence_years": "int64",
    "qualif_sentence_date": "datetime64[ns]",
    "qualif_person_name": "object",
    "qualif_person_age": "int64",
    "qualif_person_job": "object",
    "url": "object",
    "qualif_comments": "object",
}

# SCHEMA_QUALIF_SABOTAGE = {
#     "ID": "object",
#     "IDX": "object",
#     "date": "datetime64[ns]",
#     "qualif_ia": "object",
#     "qualif_date_inc": "datetime64[ns]",
#     "qualif_region": "object",
#     "qualif_location": "object",
#     "qualif_gps": "object",
#     "qualif_dmg_eqp": "object",
#     "qualif_inc_type": "object",
#     "qualif_prtsn_grp": "object",
#     "qualif_prtsn_arr": "bool",
#     "qualif_prtsn_names": "object",
#     "qualif_prtsn_age": "int64",
#     "qualif_app_laws": "object",
#     "url": "object",
#     "qualif_comments": "object",
# }

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
    "class_media_post": "object",
    "class_sentence_years": "int64",
    "class_sentence_date": "datetime64[ns]",
    "class_person_name": "object",
    "class_person_age": "int64",
    "class_person_job": "object",
    "class_sources": "object",
    "class_comments": "object",
}


# SCHEMA_EXCEL_SABOTAGE = {
#     "ID": "object",
#     "IDX": "object",
#     "class_region": "object",
#     "class_location": "object",
#     "class_date_sab": "datetime64[ns]",
#     "class_arrest_reason": "object",
#     "class_app_laws": "object",
#     "class_sentence_years": "int64",
#     "class_sentence_date": "datetime64[ns]",
#     "class_person_name": "object",
#     "class_person_age": "int64",
#     "class_sources": "object",
#     "class_comments": "object",
# }
