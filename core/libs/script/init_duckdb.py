import duckdb

from core.config.paths import PATH_DB_QUALIF_DATALAKE

con = duckdb.connect(database=PATH_DB_QUALIF_DATALAKE, read_only=False)

con.execute("""
CREATE TABLE IF NOT EXISTS messages (
    id_message VARCHAR PRIMARY KEY,
    date_message TIMESTAMP,
    url VARCHAR,
    text_original VARCHAR,
    text_translate VARCHAR
);

CREATE TABLE IF NOT EXISTS messages_theme (
    id_message VARCHAR PRIMARY KEY REFERENCES messages(id_message),
    theme_inc_railway BOOLEAN,
    theme_arrest BOOLEAN,
    theme_sabotage BOOLEAN,
    terms_found_inc_railway VARCHAR,
    terms_found_arrest VARCHAR,
    terms_found_sabotage VARCHAR,
);


--DROP TABLE IF EXISTS messages_qualif_inc_railway;
CREATE TABLE IF NOT EXISTS messages_qualif_inc_railway (
    id_message VARCHAR PRIMARY KEY REFERENCES messages(id_message),
    idx VARCHAR,
    qualif_date_inc TIMESTAMP,
    qualif_region VARCHAR,
    qualif_location VARCHAR,
    qualif_gps VARCHAR,
    qualif_dmg_eqp VARCHAR,
    qualif_inc_type VARCHAR,
    qualif_coll_with VARCHAR,
    qualif_prtsn_grp VARCHAR,
    qualif_prtsn_arr BOOLEAN,
    qualif_prtsn_names VARCHAR,
    qualif_prtsn_age VARCHAR,
    qualif_app_laws VARCHAR,
    qualif_comments VARCHAR,
    is_classify BOOLEAN
);
""")

tables = con.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'main'
""").fetchall()
print("Tables :", [t[0] for t in tables])

con.close()
