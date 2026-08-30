-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS sample_mflix;

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS sample_mflix.landing;
CREATE SCHEMA IF NOT EXISTS sample_mflix.bronze;
CREATE SCHEMA IF NOT EXISTS sample_mflix.silver;

-- COMMAND ----------
CREATE VOLUME IF NOT EXISTS sample_mflix.landing.mongo;

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS sample_mflix.bronze.control_ingestion_log (
  _ingestion_id STRING,
  collection STRING,
  load_type STRING,
  watermark_inicial STRING,
  watermark_final STRING,
  qtd_lida_origem BIGINT,
  qtd_gravada_destino BIGINT,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duracao_seg INT,
  status STRING,
  error_message STRING,
  pct_nulos_source_id DOUBLE,
  qtd_duplicados_source_id BIGINT
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.bronze.quarentena (
  id STRING,
  collection STRING,
  raw_json STRING,
  motivo STRING,
  _ingestion_id STRING,
  _ingestion_timestamp TIMESTAMP
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.bronze.movies (
  id STRING,
  json STRING,
  _ingestion_id STRING,
  _ingestion_timestamp TIMESTAMP,
  _source_path STRING,
  _load_type STRING,
  _ingestion_date DATE
) USING delta PARTITIONED BY (_ingestion_date);

CREATE TABLE IF NOT EXISTS sample_mflix.bronze.comments (
  id STRING,
  json STRING,
  _ingestion_id STRING,
  _ingestion_timestamp TIMESTAMP,
  _source_path STRING,
  _load_type STRING,
  _ingestion_date DATE
) USING delta PARTITIONED BY (_ingestion_date);

CREATE TABLE IF NOT EXISTS sample_mflix.bronze.users (
  id STRING,
  json STRING,
  _ingestion_id STRING,
  _ingestion_timestamp TIMESTAMP,
  _source_path STRING,
  _load_type STRING,
  _ingestion_date DATE
) USING delta PARTITIONED BY (_ingestion_date);

CREATE TABLE IF NOT EXISTS sample_mflix.bronze.theaters (
  id STRING,
  json STRING,
  _ingestion_id STRING,
  _ingestion_timestamp TIMESTAMP,
  _source_path STRING,
  _load_type STRING,
  _ingestion_date DATE
) USING delta PARTITIONED BY (_ingestion_date);

CREATE TABLE IF NOT EXISTS sample_mflix.bronze.sessions (
  id STRING,
  json STRING,
  _ingestion_id STRING,
  _ingestion_timestamp TIMESTAMP,
  _source_path STRING,
  _load_type STRING,
  _ingestion_date DATE
) USING delta PARTITIONED BY (_ingestion_date);

CREATE TABLE IF NOT EXISTS sample_mflix.bronze.embedded_movies (
  id STRING,
  json STRING,
  _ingestion_id STRING,
  _ingestion_timestamp TIMESTAMP,
  _source_path STRING,
  _load_type STRING,
  _ingestion_date DATE
) USING delta PARTITIONED BY (_ingestion_date);

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS sample_mflix.silver.movies (
  id STRING,
  _ingestion_timestamp TIMESTAMP,
  _id STRING,
  awards_nominations BIGINT,
  awards_text STRING,
  awards_wins BIGINT,
  fullplot STRING,
  imdb_id BIGINT,
  imdb_rating DOUBLE,
  imdb_votes BIGINT,
  languages STRING,
  lastupdated STRING,
  metacritic STRING,
  num_mflix_comments STRING,
  plot STRING,
  poster STRING,
  rated STRING,
  released STRING,
  runtime STRING,
  title STRING,
  tomatoes_boxOffice STRING,
  tomatoes_consensus STRING,
  tomatoes_critic_meter BIGINT,
  tomatoes_critic_numReviews BIGINT,
  tomatoes_critic_rating DOUBLE,
  tomatoes_dvd STRING,
  tomatoes_fresh BIGINT,
  tomatoes_lastUpdated STRING,
  tomatoes_production STRING,
  tomatoes_rotten BIGINT,
  tomatoes_viewer_meter BIGINT,
  tomatoes_viewer_numReviews BIGINT,
  tomatoes_viewer_rating DOUBLE,
  tomatoes_website STRING,
  type STRING,
  year STRING,
  _silver_timestamp TIMESTAMP,
  _ingestion_id STRING
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.comments (
  id STRING,
  _ingestion_timestamp TIMESTAMP,
  _id STRING,
  date STRING,
  email STRING,
  movie_id STRING,
  name STRING,
  text STRING,
  _silver_timestamp TIMESTAMP,
  _ingestion_id STRING
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.users (
  id STRING,
  _ingestion_timestamp TIMESTAMP,
  _id STRING,
  email STRING,
  name STRING,
  _silver_timestamp TIMESTAMP,
  _ingestion_id STRING
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.theaters (
  id STRING,
  _ingestion_timestamp TIMESTAMP,
  _id STRING,
  location_address_city STRING,
  location_address_state STRING,
  location_address_street1 STRING,
  location_address_street2 STRING,
  location_address_zipcode STRING,
  location_geo_coordinates ARRAY<DOUBLE>,
  location_geo_type STRING,
  theaterId STRING,
  _silver_timestamp TIMESTAMP,
  _ingestion_id STRING
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.sessions (
  id STRING,
  _ingestion_timestamp TIMESTAMP,
  _id STRING,
  user_id STRING,
  _silver_timestamp TIMESTAMP,
  _ingestion_id STRING
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.embedded_movies (
  id STRING,
  _ingestion_timestamp TIMESTAMP,
  _id STRING,
  awards_nominations BIGINT,
  awards_text STRING,
  awards_wins BIGINT,
  fullplot STRING,
  imdb_id BIGINT,
  imdb_rating DOUBLE,
  imdb_votes BIGINT,
  languages STRING,
  metacritic STRING,
  num_mflix_comments STRING,
  plot STRING,
  poster STRING,
  rated STRING,
  runtime STRING,
  title STRING,
  type STRING,
  writers STRING,
  countries STRING,
  directors STRING,
  _silver_timestamp TIMESTAMP,
  _ingestion_id STRING
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.movies_cast (
  id STRING,
  cast STRING,
  _silver_timestamp TIMESTAMP
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.movies_genres (
  id STRING,
  genres STRING,
  _silver_timestamp TIMESTAMP
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.movies_countries (
  id STRING,
  countries STRING,
  _silver_timestamp TIMESTAMP
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.movies_directors (
  id STRING,
  directors STRING,
  _silver_timestamp TIMESTAMP
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.movies_writers (
  id STRING,
  writers STRING,
  _silver_timestamp TIMESTAMP
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.embedded_movies_cast (
  id STRING,
  cast STRING,
  _silver_timestamp TIMESTAMP
) USING delta;

CREATE TABLE IF NOT EXISTS sample_mflix.silver.embedded_movies_genres (
  id STRING,
  genres STRING,
  _silver_timestamp TIMESTAMP
) USING delta;
