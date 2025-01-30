# Data Lake

## Introduction

Data Lake is used for storing, clean, transform, filter, and pre-classify data from Datalake (Telegram, Twitter)

Data are stored in .parquet files

The pipeline is composed of the following steps:

1. Process data from Telegram
2. Process data from Twitter
3. Filter data
4. Pre-classify data
5. Classify data

## Telegram

Data are collected from Telegram using the Telegram API, only messages are collected

All messages after 2022-01-01, from the following channels are collected:

- electrichki
- astrapress
- shot_shot
- belzhd_live
- atesh_ua
- legionoffreedom
- VDlegionoffreedom
- ENews112
- telerzd
- mzd_rzd
- magistral_kuvalda
- nskzd
- news_zszd
- D4msk
- rospartizan
- boakom
- rdpsru
- ostanovi_vagony_2023
- activatica
- russvolcorps
- soprotivleniye_lsr
- Sib_EXpress
- algizrpd

### Ingestion

#### Schema

| Column Name   | Data Type           | Description                  |
| ------------- | ------------------- | ---------------------------- |
| ID            | object              | Telegram channel ID          |
| account       | object              | Telegram channel name        |
| id_message    | int64               | Telegram message ID          |
| date          | datetime64[ns, UTC] | Date and time of the message |
| text_original | object              | Original text of the message |

#### ..........

#### ..........

#### ..........


### Cleaning

#### Schema

| Column Name   | Data Type      | Description                  |
| ------------- | -------------- | ---------------------------- |
| ID            | object         | Telegram channel ID          |
| account       | object         | Telegram channel name        |
| id_message    | int64          | Telegram message ID          |
| date          | datetime64[ns] | Date and time of the message |
| text_original | object         | Original text of the message |

#### ..........

#### ..........

#### ..........



### Transformation

#### Schema

| Column Name    | Data Type      | Description                    |
| -------------- | -------------- | ------------------------------ |
| ID             | object         | Telegram channel ID            |
| account        | object         | Telegram channel name          |
| id_message     | int64          | Telegram message ID            |
| date           | datetime64[ns] | Date and time of the message   |
| text_original  | object         | Original text of the message   |
| text_translate | object         | Translated text of the message |
| url            | object         | URL in the message             |

#### ..........

#### ..........

#### ..........

### Twitter

Data are collected from Twitter using the Twitter API, only tweets are collected

All tweets after 2022-01-01, from the following users and who contain specific keywords are collected:

Accounts:

- @Prune602
- @LXSummer1
- @igorsushko
- @Schizointel

### Ingestion

#### Schema

| Column Name   | Data Type           | Description                  |
| ------------- | ------------------- | ---------------------------- |
| ID            | object              | Twitter user ID              |
| account       | object              | Twitter user name            |
| id_message    | object              | Twitter message ID           |
| date          | datetime64[ns, UTC] | Date and time of the message |
| text_original | object              | Original text of the message |
| url           | object              | URL in the message           |
| filter_theme  | object              | Theme of the message         |

#### ..........

#### ..........

#### ..........


### Cleaning

#### Schema

| Column Name   | Data Type      | Description                  |
| ------------- | -------------- | ---------------------------- |
| ID            | object         | Twitter user ID              |
| account       | object         | Twitter user name            |
| id_message    | object         | Twitter message ID           |
| date          | datetime64[ns] | Date and time of the message |
| text_original | object         | Original text of the message |
| url           | object         | URL in the message           |
| filter_theme  | object         | Theme of the message         |

#### ..........

#### ..........

#### ..........

## Filter
