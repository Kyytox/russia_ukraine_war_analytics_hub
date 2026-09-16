# Data Lake

## Introduction

Data Lake is used for storing, clean, transform, filter, and pre-classify data (Telegram, Twitter)

Data are stored in .parquet files, partitioned by account

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

#### Processing

For each account in the list (LIST_ACCOUNTS_TELEGRAM), the following steps are performed:

- Check if account has already been processed
- If account has not been processed, get all messages from date 2022-01-01 else get the last ID message processed.
- Retrieve the messages

#### Storage

Data are stored in .parquet files, partitioned by account in the following path:

```
/data/datalake/telegram/raw/raw_telegram.parquet
```

### Cleaning

#### Schema

| Column Name   | Data Type      | Description                  |
| ------------- | -------------- | ---------------------------- |
| ID            | object         | Telegram channel ID          |
| account       | object         | Telegram channel name        |
| id_message    | int64          | Telegram message ID          |
| date          | datetime64[ns] | Date and time of the message |
| text_original | object         | Original text of the message |

#### Processing

- Get data previously extracted
- Get data already cleaned
- Keep only the new data
- Update Date with the correct timezone
- Clean the text of the message

#### Storage

Data are stored in .parquet files, partitioned by account in the following path:

```
/data/datalake/telegram/clean/clean_telegram.parquet
```

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

#### Processing

- Get data previously cleaned
- Get data already transformed
- For data already transformed, remove data where the text translated is 65% less than the original text, for retranlate it
- Keep only the new data

- For each account in the data
  - Add a new column `url` with concatenate account and id_message
  - Sort data by the number of words in the message (for translate in first the shortest messages)
  - Keep x messages to translate (x = SIZE_TO_TRANSLATE)
  - Translate the messages
  - Clean the text translated


#### Storage

Data are stored in .parquet files, partitioned by account in the following path:

```
/data/datalake/telegram/transform/transform_telegram.parquet
```

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
