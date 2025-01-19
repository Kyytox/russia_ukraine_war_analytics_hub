# Data Lake 

## Introduction

Data Lake is used for storing, clean, transform, filter, and pre-classify data from social media (Telegram, Twitter)

Data are stored in .parquet files

## Data Ingestion

### Telegram

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


Parquet files are stored by channel and here is the schema of the data:




### Twitter

Data are collected from Twitter using the Twitter API, only tweets are collected

All tweets after 2022-01-01, from the following users and who contain specific keywords are collected:

Accounts:
- @Prune602
- @LXSummer1
- @igorsushko
- @Schizointel







