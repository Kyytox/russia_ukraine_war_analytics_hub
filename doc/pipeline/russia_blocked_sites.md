# Russia Blocked Sites


[data sources](https://docs.google.com/spreadsheets/d/1KN3isOEE7A4vBL9-QbEd-gQUgYcg4Hn50pKHJ75SS-A/edit?gid=0#gid=0)



## Data Source Schema

Domain
Banning Authority
Locale
Type of site
Subcategory
Date blocked

| Field             | Type   | Description                                  |
| ----------------- | ------ | -------------------------------------------- |
| Domain            | String | The domain of the blocked site               |
| Banning Authority | String | The authority that blocked the site          |
| Locale            | String | The country where the site is blocked        |
| Type of site      | String | The type of site that was blocked            |
| Subcategory       | String | The subcategory of the site that was blocked |
| Date blocked      | Date   | The date the site was blocked                |


## Data Warehouse Schema

| Field             | Type   | Description                                  |
| ----------------- | ------ | -------------------------------------------- |
| domain            | String | The domain of the blocked site               |
| banning_authority | String | The authority that blocked the site          |
| country_domain    | String | The country where the site is blocked        |
| category          | String | The type of site that was blocked            |
| subcategory       | String | The subcategory of the site that was blocked |
| date_blocked      | Date   | The date the site was blocked                |


## DataMart Charts 

#### Global

- Number of sites blocked by country, top 20
  - Bar chart

- Number of sites blocked by authority
  - Bar chart

- Number of sites blocked by category
  - Bar chart

- Number of sites blocked by subcategory
  - Bar chart


#### Date

- Number of sites blocked by day
  - Line chart
  - Bar chart


- Number of sites blocked by month
  - Line chart
  - Bar chart


- Number of sites blocked by year
  - Line chart
  - Bar chart


#### By Banning Authority and Category

- Number of sites blocked by authority and category
  - Sankey chart
  - Bar chart



#### By Category and Country

- Number of sites blocked by category and country
  - Sankey chart
  - Bar chart
  
- Number of sites blocked by subcategory and country
  - Sankey chart
  - Bar chart