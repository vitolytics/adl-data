
# TODO

Ingestion Scripts:

1. Assets: https://api.myfantasyleague.com/2024/api_info?STATE=test&CCAT=export&TYPE=assets (similar structure to historical files being EOY and current being as the date scraped)
2. Players Table: Loop through seasons, save down the historical and keep the current year table current: https://www46.myfantasyleague.com/2025/export?TYPE=players&L=60206&APIKEY=&DETAILS=1&SINCE=&PLAYERS=&JSON=1

Automation: 
1. Replace hardcoded current season and week env variable with endpoint from nfl

Other: 
1. Salaries: script outputs 'salaries_currentyear' so that the workflow naming-wise to data downstream is not affected
