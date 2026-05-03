# crypto-etl
Work with API

## Project structure
- 'db' folder contains scripts for PostgreSQL
  - first of all you have to execute files extensions.sql, schemas.sql and
  sql_details.sql
  - then 'stg' and 'dwh' folders
- 'JSON history' is for json practise
  - 2 scripts for generating jsonl and json files
  - 2 scripts for extract and load data to db
  - main_json file is for starting json-read processes
- 'logs' folder is for logs
  - etl.log - logs data for extract, load and transform files
  - etl_json_load.log - logs data for 'JSON history' folder 
- external files are for API request (extract, load, transform)

## ToDO
#### DataQuality проверку
Создать процедуру DataQuality,
в которой, на стороне ХД, будут проводить проверки
Объектов и возвращать значения таблиц с 
наименованием не пройденной проверки
#### Unit тесты
#### Улучшить наблюдаемость и run metadata
#### Добавить процедуру и таблицу логов логирования
А лучше 2 таблицы (одна таблица для Python скриптов, другая - для внутренних процедур).
- расширить атрибутивный состав и, возможно, справочный.
- материализованный представление + обновление
- Внедрить в проект!
  - response = requests.get(url, stream=True)
  for item in ijson.items(response.raw, "data.item"):
  print(item)