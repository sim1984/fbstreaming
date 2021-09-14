Firebird Streaming
==================

Проект Firebird Streaming - это набор библиотек для разбора логов репликации Firebird после обработки их утилитой `fb_repl_print`. 
На каждое событие в логе можно написать свой обработчик унаследовав интерфейс `SegmentProcessEventListener`.

Данная библиотека может применяться для оповещения о событиях через систему очередей, или для написания собственного репликатора в другие СУБД.

Альфа релиз можно скачать по ссылке https://github.com/sim1984/fbstreaming/files/7101299/release.zip

Папка `journals/incoming` содержит логи репликации после обработки их утилитой `fb_repl_print`.

Папка `journals/outgoing` некоторые выходные файлы, которые являются результатом работы одного из плагинов.

Файл `journals/segments.journal` файл в который записываются имена файлов обработанных сегментов. Используется некоторыми плагинами.

JAR файлы содержащие плагины расположены в папке `plugins`.
В примере имеются следующие плагины:
* преобразование сегментов репликации в формат JSON
* отправка связанных данных DML операторов (в формате JSON) в очередь RabbitMQ по событию подтверждения транзакции
* запись DML операторов в SQL файлы. Запись происходит по событию подтверждения транзакции.

Для настройки используется файл `config.properties`, который должен быть расположен рядом с JAR файлом `fbstreaming-1.0.jar`.
Свойства в файле конфигурации:
* `pluginClassName` - полное имя класса плагина
* `incomingFolder` - папка со входными журналами (сегментами репликации)
* `outgoingFolder` - папка с результатами работы плагина (используется плагинами JSON и SQL)
* `journalFileName` - файл с обработанными сегментами репликации (используется плагинами SQL и RabbitMQ)
* `segmentFileNameMask` - маска для входных файлов (сегментов репликации)
* `segmentFileCharset` - кодировка сегментов репликации
* `includeTables` - регулярное выражение для фильтрации таблиц (если не задано обрабатываются все таблицы)
* `rabbit.host` - хот для плагина RabbitMQ
* `rabbit.queueName` - имя очереди для плагина RabbitMQ

Для того чтобы утилита могла обработать сегмент репликации его необходимо обработать утилитой `fb_repl_print` с помощью следующей команды:

```
fb_repl_print -d -b <archive_journal_file> > <journal_file_for_streaming>
```

Пример:

```
fb_repl_print -d -b f:\fbdata\archives\examples.fdb.arch-000000010 > f:\journals\incoming\examples.fdb.arch-000000010.txt
```

Для запуска обработчика полученных файлов введите команду:

```
java -jar fbstreaming-1.0.jar
```

## Описание плагина json

Полное имя плагина `com.hqbird.fbstreaming.plugin.json.JsonStreamPlugin`

Плагин `JsonStreamPlugin` читает файлы сегментов репликации и сохраняет данные DML операторов над таблицами в формате JSON. Для каждого файла сегмента репликации создаётся файл с данными в формате JSON. JSON файлы сохраняются в папке которая прописана в параметре `outgoingFolder`.

Файл JSON имеет следующий формат. 

```json
[
  {
    "transactionNumber": 409036118,
    "statements": [
      {
        "tableName": "CLBULLETS",
        "statementType": "INSERT",
        "keyValues": {
          "NUMBULL": 156803509
        },
        "newFieldValues": {
          "CLAGE": 45,
          "REPL$GRPID": 15,
          "PROFID": 990000090,
          "SENDSTATUS": 0,
          "POL": 1,
          "FILIAL": 15,
          "BDATE": "1976-05-03",
          "CLINICID": 38,
          "PRIMLIST": 1,
          "MODIFYDATE": "16-AUG-2021 23:50:14.2480",
          "DGOPEN": 14438,
          "DATEOPEN": "16-AUG-2021",
          "TREATCODE": 156802708,
          "PCODE": 150002994,
          "ISSUEDATE": "16-AUG-2021",
          "DISABILITYID": 150050614,
          "DCODEOPEN": 150000278,
          "NUMBULL": 156443509
        },
        "oldFieldValues": {}
      },
      ...
    ]
  },
  ....
]
```

JSON файл на верхнем уровне представляет собой массив транзакций. Для каждой
транзакции сохраняется её номер в ключе `transactionNumber`. В ключе `statements` сохраняется массив с описанием всех DML операторов над таблицами произведённых в рамках этой транзакции. Для каждого DML оператора сохраняются следующие данные:
* `tableName` - имя таблицы над которой осуществляется операция;
* `statementType` - тип оператора (INSERT, UPDATE, DELETE);
* `keyValues` - значения ключевых полей;
* `newFieldValues` - новые значения полей;
* `oldFieldValues` - старые значения полей.

Пример файла конфигурации `config.properties`:

```
pluginClassName=com.hqbird.fbstreaming.plugin.json.JsonStreamPlugin
incomingFolder=./journals/incoming
outgoingFolder=./journals/outgoing/json
segmentFileNameMask=.*txt
segmentFileCharset=windows-1251
includeTables=CLBULLETS|CLREFDET
```

## Описание плагина sql

Полное имя плагина `com.hqbird.fbstreaming.plugin.sql.SqlStreamPlugin`

Плагин `SqlStreamPlugin` читает файлы сегментов репликации и сохраняет их в файлы с DML операторами в виде SQL. Для каждого транзакции из сегмента репликации создаётся файл с SQL операторами в формате SQL. SQL файлы сохраняются в папке которая прописана в параметре `outgoingFolder`.

SQL хранит скрипт DML операторов которые разделены `;`. BLOB подтипа TEXT преобразуется в символьный литерал, если подтип BINARY, то преобразуется в бинарный литерал в 16-ричном виде. Внимание для BLOB длиннее 65535 байт будет сгенерирован ошибочный скрипт. Это планируется исправить в будущем.

Пример файла конфигурации `config.properties`:

```
pluginClassName=com.hqbird.fbstreaming.plugin.sql.SqlStreamPlugin
incomingFolder=./journals/incoming
outgoingFolder=./journals/outgoing/sql
journalFileName=./journals/segments.journal
segmentFileNameMask=.*txt
segmentFileCharset=windows-1251
includeTables=CLBULLETS|CLREFDET
```

## Описание плагина rabbitmq

Полное имя плагина `com.hqbird.fbstreaming.plugin.rabbitmq.RabbitMQStreamPlugin`

Плагин `RabbitMQStreamPlugin` читает файлы сегментов репликации и сохраняет данные DML операторов над таблицами в формате JSON. При обнаружении события подтверждения транзакции, данные в формате JSON отсылаются в очередь RabbitMQ. Формат JSON сообщения аналогичен
формату описанному выше.

Пример файла конфигурации `config.properties`:

```
pluginClassName=com.hqbird.fbstreaming.plugin.rabbitmq.RabbitMQStreamPlugin
incomingFolder=./journals/incoming
journalFileName=./journals/segments.journal
segmentFileNameMask=.*txt
segmentFileCharset=windows-1251
includeTables=CLBULLETS|CLREFDET
rabbit.host=localhost
rabbit.queueName=hello
```

В поставке есть простейший пример клиента который читает сообщение из очереди RabbitMQ: `RabbitMQReceiver-1.0.jar`.

Запуск клиента осуществляется командой:

```
java -jar RabbitMQReceiver-1.0.jar
```

