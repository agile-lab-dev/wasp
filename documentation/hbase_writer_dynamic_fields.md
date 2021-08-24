Ciao a tutti,
oggi ho reindirizzato la questione sui binari cassandra-like come suggerito da Paolo.
Cosa cambia ad alto livello:
- Ora per scrivere su più colonne di clustering  bisogno avere una riga nel DataFrame per ogni colonna di clustering. 
Questo non dovrebbe incidere sulle prestazioni poichè nel writer le put che arrivano dalla stessa partizione vengono 
comunque raggruppate in bulk, ma lato strategy, bisogna flatmappare eventuali scritture di più "segmenti".
- L'utilizzo è più elegante e dovrebbe essere più semplice lato sviluppatore.

## Configurazione KeyValueModel

**tableCatalog**: nello schema contenuto nel tableCatalog andrà inserita una colonna dal nome "clustering", 
con l'indicazione della column family (cf) sulla quale andranno i qualifier denormalizzati e la lista delle colonne 
(divisa dai due punti ":") che guideranno la denormalizzazione.
(TUTTE le colonne definite in quella column family, escludendo quelle che guidano la denormalizzazione, saranno denormalizzate)


Esempio:

```scala
s"""
|"rowkey":{"cf":"rowkey", "col":"key", "type":"string"}, |"arrivalSegmentTs":{"cf":"m", "col":"arrivalSegmentTs", "type":"long"}, |"consolidationTs":{"cf":"m", "col":"consolidationTs", "type":"long"}, |"segmentTS":{"cf":"s", "col":"segmentTS", "type":"string"}, |"clustering":{"cf":"s", "columns":"segmentTS"}, |"segment":{"cf":"s", "col":"segment", "avro":"journeySchema"} |""".stripMargin
```

In questo esempio una il "segmentTS" verrà usato come guida per la denormalizzazione, il "segment" sarà il valore scritto su hbase nella colonna

```
${segmentTS}:segment
```

Ad esempio

```
hbase(main):003:0> scan 'test:pizze' ROW COLUMN+CELL
TEST!!!!!|TEST!!!!!|19700101|123 column=m:arrivalSegmentTs, timestamp=1518105340578, value=\x00 \x00\x00\x00\x00\x00\x04\xD2
TEST!!!!!|TEST!!!!!|19700101|123 column=s:19700101000000123:segment, timestamp=1518105340578, value=\x12TEST!!!!!\x1020180207\xF6\x01\x12TEST!!!!!\xF6\x01\xF6\x01\x16\xF6\x01\x16\x12TEST!!!!!\xF6\x01 \xF6\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF6\x01\x16\xF6 \x01ff\x06@ff\x06@ff\x06@ff\x06@ff\x06@\x16\x16\x01\xF6\x01\xF6\x01\xF6\x01\x16\x16ff\x06@\x16\x16ff \x06@ff\x06@\x16\x16\xF6\x01\x12TEST!!!!!
1 row(s) in 0.0440 seconds
```

Dati contenuti nel DataFrame

Il dataframe che esce dalla strategy non ha requisiti particolari se non quelli di avere i campi indicati come colonne di clustering. Per l'esempio precedente può essere ad esempio:

```
root
|------- rowkey: string (nullable = true)
|------- arrivalSegmentTs: long (nullable = true) 
|------- consolidationTs: long (nullable = true) 
|------- segmentTS: string (nullable = true) 
|------- segment: struct (nullable = true)
| |-------- rt_provider_id: string (nullable = true)
| |-------- rt_trip_day: string (nullable = true)
| |-------- rt_id: long (nullable = false)
[ ...................... ]
```
È tutto retrocompatibile, l'unica limitazione che vedo è che "clustering" è una parola riservata e non puo' essere usata per una colonna standard.
