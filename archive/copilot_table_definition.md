\-- base\_datasets.enterprise\_level\_copilot\_metrics  
CREATE TABLE base\_datasets.enterprise\_level\_copilot\_metrics (  
  enterprise\_id BIGINT,  
  enterprise STRING,  
  created\_at TIMESTAMP,  
  usage\_date DATE,  
  code\_acceptance\_activity\_count BIGINT,  
  code\_generation\_activity\_count BIGINT,  
  daily\_active\_users BIGINT,  
  loc\_added\_sum BIGINT,  
  loc\_deleted\_sum BIGINT,  
  loc\_suggested\_to\_add\_sum BIGINT,  
  loc\_suggested\_to\_delete\_sum BIGINT,  
  monthly\_active\_agent\_users BIGINT,  
  monthly\_active\_chat\_users BIGINT,  
  monthly\_active\_users BIGINT,  
  totals\_by\_feature ARRAY\<STRUCT\<accepted\_loc\_sum: BIGINT, code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, generated\_loc\_sum: BIGINT, user\_initiated\_interaction\_count: BIGINT, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>,  
  totals\_by\_ide ARRAY\<STRUCT\<accepted\_loc\_sum: BIGINT, code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, generated\_loc\_sum: BIGINT, ide: STRING, user\_initiated\_interaction\_count: BIGINT, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>,  
  totals\_by\_language\_feature ARRAY\<STRUCT\<accepted\_loc\_sum: BIGINT, code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, generated\_loc\_sum: BIGINT, language: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>,  
  totals\_by\_language\_model ARRAY\<STRUCT\<accepted\_loc\_sum: BIGINT, code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, generated\_loc\_sum: BIGINT, language: STRING, model: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>,  
  totals\_by\_model\_feature ARRAY\<STRUCT\<accepted\_loc\_sum: BIGINT, code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, generated\_loc\_sum: BIGINT, model: STRING, user\_initiated\_interaction\_count: BIGINT, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>,  
  user\_initiated\_interaction\_count BIGINT,  
  weekly\_active\_users BIGINT)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.enterprise\_user\_feature\_level\_copilot\_metrics  
CREATE TABLE base\_datasets.enterprise\_user\_feature\_level\_copilot\_metrics (  
  usage\_date DATE,  
  enterprise\_id BIGINT,  
  enterprise STRING,  
  user\_id BIGINT,  
  user\_login STRING,  
  assignee\_login STRING,  
  feature STRING,  
  user\_initiated\_interaction\_count BIGINT,  
  accepted\_loc\_sum BIGINT,  
  generated\_loc\_sum BIGINT,  
  code\_acceptance\_activity\_count BIGINT,  
  code\_generation\_activity\_count BIGINT,  
  loc\_added\_sum BIGINT,  
  loc\_deleted\_sum BIGINT,  
  loc\_suggested\_to\_add\_sum BIGINT,  
  loc\_suggested\_to\_delete\_sum BIGINT)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.enterprise\_user\_ide\_level\_copilot\_metrics  
CREATE TABLE base\_datasets.enterprise\_user\_ide\_level\_copilot\_metrics (  
  usage\_date DATE,  
  enterprise\_id BIGINT,  
  enterprise STRING,  
  user\_id BIGINT,  
  user\_login STRING,  
  assignee\_login STRING,  
  ide STRING,  
  user\_initiated\_interaction\_count BIGINT,  
  accepted\_loc\_sum BIGINT,  
  generated\_loc\_sum BIGINT,  
  code\_acceptance\_activity\_count BIGINT,  
  code\_generation\_activity\_count BIGINT,  
  loc\_added\_sum BIGINT,  
  loc\_deleted\_sum BIGINT,  
  loc\_suggested\_to\_add\_sum BIGINT,  
  loc\_suggested\_to\_delete\_sum BIGINT,  
  last\_known\_plugin\_version STRUCT\<plugin: STRING, plugin\_version: STRING, sampled\_at: STRING\>)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.enterprise\_user\_language\_model\_level\_copilot\_metrics  
CREATE TABLE base\_datasets.enterprise\_user\_language\_model\_level\_copilot\_metrics (  
  usage\_date DATE,  
  enterprise\_id BIGINT,  
  enterprise STRING,  
  user\_id BIGINT,  
  user\_login STRING,  
  assignee\_login STRING,  
  language STRING,  
  model STRING,  
  accepted\_loc\_sum BIGINT,  
  generated\_loc\_sum BIGINT,  
  code\_acceptance\_activity\_count BIGINT,  
  code\_generation\_activity\_count BIGINT,  
  loc\_added\_sum BIGINT,  
  loc\_deleted\_sum BIGINT,  
  loc\_suggested\_to\_add\_sum BIGINT,  
  loc\_suggested\_to\_delete\_sum BIGINT)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.enterprise\_user\_level\_copilot\_metrics  
CREATE TABLE base\_datasets.enterprise\_user\_level\_copilot\_metrics (  
  usage\_date DATE,  
  enterprise\_id BIGINT,  
  enterprise STRING,  
  user\_id BIGINT,  
  user\_login STRING,  
  assignee\_login STRING,  
  user\_initiated\_interaction\_count BIGINT,  
  code\_generation\_activity\_count BIGINT,  
  code\_acceptance\_activity\_count BIGINT,  
  used\_agent BOOLEAN,  
  used\_chat BOOLEAN,  
  loc\_suggested\_to\_add\_sum BIGINT,  
  loc\_suggested\_to\_delete\_sum BIGINT,  
  loc\_deleted\_sum BIGINT,  
  loc\_added\_sum BIGINT,  
  totals\_by\_ide ARRAY\<STRUCT\<accepted\_loc\_sum: BIGINT, code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, generated\_loc\_sum: BIGINT, ide: STRING, last\_known\_plugin\_version: STRUCT\<plugin: STRING, plugin\_version: STRING, sampled\_at: STRING\>, user\_initiated\_interaction\_count: BIGINT, last\_known\_ide\_version: STRUCT\<ide\_version: STRING, sampled\_at: STRING\>, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>,  
  totals\_by\_feature ARRAY\<STRUCT\<accepted\_loc\_sum: BIGINT, code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, generated\_loc\_sum: BIGINT, user\_initiated\_interaction\_count: BIGINT, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>,  
  totals\_by\_language\_feature ARRAY\<STRUCT\<accepted\_loc\_sum: BIGINT, code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, generated\_loc\_sum: BIGINT, language: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>,  
  totals\_by\_language\_model ARRAY\<STRUCT\<accepted\_loc\_sum: BIGINT, code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, generated\_loc\_sum: BIGINT, language: STRING, model: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>,  
  totals\_by\_model\_feature ARRAY\<STRUCT\<accepted\_loc\_sum: BIGINT, code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, generated\_loc\_sum: BIGINT, model: STRING, user\_initiated\_interaction\_count: BIGINT, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.github\_copilot\_developer\_usage\_org\_level  
CREATE TABLE base\_datasets.github\_copilot\_developer\_usage\_org\_level (  
  copilot\_usage\_date DATE,  
  org\_name STRING,  
  param\_name STRING,  
  parameter STRING,  
  total\_lines\_accepted BIGINT,  
  total\_lines\_suggested BIGINT,  
  total\_agent\_lines INT)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.github\_copilot\_developer\_usage\_org\_level\_new  
CREATE TABLE base\_datasets.github\_copilot\_developer\_usage\_org\_level\_new (  
  copilot\_usage\_date DATE,  
  org\_name STRING,  
  param\_name STRING,  
  parameter STRING,  
  total\_lines\_accepted BIGINT,  
  total\_lines\_suggested BIGINT,  
  total\_agent\_lines BIGINT)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.github\_copilot\_developer\_usage\_teams\_level  
CREATE TABLE base\_datasets.github\_copilot\_developer\_usage\_teams\_level (  
  copilot\_usage\_date DATE,  
  org\_name STRING,  
  team\_name STRING,  
  team\_slug\_name STRING,  
  param\_name STRING,  
  parameter STRING,  
  total\_lines\_accepted DOUBLE,  
  total\_lines\_suggested DOUBLE,  
  total\_agent\_lines INT)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.github\_copilot\_developer\_usage\_teams\_level\_new  
CREATE TABLE base\_datasets.github\_copilot\_developer\_usage\_teams\_level\_new (  
  copilot\_usage\_date DATE,  
  org\_name STRING,  
  team\_name STRING,  
  team\_slug\_name STRING,  
  param\_name STRING,  
  parameter STRING,  
  total\_lines\_accepted BIGINT,  
  total\_lines\_suggested BIGINT,  
  total\_agent\_lines BIGINT)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.github\_copilot\_metrics\_dotcom\_org\_level  
CREATE TABLE base\_datasets.github\_copilot\_metrics\_dotcom\_org\_level (  
  copilot\_usage\_date DATE,  
  org\_name STRING,  
  total\_active\_users BIGINT,  
  total\_engaged\_users BIGINT,  
  model\_name STRING,  
  custom\_model\_flag STRING,  
  dotcom\_chat\_engaged\_users STRING,  
  dotcom\_chat\_chats STRING,  
  dotcom\_chat\_model\_engaged\_users STRING,  
  dotcom\_pr\_engaged\_users STRING,  
  dotcom\_pr\_repository\_name STRING,  
  dotcom\_pr\_model\_engaged\_users INT,  
  dotcom\_pr\_summaries\_created INT,  
  record\_insert\_datetime TIMESTAMP,  
  record\_inserted\_by STRING)  
USING delta  
CLUSTER BY AUTO  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.enableRowTracking' \= 'true',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.rowTracking' \= 'supported',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.github\_copilot\_metrics\_dotcom\_teams\_level  
CREATE TABLE base\_datasets.github\_copilot\_metrics\_dotcom\_teams\_level (  
  copilot\_usage\_date DATE,  
  org\_name STRING,  
  team\_name STRING,  
  team\_slug\_name STRING,  
  total\_active\_users BIGINT,  
  total\_engaged\_users BIGINT,  
  model\_name STRING,  
  custom\_model\_flag STRING,  
  dotcom\_chat\_engaged\_users STRING,  
  dotcom\_chat\_chats STRING,  
  dotcom\_chat\_model\_engaged\_users STRING,  
  dotcom\_pr\_engaged\_users STRING,  
  dotcom\_pr\_repository\_name STRING,  
  dotcom\_pr\_model\_engaged\_users INT,  
  dotcom\_pr\_summaries\_created INT,  
  record\_insert\_datetime TIMESTAMP,  
  record\_inserted\_by STRING)  
USING delta  
CLUSTER BY AUTO  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.enableRowTracking' \= 'true',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.rowTracking' \= 'supported',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.github\_copilot\_metrics\_ide\_org\_level  
CREATE TABLE base\_datasets.github\_copilot\_metrics\_ide\_org\_level (  
  copilot\_usage\_date DATE,  
  org\_name STRING,  
  total\_active\_users BIGINT,  
  total\_engaged\_users BIGINT,  
  ide\_chat\_engaged\_users BIGINT,  
  ide\_chat\_model\_name STRING,  
  ide\_chat\_custom\_model\_flag STRING,  
  ide\_chat\_editor\_name STRING,  
  ide\_chat\_editor\_chats BIGINT,  
  ide\_chat\_editor\_engaged\_users BIGINT,  
  ide\_chat\_editor\_chat\_copy\_events BIGINT,  
  ide\_chat\_editor\_chat\_insertion\_events BIGINT,  
  ide\_code\_completion\_engaged\_users BIGINT,  
  ide\_code\_completion\_model\_name STRING,  
  ide\_code\_completion\_editor\_name STRING,  
  ide\_code\_completion\_editor\_language STRING,  
  ide\_code\_completion\_laguage\_engaged\_users BIGINT,  
  ide\_code\_completion\_code\_suggestions BIGINT,  
  ide\_code\_completion\_code\_acceptances BIGINT,  
  ide\_code\_completion\_code\_lines\_suggested BIGINT,  
  ide\_code\_completion\_code\_lines\_accepted BIGINT,  
  total\_suggestions\_count BIGINT,  
  total\_acceptances\_count BIGINT,  
  total\_lines\_suggested BIGINT,  
  total\_lines\_accepted BIGINT,  
  total\_active\_users\_usage BIGINT,  
  total\_chat\_acceptances BIGINT,  
  total\_chat\_turns BIGINT,  
  total\_active\_chat\_users BIGINT,  
  rest\_api\_name STRING,  
  record\_insert\_datetime TIMESTAMP,  
  record\_inserted\_by STRING)  
USING delta  
CLUSTER BY AUTO  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.enableRowTracking' \= 'true',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.rowTracking' \= 'supported',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.github\_copilot\_metrics\_ide\_org\_level\_new  
CREATE TABLE base\_datasets.github\_copilot\_metrics\_ide\_org\_level\_new (  
  copilot\_usage\_date DATE,  
  org\_name STRING,  
  total\_active\_users BIGINT,  
  total\_engaged\_users BIGINT,  
  ide\_chat\_engaged\_users BIGINT,  
  ide\_chat\_model\_name STRING,  
  ide\_chat\_custom\_model\_flag STRING,  
  ide\_chat\_editor\_name STRING,  
  ide\_chat\_editor\_chats INT,  
  ide\_chat\_editor\_engaged\_users INT,  
  ide\_chat\_editor\_chat\_copy\_events INT,  
  ide\_chat\_editor\_chat\_insertion\_events INT,  
  ide\_code\_completion\_engaged\_users BIGINT,  
  ide\_code\_completion\_model\_name STRING,  
  ide\_code\_completion\_editor\_name STRING,  
  ide\_code\_completion\_editor\_language STRING,  
  ide\_code\_completion\_laguage\_engaged\_users BIGINT,  
  ide\_code\_completion\_code\_suggestions BIGINT,  
  ide\_code\_completion\_code\_acceptances BIGINT,  
  ide\_code\_completion\_code\_lines\_suggested\_to\_add BIGINT,  
  ide\_code\_completion\_code\_lines\_suggested\_to\_delete BIGINT,  
  ide\_code\_completion\_code\_lines\_suggested BIGINT,  
  ide\_code\_completion\_code\_lines\_accepted\_to\_add BIGINT,  
  ide\_code\_completion\_code\_lines\_accepted\_to\_delete BIGINT,  
  ide\_code\_completion\_code\_lines\_accepted BIGINT,  
  agent\_lines\_accepted\_to\_add BIGINT,  
  agent\_lines\_accepted\_to\_delete BIGINT,  
  agent\_engaged\_users BIGINT,  
  total\_suggestions\_count BIGINT,  
  total\_acceptances\_count BIGINT,  
  total\_interactions\_count BIGINT,  
  total\_lines\_suggested\_to\_add BIGINT,  
  total\_lines\_suggested\_to\_delete BIGINT,  
  total\_lines\_suggested BIGINT,  
  total\_lines\_accepted\_to\_add BIGINT,  
  total\_lines\_accepted\_to\_delete BIGINT,  
  total\_lines\_accepted BIGINT,  
  total\_active\_users\_usage BIGINT,  
  total\_chat\_acceptances BIGINT,  
  total\_chat\_turns INT,  
  total\_active\_chat\_users BIGINT,  
  rest\_api\_name STRING,  
  record\_insert\_datetime TIMESTAMP,  
  record\_inserted\_by STRING)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.github\_copilot\_metrics\_ide\_team\_level  
CREATE TABLE base\_datasets.github\_copilot\_metrics\_ide\_team\_level (  
  copilot\_usage\_date DATE,  
  org\_name STRING,  
  team\_name STRING,  
  team\_slug\_name STRING,  
  total\_active\_users STRING,  
  total\_engaged\_users STRING,  
  ide\_chat\_engaged\_users STRING,  
  ide\_chat\_model\_name STRING,  
  ide\_chat\_custom\_model\_flag STRING,  
  ide\_chat\_editor\_name STRING,  
  ide\_chat\_editor\_chats STRING,  
  ide\_chat\_editor\_engaged\_users STRING,  
  ide\_chat\_editor\_chat\_copy\_events STRING,  
  ide\_chat\_editor\_chat\_insertion\_events STRING,  
  ide\_code\_completion\_engaged\_users STRING,  
  ide\_code\_completion\_model\_name STRING,  
  ide\_code\_completion\_editor\_name STRING,  
  ide\_code\_completion\_editor\_language STRING,  
  ide\_code\_completion\_laguage\_engaged\_users STRING,  
  ide\_code\_completion\_code\_suggestions STRING,  
  ide\_code\_completion\_code\_acceptances STRING,  
  ide\_code\_completion\_code\_lines\_suggested STRING,  
  ide\_code\_completion\_code\_lines\_accepted STRING,  
  total\_suggestions\_count DOUBLE,  
  total\_acceptances\_count DOUBLE,  
  total\_lines\_suggested DOUBLE,  
  total\_lines\_accepted DOUBLE,  
  total\_active\_users\_usage STRING,  
  total\_chat\_acceptances DOUBLE,  
  total\_chat\_turns INT,  
  total\_active\_chat\_users STRING,  
  rest\_api\_name STRING,  
  record\_insert\_datetime TIMESTAMP,  
  record\_inserted\_by STRING)  
USING delta  
CLUSTER BY AUTO  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.enableRowTracking' \= 'true',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.rowTracking' \= 'supported',  
  'delta.feature.v2Checkpoint' \= 'supported',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.github\_copilot\_metrics\_ide\_teams\_level\_new  
CREATE TABLE base\_datasets.github\_copilot\_metrics\_ide\_teams\_level\_new (  
  copilot\_usage\_date DATE,  
  org\_name STRING,  
  team\_name STRING,  
  team\_slug\_name STRING,  
  workday\_team\_flag STRING,  
  team\_type STRING,  
  total\_active\_users BIGINT,  
  total\_engaged\_users BIGINT,  
  ide\_chat\_engaged\_users BIGINT,  
  ide\_chat\_model\_name STRING,  
  ide\_chat\_custom\_model\_flag STRING,  
  ide\_chat\_editor\_name STRING,  
  ide\_chat\_editor\_chats INT,  
  ide\_chat\_editor\_engaged\_users INT,  
  ide\_chat\_editor\_chat\_copy\_events INT,  
  ide\_chat\_editor\_chat\_insertion\_events INT,  
  ide\_code\_completion\_engaged\_users BIGINT,  
  ide\_code\_completion\_model\_name STRING,  
  ide\_code\_completion\_editor\_name STRING,  
  ide\_code\_completion\_editor\_language STRING,  
  ide\_code\_completion\_laguage\_engaged\_users BIGINT,  
  ide\_code\_completion\_code\_suggestions BIGINT,  
  ide\_code\_completion\_code\_acceptances BIGINT,  
  ide\_code\_completion\_code\_lines\_suggested\_to\_add BIGINT,  
  ide\_code\_completion\_code\_lines\_suggested\_to\_delete BIGINT,  
  ide\_code\_completion\_code\_lines\_suggested BIGINT,  
  ide\_code\_completion\_code\_lines\_accepted\_to\_add BIGINT,  
  ide\_code\_completion\_code\_lines\_accepted\_to\_delete BIGINT,  
  ide\_code\_completion\_code\_lines\_accepted BIGINT,  
  agent\_lines\_accepted\_to\_add BIGINT,  
  agent\_lines\_accepted\_to\_delete BIGINT,  
  agent\_engaged\_users BIGINT,  
  total\_suggestions\_count BIGINT,  
  total\_acceptances\_count BIGINT,  
  total\_interactions\_count BIGINT,  
  total\_lines\_suggested\_to\_add BIGINT,  
  total\_lines\_suggested\_to\_delete BIGINT,  
  total\_lines\_suggested BIGINT,  
  total\_lines\_accepted\_to\_add BIGINT,  
  total\_lines\_accepted\_to\_delete BIGINT,  
  total\_lines\_accepted BIGINT,  
  total\_active\_users\_usage BIGINT,  
  total\_chat\_acceptances BIGINT,  
  total\_chat\_turns INT,  
  total\_active\_chat\_users BIGINT,  
  rest\_api\_name STRING,  
  record\_insert\_datetime TIMESTAMP,  
  record\_inserted\_by STRING)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.github\_copilot\_usage\_report  
CREATE TABLE base\_datasets.github\_copilot\_usage\_report (  
  SBG STRING,  
  Login STRING,  
  Status STRING,  
  Team STRING,  
  Last\_Usage\_Date STRING,  
  Last\_Editor\_Used STRING,  
  Downloaded\_Timestamp STRING,  
  Report\_Downloaded\_Date STRING,  
  Last\_Usage\_Month STRING,  
  Active\_Last\_45\_Days STRING,  
  Email STRING,  
  UID STRING,  
  SBU STRING,  
  SBU\_Description STRING,  
  GBE STRING,  
  GBE\_Description STRING,  
  SBX STRING,  
  SBX\_Description STRING,  
  Vendor STRING,  
  Location STRING,  
  Allocated\_Date STRING,  
  Removed\_Access\_by\_IT STRING,  
  License\_Allocated\_Before\_Cut\_Off\_Date STRING,  
  Exclude\_From\_Count STRING,  
  Github\_Last\_Commit\_Date STRING,  
  Job\_Title STRING)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7')

\-- base\_datasets.org\_level\_copilot\_metrics  
CREATE TABLE base\_datasets.org\_level\_copilot\_metrics (  
  organization\_id BIGINT,  
  org\_name STRING,  
  created\_at TIMESTAMP,  
  usage\_date DATE,  
  code\_acceptance\_activity\_count BIGINT,  
  code\_generation\_activity\_count BIGINT,  
  daily\_active\_users BIGINT,  
  loc\_added\_sum BIGINT,  
  loc\_deleted\_sum BIGINT,  
  loc\_suggested\_to\_add\_sum BIGINT,  
  loc\_suggested\_to\_delete\_sum BIGINT,  
  monthly\_active\_agent\_users BIGINT,  
  monthly\_active\_chat\_users BIGINT,  
  monthly\_active\_users BIGINT,  
  totals\_by\_feature ARRAY\<STRUCT\<code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT, user\_initiated\_interaction\_count: BIGINT\>\>,  
  totals\_by\_ide ARRAY\<STRUCT\<code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, ide: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT, user\_initiated\_interaction\_count: BIGINT\>\>,  
  totals\_by\_language\_feature ARRAY\<STRUCT\<code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, language: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>,  
  totals\_by\_language\_model ARRAY\<STRUCT\<code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, language: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT, model: STRING\>\>,  
  totals\_by\_model\_feature ARRAY\<STRUCT\<code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT, model: STRING, user\_initiated\_interaction\_count: BIGINT\>\>,  
  user\_initiated\_interaction\_count BIGINT,  
  weekly\_active\_users BIGINT,  
  record\_inserted\_by STRING,  
  source\_record\_insert\_datetime TIMESTAMP)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

\-- base\_datasets.org\_user\_level\_copilot\_metrics  
CREATE TABLE base\_datasets.org\_user\_level\_copilot\_metrics (  
  usage\_date DATE,  
  organization\_id BIGINT,  
  org\_name STRING,  
  user\_id BIGINT,  
  user\_login STRING,  
  assignee\_login STRING,  
  user\_initiated\_interaction\_count BIGINT,  
  code\_generation\_activity\_count BIGINT,  
  code\_acceptance\_activity\_count BIGINT,  
  used\_agent BOOLEAN,  
  used\_chat BOOLEAN,  
  loc\_suggested\_to\_add\_sum BIGINT,  
  loc\_suggested\_to\_delete\_sum BIGINT,  
  loc\_deleted\_sum BIGINT,  
  loc\_added\_sum BIGINT,  
  totals\_by\_ide ARRAY\<STRUCT\<code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, ide: STRING, last\_known\_ide\_version: STRUCT\<ide\_version: STRING, sampled\_at: STRING\>, last\_known\_plugin\_version: STRUCT\<plugin: STRING, plugin\_version: STRING, sampled\_at: STRING\>, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT, user\_initiated\_interaction\_count: BIGINT\>\>,  
  totals\_by\_feature ARRAY\<STRUCT\<code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT, user\_initiated\_interaction\_count: BIGINT\>\>,  
  totals\_by\_language\_feature ARRAY\<STRUCT\<code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, language: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT\>\>,  
  totals\_by\_language\_model ARRAY\<STRUCT\<code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, language: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT, model: STRING\>\>,  
  totals\_by\_model\_feature ARRAY\<STRUCT\<code\_acceptance\_activity\_count: BIGINT, code\_generation\_activity\_count: BIGINT, feature: STRING, loc\_added\_sum: BIGINT, loc\_deleted\_sum: BIGINT, loc\_suggested\_to\_add\_sum: BIGINT, loc\_suggested\_to\_delete\_sum: BIGINT, model: STRING, user\_initiated\_interaction\_count: BIGINT\>\>,  
  record\_inserted\_by STRING,  
  source\_record\_insert\_datetime TIMESTAMP)  
USING delta  
TBLPROPERTIES (  
  'delta.enableDeletionVectors' \= 'true',  
  'delta.feature.appendOnly' \= 'supported',  
  'delta.feature.deletionVectors' \= 'supported',  
  'delta.feature.invariants' \= 'supported',  
  'delta.minReaderVersion' \= '3',  
  'delta.minWriterVersion' \= '7',  
  'delta.parquet.compression.codec' \= 'zstd')

