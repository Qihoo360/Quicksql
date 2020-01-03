#### SQL grammar

```sql
select:
      SELECT [ STREAM ] [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }
      FROM tableExpression
      [ WHERE booleanExpression ]
      [ GROUP BY { groupItem [, groupItem ]* } ]
      [ HAVING booleanExpression ]
      [ WINDOW windowName AS windowSpec [, windowName AS windowSpec ]* ]
selectWithoutFrom:
      SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }

projectItem:
      expression [ [ AS ] columnAlias ]
  |   tableAlias . *

tableExpression:
      tableReference [, tableReference ]*
  |   tableExpression [ NATURAL ] [ ( LEFT | RIGHT | FULL ) [ OUTER ] ] JOIN tableExpression [ joinCondition ]
  |   tableExpression CROSS JOIN tableExpression
  |   tableExpression [ CROSS | OUTER ] APPLY tableExpression

joinCondition:
      ON booleanExpression
  |   USING '(' column [, column ]* ')'

tableReference:
      tablePrimary
      [ FOR SYSTEM_TIME AS OF expression ]
      [ matchRecognize ]
      [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

tablePrimary:
      [ [ catalogName . ] schemaName . ] tableName
      '(' TABLE [ [ catalogName . ] schemaName . ] tableName ')'
  |   tablePrimary [ EXTEND ] '(' columnDecl [, columnDecl ]* ')'
  |   [ LATERAL ] '(' query ')'
  |   UNNEST '(' expression ')' [ WITH ORDINALITY ]
  |   [ LATERAL ] TABLE '(' [ SPECIFIC ] functionName '(' expression [, expression ]* ')' ')'

columnDecl:
      column type [ NOT NULL ]
      
aggregateCall:
        agg( [ ALL | DISTINCT ] value [, value ]*)
        [ WITHIN GROUP (ORDER BY orderItem [, orderItem ]*) ]
        [ FILTER (WHERE condition) ]
    |   agg(*) [ FILTER (WHERE condition) ]
    
describe:
      DESCRIBE DATABASE databaseName
   |  DESCRIBE CATALOG [ databaseName . ] catalogName
   |  DESCRIBE SCHEMA [ [ databaseName . ] catalogName ] . schemaName
   |  DESCRIBE [ TABLE ] [ [ [ databaseName . ] catalogName . ] schemaName . ] tableName 		[ columnName ]
   |  DESCRIBE [ STATEMENT ] ( query | insert | update | merge | delete )
   

```

#### Functions and Operators

```sql
Conditional Expressions
support:
case | if
(1)case
SELECT CASE type WHEN 'a' THEN 'b' ELSE 'c' END FROM db.tablename
(2)if
SELECT IF(10 > 1, 'hello', 2) FROM db.tablename

Comparison operators
support:
= | <> | != | > | >=  | <  | <= | is null | is not null | between and 
| not between and| like | not like | in (sub-query) | not in(sub_query) 
| exists (sub-query) | not exists(sub-query)
(1)=
SELECT type FROM db.tablename where type=value
(2)<> | !=
SELECT  type FROM db.tablename where [type<> | !=] value
(3)>
SELECT type FROM db.tablename where type>value
(4)>=
SELECT type FROM db.tablename where type>=value 
(5)<
SELECT type FROM　db.tablename where type<value
(6)<=
SELECT type FROM  db.tablename where type<=value
(7)is null | is not null 
SELECT type FROM db.tablename where type [is null | is not  null]
(8)between and | not between and 
SELECT type FROM db.tablename where type [between value1 and value2 | not between value1 and value2]
(9)like | not like 
SELECT type FROM db.tablename where type [like | not like] [value|"%value"|"%value%"|"value%"|...]
(10)in | not in 
SELECT type FROM db.tablename where type [in|not in] (sub-query|value-list)
(11)exists | not exists
SELECT type FROM db.tablename where type [exists | not exists] (sub-query)

Logical operators
support:
boolean1 or boolean2 | boolean1 and boolean2 | not boolean | boolean is false
|boolean is not false | boolean is true | boolean is not true | boolean is unknown
|boolean is not unknown
(1)boolean1 or boolean2 --> boolean

Arithmetic operators and functions
support:
+ | - | * | / | % | POWER(numeric1, numeric2) | ABS(numeric) 
| MOD(numeric1, numeric2) | SQRT(numeric) | LN(numeric) | LOG10(numeric)
| EXP(numeric) | CEIL(numeric) | FLOOR(numeric) | RAND([seed]) | RAND_INTEGER([seed, ] numeric) | ACOS(numeric) | ASIN(numeric) | ATAN(numeric) | ATAN2(numeric, numeric)
| COS(numeric) | COT(numeric) | PI() | RADIANS(numeric) | ROUND(numeric1 [, numeric2])
| SIGN(numeric) | SIN(numeric) | TAN(numeric) | TRUNCATE(numeric1 [, numeric2])
(1)+ --> numeric1 + numeric2 = numeric

(2)- --> numeric1 - numeric2 = numeric

Character string operators and functions
support:
string || string | concat(string1, ..., stringN) | CHAR_LENGTH(string) 
| CHARACTER_LENGTH(string) | UPPER(string) | LOWER(string) | POSITION(string1 IN string2)
| POSITION(string1 IN string2 FROM integer) | TRIM( { BOTH | LEADING | TRAILING } string1 FROM string2) | OVERLAY(string1 PLACING string2 FROM integer [ FOR integer2 ]) 
| SUBSTRING(string FROM integer) | SUBSTRING(string FROM integer FOR integer) 
| INITCAP(string)
(1)[|| | concat(string1, ..., stringN)] --> Returns the concatenation of string1...stringN


(2)CHAR_LENGTH(string) --> Returns the number of characters in a character string

Date/time functions
support:
LOCALTIME | LOCALTIME(precision) | LOCALTIMESTAMP | LOCALTIMESTAMP(precision)	
| CURRENT_TIME | CURRENT_DATE | CURRENT_TIMESTAMP | EXTRACT(timeUnit FROM datetime)
| FLOOR(datetime TO timeUnit) | CEIL(datetime TO timeUnit) | YEAR(date)
| QUARTER(date) | MONTH(date) | WEEK(date) | DAYOFYEAR(date) | DAYOFMONTH(date) 
| DAYOFWEEK(date) | HOUR(date) | MINUTE(date) | SECOND(date) | TIMESTAMPADD(timeUnit, integer, datetime) | TIMESTAMPDIFF(timeUnit, datetime, datetime2) | LAST_DAY(date)


Aggregate functions
support:
COUNT(*|1|...) | SUM([ALL | DISTINCT]numeric) | AVG([ALL | DISTINCT]numeric)
| MAX([ALL | DISTINCT]value) | MIN([ALL | DISTINCT]value)  

```

#### Key words

```sql
support:
A | ABS | ABSOLUTE | ACTION | ADA | AND | AS | ADMIN | AVG | AFTER | ALL | ALLOCATE | ALLOW | ALTER | ALWAYS | ANY | APPLY | ARE | ARRAY | ARRAY_MAX_CARDINALITY | ASC | ASENSITIVE | ASSERTION | ASSIGNMENT | ASYMMETRIC | AT | ATOMIC | ATTRIBUTE | ATTRIBUTES | 
AUTHORIZATION
BETWEEN | BY | BEFORE | BEGIN | BEGIN_FRAME | BEGIN_PARTITION | BERNOULLI | BIGINT | BINARY | BITBLOB | BOOLEAN | BOTH | BREADTH
C | CAST | CASE | CALL | CALLED | CARDINALITY | CASCADE | CASCADED | CATALOG | CATALOCEILG_NAME | CEIL | CEILING | CENTURY | CHAIN | CHAR | CHAR_LENGTH | CHARACTER
CHARACTER_LENGTH | CHARACTER_SET_CATALOG | CHARACTER_SET_NAME | CHARACTER_SET_SCHEMA
CHARACTERISTICS | CHARACTERS | CHECK | CLASSIFIER | CLASS_ORIGIN | CLOB | CLOSE | COALESCE | COBOL | COLLATE | COLLATION_CATALOG | COLLATION | COLLATION_NAME | COLLATION_SCHEMA | COLLECT | COLUMN_NAME | COLUMN | COMMAND_FUNCTION | COMMAND_FUNCTION_CODE | COMMIT | COMMITTED | CONDITION | CONDITION_NUMBER | CONNECT | CONNECTION | CONNECTION_NAME | CONSTRAINT | CONSTRAINT_CATALOG | CONSTRAINT_NAME | CONSTRAINT_SCHEMA | CONSTRAINTS | CONSTRUCTOR | CONTAINS | CONTINUE
CONVERT | CORR | CORRESPONDING | COUNT | COVAR_POP | COVAR_SAMP | CREATE | CROSS | CUBE | CUME_DIST | CURRENT | CURRENT_CATALOG | CURRENT_DATE | CURRENT_DEFAULT_TRANSFORM_GROUP | CURRENT_PATH | CURRENT_ROLE | CURRENT_ROW | CURRENT_SCHEMA | CURRENT_TIME | CURRENT_TIMESTAMP | CURRENT_TRANSFORM_GROUP_FOR_TYPE | CURRENT_USER | CURSOR | CURSOR_NAME | CYCLE 
DATA | DATABASE | DATE | DATETIME_INTERVAL_CODE | DATETIME_INTERVAL_PRECISION | DAY | DEALLOCATE | DEC | DECADE | DECIMAL | DECLARE | DEFAULT_ | DEFAULTS | DEFERRABLE | DEFERRED | DEFINE | DEFINED | DEFINER | DEGREE | DELETE | DENSE_RANK | DEPTH | DEREF | DERIVED | DESC | DESCRIBE | DESCRIPTION | DESCRIPTOR | DETERMINISTIC | DIAGNOSTICS | DISALLOW | DISCONNECT | DISPATCH | DISTINCT | DOMAIN | DOUBLE | DOW | DOY | DROP | DYNAMIC | DYNAMIC_FUNCTION | DYNAMIC_FUNCTION_CODE 
EACH | ELEMENT | ELSE | EMPTY | END | END_EXEC | END_FRAME | END_PARTITION | EPOCH | EQUALS | ESCAPE | EVERY | EXCEPT | EXCEPTION | EXCLUDE | EXCLUDING | EXEC | EXECUTE | EXISTS | EXP | EXPLAIN | EXTEND | EXTERNAL | EXTRACT 
FALSE | FETCH | FILTER | FINAL | FIRST | FIRST_VALUE | FLOAT | FLOOR | FOLLOWING | FOR | FOREIGN | FORTRAN | FOUND | FRAC_SECOND | FRAME_ROW | FREE | FROM | FULL | FUNCTION | FUSION 
G | GENERAL | GENERATED | GEOMETRY | GET | GLOBAL | GO | GOTO | GRANT | GROUP | GRANTED | GROUPS 
HAVING | HOLD | HIERARCHY | HOUR 
IDENTITY | IMMEDIATE | IMMEDIATELY | IMPLEMENTATION | IMPORT | IN | INCLUDING | INCREMENT | INDICATOR | INITIAL | INITIALLY | INNER | INOUT | INPUT | INSENSITIVE | INSERT | INSTANCE | INSTANTIABLE | INT | INTEGER | INTERSECT | INTERSECTION | INTERVAL | INTO | INVOKER | IS | ISOYEAR | ISODOW | ISOLATION 
JAVA | JOIN | JSON 
K | KEY | KEY_MEMBER | KEY_TYPE 
LABEL | LAG | LANGUAGE | LARGE | LAST | LAST_VALUE | LATERAL | LEAD | LEADING | LEFT | LENGTH | LEVEL | LIBRARY | LIKE | LIKE_REGEX | LIMIT | LN | LOCAL | LOCALTIME | LOCALTIMESTAMP | LOCATOR | LOWER 
M | MAP | MATCH | MATCHED | MATCHES | MATCH_NUMBER | MATCH_RECOGNIZE | MAX | MAXVALUE | MEASURES | MEMBER | MERGE | MESSAGE_LENGTH | MESSAGE_OCTET_LENGTH | MESSAGE_TEXT | METHOD | MICROSECOND | MILLISECOND | MIN | MILLENNIUM | MINUTE | MINVALUE | MOD | MODIFIES | MODULE | MONTH | MORE_ | MULTISET | MUMPS 
NAME | NAMES | NANOSECOND | NATIONAL | NCHAR | NATURAL | NCLOB | NESTING | NEW | NEXT | NNONEO | NORMALIZE | NORMALIZED | NOT | NTH_VALUE | NTILE | NULL | NULLABLE | NULLIF | NULLS | NUMBER | NUMERIC OBJECT | OCCURRENCES_REGEX | OCTET_LENGTH | OCTETS | OF | OFFSET | OLD | OMIT | ON | ONE | ONLY | OPEN | OPTION | OPTIONS | OR | ORDER | ORDERING | ORDINALITY | OTHERS | OUT | OUTER | OUTPUT | OVER | OVERLAPS | OVERLAY | OVERRIDING 
PAD | PARAMETER | PARAMETER_MODE | PARAMETER_NAME | PARAMETER_ORDINAL_POSITION | PARAMETER_SPECIFIC_CATALOG | PARAMETER_SPECIFIC_NAME | PARAMETER_SPECIFIC_SCHEMA | PARTITION | PARTIAL | PASCAL | PASSTHROUGH | PAST | PATH | PATTERN | PER | PERCENT | PERCENTILE_CONT | PERCENTILE_DISC | PERCENT_RANK | PERIOD | PERMUTE | PLACING | PLAN | PLIPORTION | POSITION | POSITION_REGEX | POWER | PRECEDES | PRECEDING | PREPARE | PRECISION | PRESERVE | PREV | PRIMARY | PRIOR | PRIVILEGES | PROCEDURE | PUBLIC 
QUARTER 
RANGE | RANK | READ | READS | REAL | RECURSIVE | REFERENCES | REF | REFERENCING | REGR_AVGX | REGR_AVGY | REGR_COUNT | REGR_INTERCEPT | REGR_R2 | REGR_SLOPE | REGR_SXX | REGR_SXY | REGR_SYY | RELEASE | RELATIVE | REPEATABLE | REPLACE | RESET | RESTART | RESTRICT | RESULT | RETURN | RETURNED_CARDINALITY | RETURNED_LENGTH | RETURNED_OCTET_LENGTH | RETURNED_SQLSTATE | RETURNS | REVOKE | RIGHT | ROLE | ROLLBACK | ROLLUP | ROUTINE | ROUTINE_CATALOG | ROUTINE_NAME | ROUTINE_SCHEMA | ROW | ROW_COUNT | ROW_NUMBER | ROWS | RUNNING 
SAVEPOINT | SCALE | SCHEMA | SCHEMA_NAME | SCOPE | SCOPE_CATALOGS | SCOPE_NAME | SCOPE_SCHEMA | SCROLL | SEARCH | SECOND | SECTION | SECURITY | SEEK | SELECT | SELF | SENSITIVE | SEQUENCE | SERIALIZABLE | SERVER | SERVER_NAME | SESSION | SESSION_USER | SET | SETS | SET_MINUS | SHOW | SIMILAR | SIMPLE | SIZE | SKIP_ | SMALLINT | SOME | SOURCE | SPACE | SPECIFIC | SPECIFIC_NAME | SPECIFICTYPE | SQL | SQLEXCEPTION | SQLSTATE | SQLWARNING | SQL_BIGINT | SQL_BINARY | SQL_BIT | SQL_BLOB | SQL_BOOLEAN | SQL_CHAR | SQL_CLOB | SQL_DATE | SQL_DECIMAL | SQL_DOUBLE | SQL_FLOAT | SQL_INTEGER | SQL_INTERVAL_DAY | SQL_INTERVAL_DAY_TO_HOUR | SQL_INTERVAL_DAY_TO_MINUTE | SQL_INTERVAL_DAY_TO_SECOND | SQL_INTERVAL_HOUR | SQL_INTERVAL_HOUR_TO_MINUTE | SQL_INTERVAL_HOUR_TO_SECOND | SQL_INTERVAL_MINUTE | SQL_INTERVAL_MINUTE_TO_SECOND | SQL_INTERVAL_MONTH | SQL_INTERVAL_SECOND | SQL_INTERVAL_YEAR | SQL_INTERVAL_YEAR_TO_MONTH | SQL_LONGVARBINARY | SQL_LONGVARCHAR | SQL_LONGVARNCHAR | SQL_NCHAR | SQL_NCLOB | SQL_NUMERIC | SQL_NVARCHAR | SQL_REAL | SQL_SMALLINT | SQL_TIME | SQL_TIMESTAMP | SQL_TINYINT | SQL_TSI_DAY | SQL_TSI_FRAC_SECOND | SQL_TSI_HOUR | SQL_TSI_MICROSECOND | SQL_TSI_MINUTE | SQL_TSI_MONTH | SQL_TSI_QUARTER | SQL_TSI_SECOND | SQL_TSI_WEEK | SQL_TSI_YEAR | SQL_VARBINARY | SQL_VARCHAR | SQRT | START | STATE | STATEMENT | STATIC | STDDEV_POP | STDDEV_SAMP | STREAM | STRUCTURE | STYLE | SUBCLASS_ORIGIN | SUBMULTISET | SUBSET | SUBSTITUTE | SUBSTRING | SUBSTRING_REGEX | SUCCEEDS | SUM | SYMMETRIC | SYSTEM | SYSTEM_TIME | SYSTEM_USER 
TABLE | TABLE_NAME | TABLESAMPLE | TEMPORARY | THEN | TIES | TIME | TIMESTAMP | TIMESTAMPADD | TIMESTAMPDIFF | TIMEZONE_HOUR | TIMEZONE_MINUTE | TINYINT | TO | TOP_LEVEL_COUNT | TRAILING | TRANSACTION | TRANSACTIONS_ACTIVE | TRANSACTIONS_COMMITTED | TRANSACTIONS_ROLLED_BACK | TRANSFORM | TRANSFORMS | TRANSLATE | TRANSLATE_REGEX | TRANSLATION | TREAT | TRIGGER | TRIGGER_CATALOG | TRIGGER_NAME | TRIGGER_SCHEMA | TRIM | TRIM_ARRAY | TRUE | TRUNCATE | TYPE 
UESCAPE | UNBOUNDED | UNCOMMITTED | UNDER | UNION | UNIQUE | UNKNOWN | UNNAMED | UNNEST | UPDATE | UPPER | UPSERT | USAGE | USER | USER_DEFINED_TYPE_CATALOG | USER_DEFINED_TYPE_CODE | USER_DEFINED_TYPE_NAME | USER_DEFINED_TYPE_SCHEMA | USING 
VALUE | VALUES | VALUE_OF | VAR_POP | VAR_SAMP | VARBINARY | VARCHAR | VARYING | VERSION | VERSIONING | VIEW 
WEEK | WHEN | WHENEVER | WHERE | WIDTH_BUCKET | WINDOW | WITH | WITHIN | WITHOUT | WORK | WRAPPER | WRITE | XML | YEAR | ZONE
```

#### Data Types

```
lsupport:
ARRAY
BIGINT
BOOLEAN
DOUBLE
DECIMAL
DATE
FLOAT
INT
INTEGER
LONG
MAP
STRING
SMALLINT
TINYINT
TIMESTAMP
VARCHAR
```
