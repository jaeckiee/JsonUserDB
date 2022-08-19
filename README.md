## 프로그램명
JsonUserDB

## 프로그램 간략 설명
DB에서 AccountUID 컬럼을 가지고 있는 테이블들 중에 입력 받은 특정 AccountUID 데이터들을
JSON으로 EXPORT하여 JSON파일에 저장을 하거나 저장된 JSON 파일의 JSON을 DB에 IMPORT
하는 프로그램입니다.

## 사용방법
JsonUserDB [options] [[--] args]

JsonUserDB [options]

## 개요
### EXPORT 모드
```sh
JsonUserDB –e –u AccountUID값 –t JSON파일이름 {-s ConnectionString | -c 섹션이름}
```
### IMPORT 모드
```sh
JsonUserDB –i –u AccountUID값 –s JSON파일이름 {-t ConnectionString | -c 섹션이름}
```
### DELETE 모드
```sh
JsonUserDB –d –u AccountUID값 {-t ConnectionString | -c 섹션이름}
```
### PRINT 모드
```sh
JsonUserDB –p –u AccountUID값 {-t ConnectionString | -c 섹션이름}
```

## 옵션
### 모드
`-e, --export`
프로그램이 EXPORT 모드로 동작하게 특정합니다. EXPORT 모드는 source DB에서 특정 AccountUID에 해당하는 데이터를 EXPORT하여 target JSON 파일에 저장합니다. EXPORT 모드는 파라미터 옵션으로 –u, –t 옵션이 필수로 입력되어야 하며 –s 와 –c 옵션 둘 중 하나가 필수로 입력되어야 합니다. –c 옵션을 입력받게 되면 해당 접속 DB가 source가 됩니다.

`-i, --import`
프로그램이 IMPORT 모드로 동작하게 특정합니다. IMPORT 모드는 source JSON 파일에서 target DB로 입력받은 특정 AccountUID 값으로 데이터를 IMPORT합니다. IMPORT 모드는 파라미터 옵션으로 –u, -s 옵션이 필수로 입력되어야 하며 –t와 –c 옵션 둘 중 하나가 필수로 입력되어야 합니다. –c 옵션을 입력받게 되면 해당 접속 DB가 target이 됩니다.

`-d, --delete`
프로그램이 DELETE 모드로 동작하게 특정합니다. DELETE 모드는 target DB에 특정 AccountUID의 데이터들을 DELETE합니다. –u 옵션이 필수로 입력되어야 하며 –t와 –c 옵션 둘 중 하나가 필수로 입력되어야 합니다. –c 옵션을 입력받게 되면 해당 접속 DB가 target이 됩니다.

`-p, --print`
프로그램이 PRINT 모드로 동작하게 특정합니다. PRINT 모드는 target DB에 특정 AccountUID의 데이터들이 출력됩니다. 출력 형식은 테이블 명과 테이블이 출력되게 됩니다. –u 옵션이 필수로 입력되어야 하며 –t와 –c 옵션 둘 중 하나가 필수로 입력되어야 합니다. –c 옵션을 입력받게 되면 해당 접속 DB가 target이 됩니다.

### 파라미터
`-s, --source {ConnectionString | JSON파일이름}`
EXPORT 모드일 경우에는 ConnectionString을 파라미터로 받습니다. 파라미터로 입력받은 값을 통해 DB에 접속하게 되고 해당 DB의 데이터가 EXPORT됩니다.
IMPORT 모드일 경우에는 JSON파일이름을 파라미터로 받습니다. 입력받은 JSON 파일이름의 파일의 JSON을 target DB에 IMPORT하게 됩니다.

`-t, --target {ConnectionString | JSON파일이름}`
EXPORT 모드일 경우에는 JSON 파일이름을 파라미터로 받습니다. 입력받은 JSON 파일이름으로 파일이 없는 경우 파일이 생성되어 source DB에 있는 데이터가 EXPORT 되어 저장됩니다.
IMPORT, DELETE, PRINT 모드일 경우에는 ConnectionString을 파라미터로 받습니다. 파라미터로 입력받은 값을 통해 DB에 접속하게 되고 해당 DB에 입력받은 모드의 동작의 target이 됩니다.

`-c, --connect 섹션이름`
INI 파일에 있는 섹션이름을 특정합니다. 특정된 섹션에 있는 키들의 값들을 통해 DB 접속을 하게 되며 해당 DB가 target 또는 source가 됩니다.

`-u, --uid AccountUID값`
AccountUID의 값을 특정합니다. 특정된 AccountUID의 데이터가 JSON을 통해DB에 IMPORT되거나 DB에 해당하는 AccountUID의 데이터들을 JSON으로 EXPORT하여 파일에 저장합니다.

### 그 외 옵션들
`-v –verbose`
실행된 SQL문들이 출력됩니다. (로그 레벨 : LOG_INFO)

`-h, -–help`
Command syntax에 대한 짧은 설명을 제공합니다.

## 부가설명
### INI 파일 이름
JsonUserDB.ini
### INI 파일 형식 설명
CONFIG 제외 섹션
- 섹션은 형식에 맞게 추가하여 사용가능합니다. 
- KEY가 될 수 있는 것은 DSN, Server, Driver, trusted_connection, UID, PWD, Database입니다. trusted_connection의 초기값은 No 입니다.
- Database는 필수 KEY입니다. 
- DSN을 키로 받거나 Server, Driver를 키로 필수로 받아야 합니다.

CONFIG 섹션
- CONFIG섹션은 프로그램 설정 값들을 정할 수 있습니다
- 설정 KEY로는 LOG_STANDARD_SEVERITY_LEVEL, ACCOUNT_UID_FIELD_NAME, IGNORE_TABLE_NAME 있습니다.
- LOG_STANDARD_SEVERITY_LEVEL의 값은 0부터 7까지 정수 중 하나만 가능합니다. (LOG_OFF : 0, LOG_FATAL : 1, LOG_ERROR : 2, LOG_WARN : 3, LOG_INFO : 4, LOG_DEBUG : 5, LOG_TRACE : 6, LOG_ALL : 7)
- ACCOUNT_UID_FIELD_NAME 값은 AccounUID 필드 이름을 특정합니다.
- IGNORE_TABLE_NAME_LIST 값은 target이 되는 DB의 테이블들 중 값에 해당하는 테이블들이 제외되며 구분자는 `,` 입니다. 

## 참고자료
DSN and Connection String Keywords and Attributes : https://docs.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver16

