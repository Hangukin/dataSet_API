# 데이터 제공 API 구축 
- fastAPI, Nginx, Docker, MySQL
- 데이터 등록 API ( 관리자 전용 ), 데이터 조회 API ( 사용자 )

## admin PostData() 
- 사용자 인증, admin인 경우 수행 가능 

## ./src/apis/auth.py
- 사용자 등록, 검증 등 함수 

## ./src/apis/getData.py
- 사용자의 데이터 조회 기능 구현 

## ./src/db/dataDB.py
- 데이터 SELECT, INPUT 구현

## ./src/db/userDB.py
- DB SELECT, INPUT 등 구현 