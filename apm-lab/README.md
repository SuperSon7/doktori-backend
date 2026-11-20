# API APM pilot

별도 로컬 실행 파일이다. 기존 PolicyController → PolicyService → ReadingGenreRepository 코드를 직접 컴파일한다. 모킹하지 않는다. MySQL의 합성 장르 2건과 삭제 1건으로 정렬·삭제 필터를 검증한다. 전체 API 서버나 인증·스케줄러·외부 제공자 검증은 포함하지 않는다. 운영 bootJar와 소스 집합을 분리했다.

클라우드 저장소 `local/apm/README.md`의 실행기를 사용한다. `ddl-auto=create`는 이 격리된 로컬 DB에만 적용한다. 운영 프로필로 복사하지 않는다.
