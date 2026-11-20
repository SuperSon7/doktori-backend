# 데이터 HA 클라이언트 변경 검토

`codex/data-ha-clients`는 기준 `45393d6591c696686a9207e4e1bcd7ebbf263ade`에서 데이터 HA와 관련된 9개 파일만 분리한 브랜치다. APM 변경은 포함하지 않았다.

- API·Chat의 Redis Sentinel 인증 설정과 API의 MQ multi-address profile 연결.
- MQ publisher confirm/return 확인 후 다음 queue로 인계하고, 인계 실패 시 원본 재전달을 유지하는 변경.
- Sentinel profile binding 및 confirm/ACK 순서의 단위 검사.

인프라는 [doktori-cloud의 feature/data-ha-clusters](https://github.com/SuperSon7/doktori-cloud/tree/feature/data-ha-clusters)와 함께 검토한다. Mongo의 replica-set URI 준비는 cloud 쪽에 있으며 이번 백엔드 diff에 Mongo용 신규 코드를 추가한 것은 아니다.

이 코드의 기존 작업에서 Java 단위 검사 19개(발행/소비 15개·Sentinel binding 4개)를 확인했다. 이번 업로드 단계에서는 변경 파일을 그대로 분리하고 diff·비밀값·범위를 점검했으며 Java 테스트를 새로 실행하지 않았다. 전체 서비스 기동·실제 broker failover·FCM/SSE 전달·운영 프로필/이미지 배포 검증은 포함하지 않는다.

브랜치 게시만 수행하며 main/develop 병합·배포·PR 생성은 하지 않는다. `codex/` 접두어를 사용해 기존 `feature/**` push workflow의 원격 빌드·알림을 이번 리뷰 업로드에서 실행하지 않는다. 이 선택은 CI 통과를 의미하지 않는다.
