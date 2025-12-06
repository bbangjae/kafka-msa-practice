# kafka-msa-practice

## 개발 환경
### Language 
![Java](https://img.shields.io/badge/java-%23ED8B00.svg?style=for-the-badge&logo=openjdk&logoColor=white)
### Framework & Runtime
![Spring](https://img.shields.io/badge/spring-%236DB33F.svg?style=for-the-badge&logo=spring&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-6DB33F?style=for-the-badge&logo=springboot&logoColor=white)
### Messaging & Infrastructure
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
![Zookeeper](https://img.shields.io/badge/Zookeeper-00897B?style=for-the-badge)
### DevOps & Tools
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Gradle](https://img.shields.io/badge/Gradle-02303A?style=for-the-badge&logo=gradle&logoColor=white)


## 구현 기술

### 1. Apache Kafka 기반 비동기 메시징
- Kafka를 활용한 Publisher/Subscriber 메시지 송수신 구현
- 다중 Consumer Group을 통한 메시지 병렬 처리
- Topic 기반 이벤트 라우팅 및 파티셔닝 지원
- ACK 설정 및 재시도 메커니즘을 통한 안정성 보장

### 2. Orchestration-based Saga Pattern
- 분산 트랜잭션 처리를 위한 Saga 패턴 구현
- 중앙 집중식 오케스트레이터를 통한 전체 워크플로우 관리
- 각 단계별 성공/실패 처리 및 보상 트랜잭션 자동 실행
- 주문 → 결제 → 재고 예약의 3단계 분산 트랜잭션 시나리오 구현

### 3. 이벤트 기반 마이크로서비스 통신
- 서비스 간 직접 호출 없이 Kafka 메시지를 통한 느슨한 결합 실현
- 이벤트 소싱 패턴 적용으로 시스템 상태 추적 가능
- 비동기 이벤트 처리를 통한 성능 최적화
- 서비스 독립성 및 확장성 확보

### 4. 실패 시뮬레이션 및 보상 처리
- 결제 실패(30% 확률) 및 재고 부족(20% 확률) 시나리오 시뮬레이션
- 실패 발생 시 자동 보상 트랜잭션 실행 (결제 환불, 주문 취소)
- Saga 상태 관리를 통한 트랜잭션 일관성 유지

