package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.*;

// 전체 화면 날리기 command + shift + F12

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {
        System.out.println("hello~~~~");
        new JPAQueryFactory(em); // 필드로 빼도 동시성 문제 없음 -> 스프링 프레임워크에서 주입하는 em은 트랜잭션 바인딩 해줌 -> 멀티스레드 환경에서 문제없음
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);

        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() {
        // member1
        // JPQL 은 컴파일 이후 메서드가 호출 했을때 오류 발견 가능, 그나마 인텔리제이에서 약간의 검증을 해줌
        Member findMember = em.createQuery("select m from Member m where m.username = :username", Member.class)
            .setParameter("username", "member1")
            .getSingleResult();

        // Option + enter: static import
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {
        QMember m = new QMember("m");

        Member findMember = queryFactory
            .select(m)
            .from(m)
            .where(m.username.eq("member1")) // 파라미터 바인딩을 자동으로 해준다 -> prepare statement parameter binding ??? 사용 -> 성능 유리, SQL injection 예방
            .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member findMember = queryFactory
            .select(member)
            .where(
                member.username.eq("member1")
                    .and(member.age.eq(10))
            )
            .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() {
        Member findMember = queryFactory
            .select(member)
            .where(
                member.username.eq("member1"),
                null, // null은 무시하기 때문에 동적쿼리 사용할 때 유용함
                (member.age.eq(10))
            )
            .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() {
        List<Member> fetch = queryFactory
            .selectFrom(member)
            .fetch();

        Member fetchOne = queryFactory
            .selectFrom(member)
            .fetchOne();

        Member fetchFirst = queryFactory
            .selectFrom(member)
            .fetchFirst();

        QueryResults<Member> results = queryFactory
            .selectFrom(member)
            .fetchResults();

        results.getTotal();
        /* getResult를 사용해야 확인 가능 */
        List<Member> content = results.getResults();
    }

    @Test
    public void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
            .selectFrom(member)
            .where(member.age.eq(100))
            .orderBy(member.age.desc(), member.username.asc().nullsLast())
            .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    public void paging1() {
        List<Member> result = queryFactory
            .selectFrom(member)
            .orderBy(member.username.desc())
            .offset(1)
            .limit(2)
            .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void aggregation() {
        List<Tuple> result = queryFactory
            .select(
                member.count(),
                member.age.sum(),
                member.age.avg(),
                member.age.max(),
                member.age.min()
            )
            .from(member)
            .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
    }

    @Test
    public void group() throws Exception {
        QTeam team = QTeam.team;

        List<Tuple> result = queryFactory
            .select(team.name, member.age.avg())
            .from(member)
            .join(member.team, team)
            .groupBy(team.name)
            .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);
    }

    /**
     * 팀 A에 소속된 모든 회원
     */
    @Test
    public void join() {
        QTeam team = QTeam.team;

        List<Member> result = queryFactory
            .selectFrom(member)
            .join(member.team, team) // inner join 같음
            .where(team.name.eq("teamA"))
            .fetch();

        assertThat(result)
            .extracting("username")
            .containsExactly("member1", "member2");
    }

    /**
     * 세타 조인 -> 연관관계가 없는 필드로 조인
     * 회원의 이름이 팀 이름과 같은 회원 조회
     *
     * from 절에 여러 엔티티를 선택해서 세타 조인
     * 외부 조인 불가능하지만 on 절을 사용하면 가능함 -> 하이버네이트에서 지원하기 시작함
     */
    @Test
    public void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        QTeam team = QTeam.team;

        queryFactory
            .select(member)
            .from(member.team, team)
            .where(member.username.eq(team.name))
            .fetch();
    }

    /**
     * on 절을 활용한 조인(JPA 2.1부터 지원)
     * 1. 조인 대상 필터링
     * 2. 연관관계 없는 엔티티 외부 조인
     *
     * 예) 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL: select m, t from Member m left join m.team t on t.name = 'teamA'
     */
    @Test
    public void joinOnFiltering() {
//        List<Tuple> result = queryFactory
//            .select(member, team)
//            .from(member)
//            .leftJoin(member.team, team).on(team.name.eq("teamA"))
//            .fetch();

        List<Tuple> result = queryFactory
            .select(member, team)
            .from(member)
            .join(member.team, team)
//            .on(team.name.eq("teamA))  inner join이라 On 이든 where 이든 쿼리 결과는 같다
            .where(team.name.eq("teamA"))
            .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

    }

    /**
     * 하이버네이트 5.1 부터 `on` 을 사용해서 서로 관계가 없는 필드로 외부 조인 가
     */
    @Test
    public void joinOnWithoutRelation() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        QTeam team = QTeam.team;

        queryFactory
            .select(member)
            .from(member)
            .leftJoin(team)
            .on(member.username.eq(team.name))
            .where(member.username.eq(team.name))
            .fetch();
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() {
        em.flush();
        em.clear();

        Member findMemberWithoutFetchJoin = queryFactory
            .selectFrom(QMember.member)
            .where(QMember.member.username.eq("member1"))
            .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMemberWithoutFetchJoin.getTeam());
        assertThat(loaded).as("페치 조인 미적용").isFalse();

        Member findMemberWithFetchJoin = queryFactory
            .selectFrom(QMember.member)
            .join(member.team, team).fetchJoin()
            .where(QMember.member.username.eq("member1"))
            .fetchOne();

        boolean loadedWithFetchJoin = emf.getPersistenceUnitUtil().isLoaded(findMemberWithFetchJoin.getTeam());
        assertThat(loadedWithFetchJoin).as("페치 조인 적용").isTrue();
    }

    /*
    * 나이가 가장 많은 회원 조회
    * */
    @Test
    public void subQuery() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
            .selectFrom(member)
            .where(member.age.eq(
                JPAExpressions
                    .select(memberSub.age.max())
                    .from(memberSub)
            ))
            .fetch();

        assertThat(result).extracting("age").containsExactly();
    }

    /*
     * 나이가 평균 이상인 회원 조회
     * */
    @Test
    public void subQueryGoe() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
            .selectFrom(member)
            .where(member.age.goe(
                JPAExpressions
                    .select(memberSub.age.avg())
                    .from(memberSub)
            ))
            .fetch();

        assertThat(result).extracting("age").containsExactly(30, 40);
    }

    /*
     * in 절 예제
     * */
    @Test
    public void subQueryIn() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
            .selectFrom(member)
            .where(member.age.in(
                JPAExpressions
                    .select(memberSub.age)
                    .from(memberSub)
                    .where(memberSub.age.gt(10))
            ))
            .fetch();

        assertThat(result).extracting("age").containsExactly(20, 30, 40);
    }

    /*
     * select절 subQuery 예제
     * */
    @Test
    public void selectSubQuery() {
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
            .select(member.username,
                    JPAExpressions
                        .select(memberSub.age.avg())
                        .from(memberSub))
            .from(member)
            .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }
    /**
     * From 절 서브쿼리의 한계
     * - JPA JPQL 서브쿼리의 한계로 from 절의 서브쿼리는 지원하지 않음
     * - 하이버네이트 구현체를 사용하면 select 절 서브쿼리는 지원함
     * - 해결 방안
     *      - 서브쿼리를 join으로 변경 -> 가능한 상황이 있고 불가능한 상황도 있음
     *      - 쿼리를 2번으로 나눠서 실행
     *      - native 쿼리 사용
     *
     * - 디비는 데이터를 가져오는 용도로만 사용하고 그 외 가공은 애플리케이션에서 하는게 좋다.
     * - 한 방 쿼리가 좋은 건 아니다.
     */
}
