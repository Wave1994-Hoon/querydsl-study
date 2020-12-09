package study.querydsl;

import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;

import static org.assertj.core.api.Assertions.*;

// 전체 화면 날리기 command + shift + F12

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {
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
}
