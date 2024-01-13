package paxos

import (
	"fmt"
	"testing"
)

func main() {
	fmt.Println("111")
}

func start(acceptorIds []int, learnerIds []int) ([]*Acceptor, []*Learner) {
	acceptors := make([]*Acceptor, 0)
	for _, aid := range acceptorIds {
		a := newAcceptor(aid, learnerIds)
		acceptors = append(acceptors, a)
	}
	learners := make([]*Learner, 0)
	for _, lid := range learnerIds {
		l := newLearner(lid, acceptorIds)
		learners = append(learners, l)
	}

	return acceptors, learners
}

func cleanup(acceptors []*Acceptor, learners []*Learner) {
	for _, a := range acceptors {
		a.close()
	}
	for _, l := range learners {
		l.close()
	}
}

func testSingleProposer(t *testing.T) {
	acceptorIds := []int{1001, 1002, 1003}
	learnerIds := []int{2001}
	acceptors, leanrners := start(acceptorIds, learnerIds)

	defer cleanup(acceptors, leanrners)

	p := &Proposer{
		id:        1,
		acceptors: acceptorIds,
	}
	value := p.propose("hello world")
	if value != "hello world" {
		t.Errorf("value = %s, except %s", value, "hello world")
	}

	learnValue := leanrners[0].chosen()
	if learnValue != value {
		t.Errorf("learnValue = %s, except %s", learnValue, "hello world")
	}
}

func testTwoProposer(t *testing.T) {
	acceptorIds := []int{1001, 1002, 1003}
	learnerIds := []int{2001}
	acceptors, leanrners := start(acceptorIds, learnerIds)

	defer cleanup(acceptors, leanrners)

	p1 := &Proposer{
		id:        1,
		acceptors: acceptorIds,
	}
	v1 := p1.propose("hello world")
	p2 := &Proposer{
		id:        2,
		acceptors: acceptorIds,
	}
	v2 := p2.propose("no hello world")
	if v1 != v2 {
		t.Errorf("v1 = %s, v2 =  %s", v1, v2)
	}

	learnValue := leanrners[0].chosen()
	if learnValue != v1 {
		t.Errorf("learnValue = %s, except %s", learnValue, v1)
	}
}
