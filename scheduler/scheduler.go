package scheduler

import (
	"cube/node"
	"cube/task"
)

type Scheduler interface {
	SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node
	Score(t task.Task, nodes []*node.Node) map[string]float64
	Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}

type RoundRobin struct {
	Name       string
	LastWorker int
}

func (r *RoundRobin) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	return nodes
}

func (r *RoundRobin) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64)

	var newWorker int
	if r.LastWorker+1 < len(nodes) {
		newWorker = r.LastWorker + 1
		r.LastWorker++
	} else {
		newWorker = 0
		r.LastWorker = 0
	}

	for idx, selectedNode := range nodes {
		if idx == newWorker {
			nodeScores[selectedNode.Name] = 0.1
		} else {
			nodeScores[selectedNode.Name] = 1.0
		}
	}

	return nodeScores
}

func (r *RoundRobin) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	var bestNode *node.Node
	var lowestScore float64

	for idx, currentNode := range candidates {
		if idx == 0 {
			bestNode = currentNode
			lowestScore = scores[currentNode.Name]
			continue
		}

		if scores[currentNode.Name] < lowestScore {
			bestNode = currentNode
			lowestScore = scores[currentNode.Name]
		}
	}

	return bestNode
}
