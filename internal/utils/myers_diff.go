package utils

import (
	"fmt"
	"strings"
)

type operation string

const (
	ADD operation = "add"
	SUB operation = "sub"
	EQ  operation = "eq"
)

func GetMyersDiff(src, dst []string, maxNeighbouringLines int) string {
	script := shortestEditScript(src, dst)

	srcIndex, dstIndex := 0, 0

	type stringChange struct {
		Op   operation
		Text string
	}
	var changeBuffer []stringChange

	for _, op := range script {
		switch op {
		case ADD:
			changeBuffer = append(changeBuffer, stringChange{
				Op:   op,
				Text: dst[dstIndex],
			})
			dstIndex++
		case EQ:
			changeBuffer = append(changeBuffer, stringChange{
				Op:   op,
				Text: src[srcIndex],
			})
			srcIndex++
			dstIndex++

		case SUB:
			changeBuffer = append(changeBuffer, stringChange{
				Op:   op,
				Text: src[srcIndex],
			})
			srcIndex++
		}
	}

	eqDiffLookAheadLR := make([]int, len(changeBuffer))
	eqObserverSoFar := maxNeighbouringLines + 1
	for i, delta := range changeBuffer {
		switch delta.Op {
		case SUB, ADD:
			eqDiffLookAheadLR[i] = eqObserverSoFar
			eqObserverSoFar = 0
		case EQ:
			eqDiffLookAheadLR[i] = eqObserverSoFar
			eqObserverSoFar++
		}
	}
	eqDiffLookAheadRL := make([]int, len(changeBuffer))
	eqObserverSoFar = 2
	for i := len(changeBuffer) - 1; i >= 0; i-- {
		switch changeBuffer[i].Op {
		case SUB, ADD:
			eqDiffLookAheadRL[i] = eqObserverSoFar
			eqObserverSoFar = 0
		case EQ:
			eqDiffLookAheadRL[i] = eqObserverSoFar
			eqObserverSoFar++
		}
	}

	var buffer []string
	var skippingHunk bool
	for i, delta := range changeBuffer {
		switch delta.Op {
		case SUB:
			buffer = append(buffer, fmt.Sprintf("- %s", changeBuffer[i].Text))
		case EQ:
			if eqDiffLookAheadRL[i] < maxNeighbouringLines || eqDiffLookAheadLR[i] < maxNeighbouringLines {
				buffer = append(buffer, fmt.Sprintf("  %s", changeBuffer[i].Text))
				skippingHunk = false
			} else {
				if !skippingHunk {
					buffer = append(buffer, "......")
				}
				skippingHunk = true
			}
		case ADD:
			buffer = append(buffer, fmt.Sprintf("+ %s", changeBuffer[i].Text))
		}
	}
	if len(buffer) == 1 && buffer[0] == "......" {
		return ""
	}
	return strings.Join(buffer, "\n")
}

func shortestEditScript(src, dst []string) []operation {
	n := len(src)
	m := len(dst)
	max := n + m // nolint: predeclared
	var trace []map[int]int
	var x, y int

loop:
	for d := 0; d <= max; d++ {
		v := make(map[int]int, d+2) //nolint: mnd
		trace = append(trace, v)
		if d == 0 {
			t := 0
			for len(src) > t && len(dst) > t && src[t] == dst[t] {
				t++
			}
			v[0] = t
			if t == len(src) && t == len(dst) { //nolint: gocritic
				break loop
			}
			continue
		}
		lastV := trace[d-1]
		for k := -d; k <= d; k += 2 {
			if k == -d || (k != d && lastV[k-1] < lastV[k+1]) {
				x = lastV[k+1]
			} else {
				x = lastV[k-1] + 1
			}
			y = x - k
			for x < n && y < m && src[x] == dst[y] {
				x, y = x+1, y+1
			}
			v[k] = x
			if x == n && y == m {
				break loop
			}
		}
	}

	// Backtracking
	var script []operation
	x = n
	y = m
	var k, prevK, prevX, prevY int
	for d := len(trace) - 1; d > 0; d-- {
		k = x - y
		lastV := trace[d-1]
		if k == -d || (k != d && lastV[k-1] < lastV[k+1]) {
			prevK = k + 1
		} else {
			prevK = k - 1
		}
		prevX = lastV[prevK]
		prevY = prevX - prevK
		for x > prevX && y > prevY {
			script = append(script, EQ)
			x--
			y--
		}
		if x == prevX {
			script = append(script, ADD)
		} else {
			script = append(script, SUB)
		}
		x, y = prevX, prevY
	}
	if trace[0][0] != 0 {
		for i := 0; i < trace[0][0]; i++ {
			script = append(script, EQ)
		}
	}

	return reverse(script)
}

func reverse(s []operation) []operation {
	result := make([]operation, len(s))
	for i, v := range s {
		result[len(s)-1-i] = v
	}
	return result
}
