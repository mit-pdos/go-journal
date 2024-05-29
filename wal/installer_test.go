package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_batchBlockSplit(t *testing.T) {

	var datas = []struct {
		inIds  []uint64
		outIds [][]uint64
	}{
		{
			[]uint64{457, 457, 457},
			[][]uint64{
				{457},
			},
		},
		{
			[]uint64{1, 2, 3},
			[][]uint64{
				{1, 2, 3},
			},
		},
		{
			[]uint64{1, 3, 2, 3},
			[][]uint64{
				{1, 2, 3},
			},
		},
		{
			[]uint64{5, 1, 2, 2, 3, 5, 7, 8, 9},
			[][]uint64{
				{1, 2, 3},
				{5},
				{7, 8, 9},
			},
		},
		{
			[]uint64{10, 11, 10, 13, 15, 15, 19, 1, 2, 2, 3, 5, 7, 8, 9},
			[][]uint64{
				{1, 2, 3},
				{5},
				{7, 8, 9, 10, 11},
				{13},
				{15},
				{19},
			},
		},
		{
			[]uint64{21, 17, 18, 20, 19, 33},
			[][]uint64{
				{17, 18, 19, 20, 21},
				{33},
			},
		},
	}

	makeUpdate := func(ids []uint64) (datas []Update) {
		for _, id := range ids {
			datas = append(datas, Update{
				Addr: id,
			})
		}
		return
	}

	checkResult := func(t *testing.T, datas [][]Update, outIds [][]uint64) {
		assert.Equal(t, len(datas), len(outIds))
		for i, data := range datas {
			assert.Equal(t, len(data), len(outIds[i]))
			var ids []uint64
			for _, id := range data {
				ids = append(ids, id.Addr)
			}
			assert.Equal(t, outIds[i], ids)
		}
		return
	}

	for _, data := range datas {
		result := batchBlockSplit(makeUpdate(data.inIds))
		checkResult(t, result, data.outIds)
	}

}
