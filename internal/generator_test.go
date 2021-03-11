package internal

import (
	"fmt"
	"testing"

	"github.com/gaofubao/stress/v1.0.0/buffer"
)

func TestGenerateFromFaker_Generate(t *testing.T) {
	g := &genFromFaker{}

	pool, err := buffer.NewPool(100, 10)
	if err != nil {
		t.Fatal("实例化缓冲池失败", err.Error())
	}

	go g.Generate(pool)
	for {

		data, err := pool.Get()
		if err != nil {
			t.Fatal("从缓冲池中获取数据失败:", err.Error())
		}
		switch v := data.(type) {
		case []byte:
			fmt.Println(string(v))
		default:
			t.Fatalf("生成的数据不是[]byte类型: %T\n", v)
		}
	}
}
