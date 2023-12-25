package main
/// https://tour.golang.org/concurrency/8

import (
	"fmt"
)

func Walk(t *tree.Tree, ch chan int) {
	if t == nil {
		return
	}

	Walk(t.Left, ch)
	ch <- t.Value
	Walk(t.Right, ch)
}

func Same(t1, t2 *tree.Tree) bool {
	ch1 := make(chan int, 10)
	ch2 := make(chan int, 10)

	go func() {
		defer close(ch1)
		Walk(t1, ch1)
	}()

	go func() {
		defer close(ch2)
		Walk(t2, ch2)
	}()

	for {
		val1, ok1 := <-ch1
		val2, ok2 := <-ch2

		if val1 != val2 || (ok1 != ok2) {
			return false
		}

		if !ok1 && !ok2 {
			break
		}
	}

	return true
}

func main() {
	result1 := Same(tree.New(1), tree.New(1))
	fmt.Println(result1)

	result2 := Same(tree.New(1), tree.New(2))
	fmt.Println(result2)
}

/*
Changes and Reasons:
Buffered Channels: In the final code, the channels (ch1 and ch2) are declared as buffered channels with a capacity of 10. This avoids blocking issues when sending on channels without a receiver. In the initial code, unbuffered channels were used, which could lead to a deadlock if the receiver is not ready to receive.

Deferred Channel Closing: In the final code, the defer close(ch1) and defer close(ch2) statements ensure that the channels are closed only after all values have been sent into them. In the initial code, channels were closed immediately after sending a value, which could lead to premature closing and panic when attempting to send on a closed channel.

By using buffered channels and deferring channel closing, the final code addresses potential deadlock issues and ensures proper synchronization between the goroutines sending and receiving values.
*/
