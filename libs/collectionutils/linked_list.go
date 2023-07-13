package collectionutils

import (
	"sync"
)

type LinkedListNode[T any] struct {
	object                 T
	list                   *LinkedList[T]
	previousNode, nextNode *LinkedListNode[T]
}

func (n *LinkedListNode[T]) Next() *LinkedListNode[T] {
	return n.nextNode
}

func (n *LinkedListNode[T]) Value() T {
	return n.object
}

func (n *LinkedListNode[T]) Remove() {
	if n.previousNode != nil {
		n.previousNode.nextNode = n.nextNode
	}
	if n.nextNode != nil {
		n.nextNode.previousNode = n.previousNode
	}
	if n == n.list.firstNode {
		n.list.firstNode = n.nextNode
	}
	if n == n.list.lastNode {
		n.list.lastNode = n.previousNode
	}
}

func (n *LinkedListNode[T]) ForEach(f func(t T)) {
	f(n.object)
	if n.nextNode == nil {
		return
	}
	n.nextNode.ForEach(f)
}

type LinkedList[T any] struct {
	mu                  sync.Mutex
	firstNode, lastNode *LinkedListNode[T]
}

func (r *LinkedList[T]) Append(objects ...T) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, object := range objects {
		if r.firstNode == nil {
			r.firstNode = &LinkedListNode[T]{
				object: object,
				list:   r,
			}
			r.lastNode = r.firstNode
			continue
		}
		r.lastNode = &LinkedListNode[T]{
			object:       object,
			previousNode: r.lastNode,
			list:         r,
		}
		r.lastNode.previousNode.nextNode = r.lastNode
	}
}

func (r *LinkedList[T]) RemoveFirst(cmp func(T) bool) *LinkedListNode[T] {
	r.mu.Lock()
	defer r.mu.Unlock()

	node := r.firstNode
	for node != nil {
		if cmp(node.object) {
			node.Remove()
			return node
		}
		node = node.nextNode
	}

	return nil
}

func (r *LinkedList[T]) RemoveValue(t T) *LinkedListNode[T] {
	return r.RemoveFirst(func(t2 T) bool {
		return (any)(t) == (any)(t2)
	})
}

func (r *LinkedList[T]) TakeFirst() T {
	var t T
	if r.firstNode == nil {
		return t
	}
	ret := r.firstNode.object
	if r.firstNode.nextNode == nil {
		r.firstNode = nil
	} else {
		r.firstNode = r.firstNode.nextNode
		r.firstNode.previousNode = nil
	}
	return ret
}

func (r *LinkedList[T]) Length() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	count := 0

	node := r.firstNode
	for node != nil {
		count++
		node = node.nextNode
	}

	return count
}

func (r *LinkedList[T]) ForEach(f func(t T)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.firstNode == nil {
		return
	}

	r.firstNode.ForEach(f)
}

func (r *LinkedList[T]) Slice() []T {
	ret := make([]T, 0)
	node := r.firstNode
	for node != nil {
		ret = append(ret, node.object)
		node = node.nextNode
	}
	return ret
}

func (r *LinkedList[T]) FirstNode() *LinkedListNode[T] {
	return r.firstNode
}

func NewLinkedList[T any]() *LinkedList[T] {
	return &LinkedList[T]{}
}
