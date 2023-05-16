package org.janelia.jacs2.asyncservice.common;

import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentStack<E> {
    private static class Node<E> {
        private final E item;
        private Node<E> next;

        private Node(E item) {
            this.item = item;
        }
    }

    private final AtomicReference<Node<E>> head = new AtomicReference<>();

    public void push(E item) {
        Node<E> newHead = new Node<>(item);
        Node<E> oldHead;
        do {
            oldHead = head.get();
            newHead.next = oldHead;
        } while (!head.compareAndSet(oldHead, newHead));
    }

    public E top() {
        Node<E> headContent = head.get();
        if (headContent == null) {
            return null;
        } else {
            return headContent.item;
        }
    }

    public E pop() {
        Node<E> oldHead;
        Node<E> newHead;
        do {
            oldHead = head.get();
            if (oldHead == null) {
                return null;
            }
            newHead = oldHead.next;
        } while (!head.compareAndSet(oldHead, newHead));
        return oldHead.item;
    }
}
