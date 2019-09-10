;;;; Thread-safe proirity queue.
;;;; MIT License
;;;; 
;;;; Copyright (c) 2019 gzip4 (gzip4ever@gmail.com)
;;;; 
;;;; Permission is hereby granted, free of charge, to any person obtaining a copy
;;;; of this software and associated documentation files (the "Software"), to deal
;;;; in the Software without restriction, including without limitation the rights
;;;; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
;;;; copies of the Software, and to permit persons to whom the Software is
;;;; furnished to do so, subject to the following conditions:
;;;; 
;;;; The above copyright notice and this permission notice shall be included in all
;;;; copies or substantial portions of the Software.
;;;; 
;;;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
;;;; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
;;;; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
;;;; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
;;;; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
;;;; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
;;;; SOFTWARE.

(in-package #:cl-user)

(defpackage :pqueue
  (:use :cl)
  (:nicknames :pq)
  (:export #:*pqueue*
	   #:quit-condition
	   #:pqueue
	   #:qmax-size
	   #:qsize
	   #:qemptyp
	   #:qclear
	   #:qquit
	   #:qpush
	   #:qpush*
	   #:qpop
	   #:qpop*))

(in-package #:pqueue)

(defvar *pqueue* nil
  "This special variable may be used as default pqueue instance.")

(define-condition quit-condition (condition)
  ()
  (:documentation "The condition may be signalled when qpush/qpop"))

(defclass pqueue ()
  ((data :initform nil)
   (size :initform 0)
   (smax :initform nil :initarg :max-size :accessor qmax-size)
   (quit :initform nil)
   (lock :initform (bt:make-recursive-lock))
   (cvar :initform (bt:make-condition-variable))
   (fvar :initform (bt:make-condition-variable)))
  (:documentation "Use (make-instance 'pq:pqueue) to create priority queue."))

(defmethod initialize-instance :after ((pqueue pqueue)
				       &key initial-contents
					 priority max-size)
  (when initial-contents
    (let ((sz 0))
      (push (cons (or priority 0)
		  (map 'list (lambda (x)
			       (incf sz)
			       (cons x (get-internal-real-time)))
		       initial-contents))
	    (slot-value pqueue 'data))
      (when max-size
	(setf (slot-value pqueue 'smax) (max sz (or max-size 0))))
      (setf (slot-value pqueue 'size) sz))))

(defun qsize (&optional q)
  "Return the amount of items in a queue."
  (with-slots (lock size) (or q *pqueue*)
    (bt:with-recursive-lock-held (lock)
      (values size))))

(defun qemptyp (&optional q)
  "Return T if a queue is empty, NIL otherwise."
  (zerop (qsize (or q *pqueue*))))

(defun qclear (&optional q)
  "Clear a queue removing all items and reset its size."
  (with-slots (lock size data fvar) (or q *pqueue*)
    (bt:with-recursive-lock-held (lock)
      (setf size 0 data nil)
      (bt:condition-notify fvar)))
  (values (or q *pqueue*)))

(defun qquit (v &optional q)
  "Notify all threads waiting on queue that its time to quit.
If parameter is non-nil, a condition `quit-condition' is
signalled in context of `qpop' caller. If parameter is a function
of one argument, it is called before the condition has signalled
with current thread object as a parameter."
  (with-slots (lock cvar fvar quit) (or q *pqueue*)
    (bt:with-recursive-lock-held (lock)
      (setf quit v))
    (when v
      (bt:condition-notify cvar)
      (bt:condition-notify fvar)))
  (values (or q *pqueue*)))

(defun qpush (v &optional (p 0) q)
  "Push any object to the queue with a priority given.
All threads waiting on this queue are notified. If max-size
is reached, a caller is blocked until a new object is popped
from the queue in another thread. A condition `quit-condition'
may be signalled in context of `qpush' caller."
  (with-slots (lock size data cvar fvar smax quit) (or q *pqueue*)
    (bt:with-recursive-lock-held (lock)
      (loop
	 (when quit
	   (when (functionp quit) (funcall quit (bt:current-thread)))
	   (signal 'quit-condition))
	 (when (and smax (< size smax))
	   (let ((pq (find p data :key #'car))
		 (e (cons v (get-internal-real-time))))
	     (incf size)
	     (if pq
		 (nconc pq (list e))
		 (setf data (sort (push (list p e) data) #'< :key #'car))))
	   (return))
	 (bt:condition-wait fvar lock)))
    (bt:condition-notify cvar))
  (values t (or q *pqueue*)))
	     
(defun qpush* (v &optional (p 0) q)
  "Push any object to the queue with a priority given.
All threads waiting on this queue are notified. If `max-size'
is reached, return NIL and do not push an object to the queue.
A condition `quit-condition' may be signalled in context of
`qpush*' caller."
  (with-slots (lock size smax) (or q *pqueue*)
    (bt:with-recursive-lock-held (lock)
      (if (and smax (< size smax))
	  (qpush v p (or q *pqueue*))
	  (values nil (or q *pqueue*))))))
	     
(defun qpop (&optional q)
  "Pop an object with the highest priority from the queue.
If the queue is empty, a caller is blocked until a new object
is pushed to the queue in another thread. A condition
`quit-condition' may be signalled in context of `qpop' caller."
  (with-slots (lock size data cvar fvar quit) (or q *pqueue*)
    (bt:with-recursive-lock-held (lock)
      (loop
	 (when quit
	   (when (functionp quit) (funcall quit (bt:current-thread)))
	   (signal 'quit-condition))
	 (when data
	   (decf size)
	   (let ((p (caar data))
		 (v (pop (cdar data))))
	     (unless (cadar data) (pop data))
	     (bt:condition-notify fvar)
	     (return (values (car v) p (cdr v)))))
	 (bt:condition-wait cvar lock)))))
    
(defun qpop* (&optional q)
  "Pop an object with the highest priority from the queue.
If the queue is empty, return NIL instantly."
  (with-slots (lock data) (or q *pqueue*)
    (bt:with-recursive-lock-held (lock)
      (when data (qpop (or q *pqueue*))))))

(defmethod print-object ((pqueue pqueue) stream)
  (print-unreadable-object (pqueue stream :identity t)
    (with-slots (lock quit) pqueue
      (bt:with-recursive-lock-held (lock)
	(format stream "PQUEUE~a size: ~a"
		(if quit "/QUIT" "")
		(qsize pqueue))))))


