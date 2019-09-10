(in-package :cl-user)

(defpackage :pqueue-system
  (:use :cl :asdf))

(in-package :pqueue-system)

(defsystem :pqueue
  :version "0.1"
  :description "Thread-safe priority queue"
  :maintainer "Kirill Zverev <gzip4ever@gmail.com>"
  :author "gzip4 <https://github.com/gzip4/cl-pqueue>"
  :licence "MIT"
  :depends-on ("bordeaux-threads")
  :serial t
  :components ((:file "pqueue")))
