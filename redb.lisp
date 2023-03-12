(defpackage redb
  (:use cffi cl redb-pg)
  (:import-from redb-util dohash kw! str! sym! syms!)
  (:import-from slog slog-write)
  (:import-from local-time encode-timestamp format-timestring timestamp)
  (:export *cx* *db*
	   boolean-col
	   col col-from-sql col-to-sql create
	   db define-db def do-cols drop
	   exists?
	   field find-def find-field find-rec
	   get-key get-rec get-result
	   integer-col
	   load-rec
	   name new-boolean-col new-foreign-key new-integer-col new-key
	   new-rec new-string-col new-timestamp-col new-table
	   rec
	   set-key set-rec send-query string-col
	   table table-create table-drop table-exists? timestamp-col to-sql
	   with-cx
	   test))

(in-package redb)

(defmethod to-sql ((self string))
  (let* ((out (copy-seq self)))
    (dotimes (i (length out))
      (let* ((c (char out i)))
	(when (char= c #\-)
	  (setf (char out i) #\_))))
    out))

(defmethod to-sql ((self symbol))
  (to-sql (string-downcase (symbol-name self))))

(defclass def ()
  ((name :initarg :name :initform (error "missing name") :reader name)))

(defmethod to-sql ((self def))
  (to-sql (name self)))

(defclass rel ()
  ((cols :initform (make-array 0 :element-type 'col :fill-pointer 0) :reader cols)
   (col-indices :initform (make-hash-table))))

(defmacro do-cols ((col rel) &body body)
  (let* ((i (gensym)))
    `(dotimes (,i (length (cols ,rel)))
       (let* ((,col (aref (cols ,rel) ,i)))
	 ,@body))))
