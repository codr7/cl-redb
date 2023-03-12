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

(defvar *cx*)
(defvar *db*)

(defun cx-ok? (&key (cx *cx*))
  (eq (PQstatus cx) :CONNECTION_OK))

(defun connect (db user password &key (host "localhost"))
  (let* ((c (PQconnectdb (format nil "postgresql://~a:~a@~a/~a" user password host db))))
    (unless (cx-ok? :cx c)
      (error (PQerrorMessage c)))
    c))

(defun send-query (sql params &key (cx *cx*))
  (slog-write "send-query" :tag :db :sql sql :params params)

  (let* ((nparams (length params)))
    (with-foreign-object (cparams :pointer nparams)
      (let* ((i 0))
	(dolist (p params)
	  (setf (mem-aref cparams :pointer i) (foreign-string-alloc p))
	  (incf i)))
      
      (unless (= (PQsendQueryParams cx
				    sql
				    nparams
				    (null-pointer) cparams (null-pointer) (null-pointer)
				    0)
		 1)
	(error (PQerrorMessage cx)))
      
      (dotimes (i (length params))
	(foreign-string-free (mem-aref cparams :pointer i))))))

(defun get-result (&key (cx *cx*))
  (let* ((r (PQgetResult cx)))
    (if (null-pointer-p r)
	(values nil nil)
	(let* ((s (PQresultStatus r)))
	  (unless (or (eq s :PGRES_COMMAND_OK) (eq s :PGRES_TUPLES_OK))
	    (error "~a~%~a" s (PQresultErrorMessage r)))
	  (values r s)))))

(defmacro with-cx ((&rest args) &body body)
  `(let* ((*cx* (connect ,@args)))
     (unwind-protect
	  (progn ,@body)
       (PQfinish *cx*))))

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

(defun test ()
  (with-cx ("test" "test" "test")
    (when (not (cx-ok?))
      (error (PQerrorMessage *cx*)))

    (send-query "SELECT * FROM pg_tables" '())
    
    (let* ((r (get-result)))
      (assert (eq (PQresultStatus r) :PGRES_TUPLES_OK))
      (PQclear r))
    (assert (null (get-result)))
    
    (let* ((table (new-table 'foo '(bar) (list (new-string-col 'bar)))))
      (assert (not (exists? table)))
      (create table)
      (assert (exists? table))
      (drop table)
      (assert (not (exists? table))))))
