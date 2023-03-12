(in-package redb)

(defvar *cx*)

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

(defun test-cx ()
  (with-cx ("test" "test" "test")
    (when (not (cx-ok?))
      (error (PQerrorMessage *cx*)))

    (send-query "SELECT * FROM pg_tables" '())
    
    (let* ((r (get-result)))
      (assert (eq (PQresultStatus r) :PGRES_TUPLES_OK))
      (PQclear r))
    (assert (null (get-result)))))
