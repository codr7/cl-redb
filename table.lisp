(in-package redb)

(defclass table (def)
  ((cols :initform nil :reader cols)
   (primary-key :initarg :primary-key)
   (keys :initform nil :reader keys)
   (def-lookup :initform (make-hash-table))))

(defmethod print-object ((tbl table) out)
  (format out "(table ~a)" (str! (name tbl))))

(defmethod table-add (tbl (col col))
  (with-slots (cols def-lookup) tbl
    (setf (gethash (name col) def-lookup) col)
    (push col cols)))

(defmethod table-add (tbl (key key))
  (with-slots (def-lookup keys) tbl
    (setf (gethash (name key) def-lookup) key)
    (push key keys)))

(defun new-table (name &rest keys)
  (make-instance 'table :name name :primary-key keys))

(defmethod primary-key ((tbl table))
  (with-slots (def-lookup name primary-key) tbl
    (etypecase primary-key
      (list
       (let (cs)
	 (dolist (d primary-key)
	   (dolist (c (cols (gethash d def-lookup)))
	     (push c cs)))
	 (setf primary-key (apply #'new-key tbl (sym name '-primary) (nreverse cs)))))
      (key
       primary-key))))

(defmethod exists? ((tbl table) &key (cx *cx*))
  (send "SELECT EXISTS (
                 SELECT FROM pg_tables
                 WHERE tablename  = $1
               )"
	`(,(sql-name tbl))
	:cx cx)
  (multiple-value-bind (result status) (recv :cx cx)
    (assert (eq status :PGRES_TUPLES_OK))
    (assert (= (PQntuples result) 1))
    (assert (= (PQnfields result) 1))
    (assert (null (recv :cx cx)))
    
    (let ((exists? (boolean-from-sql (PQgetvalue result 0 0))))
      (PQclear result)
      exists?)))

(defmethod create ((tbl table) &key (cx *cx*))
  (let ((sql (with-output-to-string (out)
	       (format out "CREATE TABLE ~a (" (sql-name tbl))

		 (let ((i 0))
		   (dolist (c (cols tbl))
		     (unless (zerop i)
		       (format out ", "))
		     
		     (format out "~a ~a" (sql-name c) (data-type c))
		     (unless (null? c)
		       (format out " NOT NULL"))

		     (incf i)))
	       
	       (format out ")"))))
    (send-command sql nil :cx cx))

  (let ((pk (primary-key tbl)))
    (create pk :cx cx)
    
    (dolist (k (keys tbl))
      (unless (eq k pk)
	(create k :cx cx))))
  nil)

(defmethod drop ((tbl table) &key (cx *cx*))
  (dolist (k (keys tbl))
    (drop k :cx cx))
  
  (let ((sql (format nil "DROP TABLE IF EXISTS ~a" (sql-name tbl))))
    (send-command sql nil :cx cx))
  
  nil)

(defun load-rec (tbl rec result &key (col 0) (row 0))
  (dolist (c (cols tbl))
    (setf (field rec c) (PQgetvalue result row col))
    (incf col))
  rec)

(defun find-rec (tbl rec &key (cx *cx*))
  (let* (params
	(sql (with-output-to-string (out)
	       (format out "SELECT ")
	       (let ((i 0))
		 (dolist (c (cols tbl))
		   (unless (zerop i)
		     (format out ", "))
		   (format out (sql-name c))
		   (incf i)))
	       
	       (format out " FROM ~a WHERE " (sql-name tbl))
	       
	       (dolist (c (cols (primary-key tbl)))
		 (push (field rec c) params)
		 (format out "~a=$~a" (sql-name c) (length params))))))
    (send sql params :cx cx))

  (multiple-value-bind (result status) (recv :cx cx)
    (assert (eq status :PGRES_TUPLES_OK))
    (assert (null (recv :cx cx)))
    (load-rec tbl (new-rec) result)
    (PQclear result)
    rec))

(defun insert-rec (tbl rec &key (cx *cx*))
  (let* (params
	 (sql (with-output-to-string (out)
		(format out "INSERT INTO ~a (" (sql-name tbl))
		(let ((i 0))
		  (dolist (c (cols tbl))
		    (let-when (v (field rec c))
		      (unless (zerop i)
			(format out ", "))
		      (format out "~a" (sql-name c))
		      (push v params)
		      (incf i))))
		(format out ") VALUES (")
		(dotimes (i (length params))
		  (unless (zerop i)
		    (format out ", "))
		  (format out "$~a" (1+ i)))
		(format out ")"))))
    (send-command sql (mapcar #'rest params) :cx cx))
  nil)

(defun update-rec (tbl rec &key (cx *cx*))
  (let* (params
	 (sql (with-output-to-string (out)
		(format out "UPDATE ~a SET " (sql-name tbl))
		(dolist (c (cols tbl))
		  (let-when (v (field rec c))
		    (unless (null params)
		      (format out ", "))
		    (push v params)
		    (format out "~a=$~a" (sql-name c) (length params))))
		(format out " WHERE ")
		(let ((i 0))
		  (dolist (c (cols (primary-key tbl)))
		    (let ((v (field rec c)))
		      (assert v)
		      (unless (zerop i)
			(format out " AND "))
		      (push v params)
		      (format out "~a=$~a" (sql-name c) (length params))
		      (incf i)))))))
    (send-command sql params :cx cx))
  nil)

(defun test-table ()
  (with-cx ("test" "test" "test")
    (let* ((tbl (new-table :foo :bar))
	   (col (new-integer-col tbl :bar)))
      (assert (not (exists? tbl)))
      (create tbl)
      (assert (exists? tbl))
      (assert (exists? col))
      (drop tbl)
      (assert (not (exists? tbl))))))
