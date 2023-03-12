(in-package redb)

(defclass table (def rel)
  ((primary-key :initarg :primary-key)
   (keys :initform nil :reader keys)
   (def-lookup :initform (make-hash-table))))

(defmethod print-object ((tbl table) out)
  (format out "(table ~a)" (str! (name tbl))))

(defun map-cols (body rel)
  (let ((cs (cols rel)) out)
    (dotimes (i (length cs))
      (push (funcall body (aref cs i)) out))
    (nreverse out)))

(defmethod table-add (tbl (col col))
  (with-slots (def-lookup) tbl
    (setf (gethash (name col) def-lookup) col)
    (add-col tbl col)))

(defmethod table-add (tbl (key key))
  (with-slots (def-lookup keys) tbl
    (setf (gethash (name key) def-lookup) key)
    (push key keys)))

(defmethod table-add (tbl (key foreign-key))
  (with-slots (def-lookup keys) tbl
    (setf (gethash (name key) def-lookup) key)
    (push key keys)
    
    (do-cols (c key)
      (table-add tbl c))))

(defun new-table (name &rest keys)
  (make-instance 'table :name name :primary-key keys))

(defmethod primary-key ((tbl table))
  (with-slots (def-lookup name primary-key) tbl
    (etypecase primary-key
      (list
       (let (cs)
	 (dolist (d primary-key)
	   (do-cols (c (gethash d def-lookup))
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

	       (with-slots (cols) tbl
		 (dotimes (i (length cols))
		   (unless (zerop i)
		     (format out ", "))
		   
		   (let ((c (aref cols i)))
		     (format out "~a ~a" (sql-name c) (data-type c))
		     (unless (null? c)
		       (format out " NOT NULL")))))
	       
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
  (do-cols (c tbl)
    (setf (field rec c) (PQgetvalue result row col))
    (incf col))
  rec)

(defun find-rec (tbl key &key (cx *cx*))
  (let ((sql (with-output-to-string (out)
	       (format out "SELECT ")
	       (let ((i 0))
		 (do-cols (c tbl)
		   (unless (zerop i)
		     (format out ", "))
		   (format out (sql-name c))
		   (incf i)))

	       (format out " FROM ~a WHERE " (sql-name tbl))
	       
	       (let ((param-count 0))
		 (dolist (k key)
		   (format out "~a=$~a" (sql-name (first k)) (incf param-count)))))))
    (send sql (mapcar #'rest key) :cx cx))
  
  (multiple-value-bind (result status) (recv :cx cx)
    (assert (eq status :PGRES_TUPLES_OK))
    (assert (null (recv :cx cx)))
    
    (let ((rec (load-rec tbl (new-rec) result)))
      (PQclear result)
      rec)))

(defun insert-rec (tbl rec &key (cx *cx*))
  (let* (params
	 (sql (with-output-to-string (out)
		(format out "INSERT INTO ~a (" (sql-name tbl))
		(let ((i 0))
		  (do-cols (c tbl)
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
		(do-cols (c tbl)
		  (let-when (v (field rec c))
		    (unless (null params)
		      (format out ", "))
		    (push v params)
		    (format out "~a=$~a" (sql-name c) (length params))))
		(format out " WHERE ")
		(let ((i 0))
		  (do-cols (c (primary-key tbl))
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
