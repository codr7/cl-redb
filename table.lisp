(in-package redb)

(defclass table (def rel)
  ((primary-key :reader primary-key)
   (foreign-keys :initform nil :reader foreign-keys)
   (def-lookup :initform (make-hash-table))))

(defmethod print-object ((tbl table) out)
  (format out "(Table ~a)" (str! (name tbl))))

(defun map-cols (body rel)
  (let* ((cs (cols rel)) out)
    (dotimes (i (length cs))
      (push (funcall body (aref cs i)) out))
    (nreverse out)))

(defmethod table-add (tbl (col col))
  (with-slots (cols col-indices def-lookup) tbl
    (setf (gethash (name col) def-lookup) col)
    (add-col tbl col)))

(defmethod table-add (tbl (key foreign-key))
  (with-slots (def-lookup foreign-keys) tbl
    (setf (gethash (name key) def-lookup) key)
    (push key foreign-keys)
    
    (do-cols (c key)
      (table-add tbl c))))

(defun new-table (name primary-defs defs)
  (let* ((tbl (make-instance 'table :name name)))
    (dolist (d defs)
      (table-add tbl d))
    
    (with-slots (def-lookup primary-key) tbl
      (let* (primary-cols)
	(dolist (dk primary-defs)
	  (let ((d (gethash dk def-lookup)))
	    (etypecase d
	      (col (push d primary-cols))
	      (key (do-cols (c d) (push c primary-cols))))))
	(setf primary-key
	      (new-key (intern (format nil "~a-primary" name)) primary-cols))))
    
    tbl))

(defun table-exists? (tbl)
  (send-query "SELECT EXISTS (
                 SELECT FROM pg_tables
                 WHERE tablename  = $1
               )"
	      (list (to-sql (name tbl))))
  (let* ((r (get-result)))
    (assert (= (PQntuples r) 1))
    (assert (= (PQnfields r) 1))
    (let* ((result (boolean-from-sql (PQgetvalue r 0 0))))
      (PQclear r)
      result)))

(defmethod exists? ((tbl table))
  (table-exists? tbl))

(defmethod create ((tbl table))
  (let* ((sql (with-output-to-string (out)
		(format out "CREATE TABLE ~a (" (to-sql tbl))

		(with-slots (cols) tbl
		  (dotimes (i (length cols))
		    (unless (zerop i)
		      (format out ", "))
		    
		    (let* ((c (aref cols i)))
		      (format out "~a ~a" (to-sql c) (data-type c))
		      (unless (null? c)
			(format out " NOT NULL")))))
		
		  (format out ")"))))
    (send-query sql nil))
  (multiple-value-bind (result status) (get-result)
    (assert (eq status :PGRES_COMMAND_OK))
    (PQclear result))
  
  (key-create (primary-key tbl) tbl)
  
  (dolist (fk (foreign-keys tbl))
    (key-create fk tbl)))

(defmethod drop ((tbl table))
  (dolist (fk (foreign-keys tbl))
    (key-drop fk tbl))
  
  (let* ((sql (format nil "DROP TABLE IF EXISTS ~a" (to-sql tbl))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  nil)

(defun load-rec (tbl rec result &key (col 0) (row 0))
  (do-cols (c tbl)
    (setf (field rec c) (PQgetvalue result row col))
    (incf col))
  rec)

(defun find-rec (tbl key)
  (let* ((sql (with-output-to-string (out)
		(format out "SELECT ")
		(let* ((i 0))
		  (do-cols (c tbl)
		    (unless (zerop i)
		      (format out ", "))
		    (format out (to-sql c))
		    (incf i)))

		(format out " FROM ~a WHERE " (to-sql tbl))
		
		(let* ((param-count 0))
		  (dolist (k key)
		    (format out "~a=$~a" (to-sql (first k)) (incf param-count)))))))
    (send-query sql (mapcar #'rest key)))
  (multiple-value-bind (result status) (get-result)
    (assert (eq status :PGRES_TUPLES_OK))
    (let ((rec (load-rec tbl (new-rec) result)))
      (PQclear result)
      rec)))

(defun insert-rec (tbl rec)
  (let* (params
	 (sql (with-output-to-string (out)
		(format out "INSERT INTO ~a (" (to-sql tbl))
		(let ((i 0))
		  (do-cols (c tbl)
		    (let-when (v (field rec c))
			(unless (zerop i)
			  (format out ", "))
		      (format out "~a" (to-sql c))
		      (push v params)
		      (incf i))))
		(format out ") VALUES (")
		(dotimes (i (length params))
		  (unless (zerop i)
		    (format out ", "))
		  (format out "$~a" (1+ i)))
		(format out ")"))))
    (send-query sql (mapcar #'rest params)))
  (multiple-value-bind (result status) (get-result)
    (assert (eq status :PGRES_COMMAND_OK))
    (PQclear result))
  nil)

(defun update-rec (tbl rec)
  (let* (params
	 (sql (with-output-to-string (out)
		(format out "UPDATE ~a SET " (to-sql tbl))
		  (do-cols (c tbl)
		    (let-when (v (field rec c))
			(unless (null params)
			  (format out ", "))
		      (push v params)
		      (format out "~a=$~a" (to-sql c) (length params))))
		(format out " WHERE ")
		(let ((i 0))
		  (do-cols (c (primary-key tbl))
		    (let ((v (field rec c)))
		      (assert v)
		      (unless (zerop i)
			(format out " AND "))
		      (push v params)
		      (format out "~a=$~a" (to-sql c) (length params))
		      (incf i)))))))
    (send-query sql params))
  (multiple-value-bind (result status) (get-result)
    (assert (eq status :PGRES_COMMAND_OK))
    (PQclear result))
  nil)

(defun test-table ()
  (with-cx ("test" "test" "test")
    (let* ((tbl (new-table 'foo '(bar) (list (new-string-col 'bar)))))
      (assert (not (exists? tbl)))
      (create tbl)
      (assert (exists? tbl))
      (drop tbl)
      (assert (not (exists? tbl))))))
