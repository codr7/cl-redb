(in-package redb)

(defclass table (def rel)
  ((primary-key :reader primary-key)
   (foreign-keys :initform nil :reader foreign-keys)
   (def-lookup :initform (make-hash-table))))

(defmethod print-object ((self table) out)
  (format out "(Table ~a)" (str! (name self))))

(defun map-cols (body rel)
  (let* ((cs (cols rel)) out)
    (dotimes (i (length cs))
      (push (funcall body (aref cs i)) out))
    (nreverse out)))

(defmethod table-add (table (col col))
  (with-slots (cols col-indices def-lookup) table
    (setf (gethash (name col) def-lookup) col)
    (setf (gethash (name col) col-indices) (length cols))
    (vector-push-extend col cols)))

(defmethod table-add (table (key foreign-key))
  (with-slots (def-lookup foreign-keys) table
    (setf (gethash (name key) def-lookup) key)
    (push key foreign-keys)
    
    (do-cols (c key)
      (table-add table c))))

(defun new-table (name primary-defs defs)
  (let* ((table (make-instance 'table :name name)))
    (dolist (d defs)
      (table-add table d))
    
    (with-slots (cols col-indices def-lookup primary-key) table
      (let* (primary-cols)
	(dolist (dk primary-defs)
	  (let ((d (gethash dk def-lookup)))
	    (etypecase d
	      (col (push d primary-cols))
	      (key (do-cols (c d) (push c primary-cols))))))
	(setf primary-key
	      (new-key (intern (format nil "~a-primary" name)) primary-cols))))
    
    table))

(defun table-exists? (self)
  (send-query "SELECT EXISTS (
                 SELECT FROM pg_tables
                 WHERE tablename  = $1
               )"
	      (list (to-sql (name self))))
  (let* ((r (get-result)))
    (assert (= (PQntuples r) 1))
    (assert (= (PQnfields r) 1))
    (let* ((result (boolean-from-sql (PQgetvalue r 0 0))))
      (PQclear r)
      (assert (null (get-result)))
      result)))

(defmethod exists? ((self table))
  (table-exists? self))

(defun table-create (self)
  (let* ((sql (with-output-to-string (out)
		(format out "CREATE TABLE ~a (" (to-sql self))
		(with-slots (cols) self
		  (dotimes (i (length cols))
		    (unless (zerop i)
		      (format out ", "))
		    
		    (let* ((c (aref cols i)))
		      (format out "~a ~a NOT NULL" (to-sql c) (data-type c))))
		  (format out ")")))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  
  (assert (null (get-result)))
  (key-create (primary-key self) self)
  
  (dolist (fk (foreign-keys self))
    (key-create fk self)))

(defmethod create ((self table))
  (table-create self))

(defun table-drop (self)
  (dolist (fk (foreign-keys self))
    (key-drop fk self))
  
  (let* ((sql (format nil "DROP TABLE IF EXISTS ~a" (to-sql self))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defmethod drop ((self table))
  (table-drop self))

(defun load-rec (table result &key rec (offset 0))
  (let* ((i offset))
    (do-cols (c table)
      (push (cons c (PQgetvalue result 0 i)) rec)
      (incf i))
    rec))

(defun find-rec (table key)
  (let* ((sql (with-output-to-string (out)
		(format out "SELECT ")
		(let* ((i 0))
		  (do-cols (c table)
		    (unless (zerop i)
		      (format out ", "))
		    (format out (to-sql c))
		    (incf i)))

		(format out " FROM ~a WHERE " (to-sql table))
		
		(let* ((param-count 0))
		  (dolist (k key)
		    (format out "~a=$~a" (to-sql (first k)) (incf param-count)))))))
    (send-query sql (mapcar #'rest key)))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_TUPLES_OK))
    (let ((rec (load-rec table r)))
      (PQclear r)
      (assert (null (get-result)))
      rec)))

(defun insert-rec (table rec)
  (let* ((sql (with-output-to-string (out)
		(format out "INSERT INTO ~a (" (to-sql table))
		(with-slots (col-indices) table
		  (dolist (f rec)
		    (let* ((c (first f)))
		      (unless (gethash (name c) col-indices)
			(error "unknown col: ~a" c))
		      (unless (eq f (first rec))
			(format out ", "))
		      (format out "~a" (to-sql c)))))
		(format out ") VALUES (")
		(dotimes (i (length rec))
		  (unless (zerop i)
		    (format out ", "))
		  (format out "$~a" (1+ i)))
		(format out ")"))))
    (send-query sql (mapcar #'rest rec)))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defun update-rec (table rec)
  (let* ((sql (with-output-to-string (out)
		(format out "UPDATE ~a SET " (to-sql table))
		(let* ((param-count 0))
		  (with-slots (col-indices cols) table
		    (dolist (f rec)
		      (let* ((c (first f)))
			(unless (gethash (name c) col-indices)
			  (error "unknown col: ~a" c))
			(unless (eq f (first rec))
			  (format out ", "))
			(format out "~a=$~a" (to-sql c) (incf param-count)))))
		  (format out " WHERE "))
		(let* ((i 0))
		  (dolist (f rec)
		    (unless (zerop i)
		      (format out " AND "))
		    (format out "~a=$~a" (to-sql (first f)) (incf i)))))))
    (send-query sql (mapcar #'rest rec)))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defun test-table ()
  (with-cx ("test" "test" "test")
    (let* ((table (new-table 'foo '(bar) (list (new-string-col 'bar)))))
      (assert (not (exists? table)))
      (create table)
      (assert (exists? table))
      (drop table)
      (assert (not (exists? table))))))
