(in-package redb)

(defclass table (def)
  ((cols :initform nil :reader cols)
   (pkey :initarg :pkey :reader pkey)
   (fkeys :initform nil :reader fkeys)
   (keys :initform nil :reader keys)
   (indexes :initform nil :reader indexes)
   (def-lookup :initform (make-hash-table))))

(defmethod print-object ((tbl table) out)
  (format out "(table ~a)" (str! (name tbl))))

(defmethod table ((tbl table))
  tbl)

(defmethod table-add (tbl (col col))
  (with-slots (cols def-lookup) tbl
    (setf (gethash (name col) def-lookup) col)
    (push col cols)))

(defmethod table-add (tbl (idx index))
  (with-slots (def-lookup indexes) tbl
    (setf (gethash (name idx) def-lookup) idx)
    (push idx indexes)))

(defmethod table-add (tbl (key key))
  (with-slots (def-lookup keys) tbl
    (setf (gethash (name key) def-lookup) key)
    (push key keys)))

(defmethod table-add (tbl (key fkey))
  (call-next-method)
  
  (with-slots (fkeys) tbl
    (push key fkeys)))

(defun new-table (name &rest keys)
  (make-instance 'table :name name :pkey keys))

(defmethod pkey ((tbl table))
  (with-slots (def-lookup name pkey) tbl
    (etypecase pkey
      (list
       (let (cs)
	 (dolist (d pkey)
	   (dolist (c (cols (gethash d def-lookup)))
	     (push c cs)))
	 (setf pkey (apply #'new-key tbl (kw name '-primary) (nreverse cs)))))
      (key
       pkey))))

(defmethod exists? ((tbl table))
  (boolean-from-sql (send-val "SELECT EXISTS (
                                 SELECT FROM pg_tables
                                 WHERE tablename  = $1
                               )"
			      `(,(sql-name tbl)))))

(defmethod create ((tbl table))
  (if (exists? tbl)
      (dolist (c (cols tbl))
	(create c)))
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
	(send-cmd sql nil))

  (let ((pk (pkey tbl)))
    (create pk)
    
    (dolist (k (keys tbl))
      (unless (eq k pk)
	(create k))))

    (dolist (i (indexes tbl))
	(create i))
  nil)

(defmethod drop ((tbl table))
  (when (exists? tbl)
    (dolist (k (keys tbl))
      (drop k))

    (dolist (i (indexes tbl))
	(drop i))

    (let ((sql (with-output-to-string (out)
		 (format out "DROP TABLE ~a" (sql-name tbl)))))
      (send-cmd sql nil)))
  t)

(defun find-rec (tbl &rest keys)
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
	       
	       (dolist (c (cols (pkey tbl)))
		 (push (pop keys) params)
		 (format out "~a=$~a" (sql-name c) (length params))))))
    (send sql params))

  (multiple-value-bind (result status) (recv)
    (assert (eq status :PGRES_TUPLES_OK))
    (assert (null (recv)))
    result))

(defun insert-rec (tbl rec)
  (let* (params
	 (sql (with-output-to-string (out)
		(format out "INSERT INTO ~a (" (sql-name tbl))

		(let ((i 0))
		  (dolist (c (cols tbl))
		    (let-when (v (field rec c))
		      (unless (zerop i)
			(format out ", "))

		      (format out "~a" (sql-name c))
		      (push (to-sql c v) params)
		      (incf i))))

		(format out ") VALUES (")
		
		(dotimes (i (length params))
		  (unless (zerop i)
		    (format out ", "))
		  (format out "$~a" (1+ i)))
		(format out ")"))))
    (assert (= (send-dml sql (nreverse params)) 1)))
  nil)

(defun update-rec (tbl rec)
  (let* (params
	 (sql (with-output-to-string (out)
		(format out "UPDATE ~a SET " (sql-name tbl))

		(dolist (c (cols tbl))
		  (let-when (v (field rec c))
		    (unless (null params)
		      (format out ", "))
		    (push (to-sql c v) params)
		    (format out "~a=$~a" (sql-name c) (length params))))

		(format out " WHERE ")

		(let ((i 0))
		  (dolist (c (cols (pkey tbl)))
		    (let ((v (stored-field rec c)))
		      (assert v)

		      (unless (zerop i)
			(format out " AND "))
		      
		      (push (to-sql c v) params)
		      (format out "~a=$~a" (sql-name c) (length params))
		      (incf i)))))))
    (assert (= (send-dml sql (nreverse params)) 1)))
  nil)

(defun delete-rec (tbl rec)
  (let* (params
	 (sql (with-output-to-string (out)
		(format out "DELETE FROM ~a WHERE " (sql-name tbl))

		(let ((i 0))
		  (dolist (c (cols (pkey tbl)))
		    (let ((v (stored-field rec c)))
		      (assert v)

		      (unless (zerop i)
			(format out " AND "))
		      
		      (push (to-sql c v) params)
		      (format out "~a=$~a" (sql-name c) (length params))
		      (incf i)))))))
    (assert (= (send-dml sql (nreverse params)) 1)))
  nil)
