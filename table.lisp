(in-package redb)

(defclass table (def)
  ((cols :initform nil :reader cols)
   (pkey :initarg :pkey)
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
  (make-instance 'table :name name :pkey keys))

(defmethod pkey ((tbl table))
  (with-slots (def-lookup name pkey) tbl
    (etypecase pkey
      (list
       (let (cs)
	 (dolist (d pkey)
	   (dolist (c (cols (gethash d def-lookup)))
	     (push c cs)))
	 (setf pkey (apply #'new-key tbl (sym name '-primary) (nreverse cs)))))
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
	(send-dml sql nil))

  (let ((pk (pkey tbl)))
    (create pk)
    
    (dolist (k (keys tbl))
      (unless (eq k pk)
	(create k))))
  nil)

(defmethod drop ((tbl table))
  (when (exists? tbl)
    (dolist (k (keys tbl))
      (drop k))

    (let ((sql (with-output-to-string (out)
		 (format out "DROP TABLE ~a" (sql-name tbl)))))
      (send-dml sql nil))

    t))

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
		      (push v params)
		      (incf i))))

		(format out ") VALUES (")
		
		(dotimes (i (length params))
		  (unless (zerop i)
		    (format out ", "))
		  (format out "$~a" (1+ i)))
		(format out ")"))))
    (send-dml sql params))
  nil)

(defun update-rec (tbl rec)
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
		  (dolist (c (cols (pkey tbl)))
		    (let ((v (field rec c)))
		      (assert v)

		      (unless (zerop i)
			(format out " AND "))
		      
		      (push v params)
		      (format out "~a=$~a" (sql-name c) (length params))
		      (incf i)))))))
    (send-dml sql params))
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
