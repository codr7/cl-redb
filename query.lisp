(in-package redb)

(defstruct query
  (select nil :type list)
  (from nil :type list)
  (where nil :type list)
  (order nil :type list)
  (tables nil :type list))

(defun new-query ()
  (make-query))

(defun join-fkey (qry key)
  (let ((ft (foreign-table key)))
    (unless (member ft (query-tables qry))
      (push ft (query-tables qry)))
  
    (push (with-output-to-string (sql)
	    (format sql "JOIN ~a ON " (sql-name ft))
	    (let ((i 0))
	      (dolist (c (foreign-cols key))
		(unless (zerop i)
		  (write-string " AND " sql))
		(format sql "~a = ~a" (sql (first c)) (sql (rest c)))
		(incf i))))
	  (query-from qry))))

(defun join-table (qry tbl)
  (or (member tbl (query-tables qry))
      (progn 
	(dolist (qt (query-tables qry))
	  (dolist (k (fkeys qt))
	    (when (eq (foreign-table k) tbl)
	      (join-fkey qry k)
	      (return-from join-table t))))
	nil)))
  
(defun select (qry &rest defs)
  (dolist (d defs)
    (let ((tbl (table d)))
      (if (null (query-from qry))
	  (progn
	    (push tbl (query-tables qry))
	    (push tbl (query-from qry)))
	  (unless (join-table qry tbl)
	    (error "Failed joining table: ~a" (name tbl)))))
    
    (dolist (c (cols d))
      (push c (query-select qry)))))
   
(defmethod sql ((qry query))
  (with-output-to-string (sql)
    (write-string "SELECT " sql)
    
    (let ((i 0))
      (dolist (col (reverse (query-select qry)))
	(unless (zerop i)
	  (write-string ", " sql))
	
	(write-string (sql col) sql)
	(incf i)))
    
    (let-when (from (reverse (query-from qry)))
      (write-string " FROM" sql)
      
      (dolist (src from)
	(format sql " ~a" (sql src))))

    (let-when (where (reverse (query-where qry)))
      (write-string " WHERE" sql)
      
      (let ((i 0))
	(dolist (cnd where)
	  (unless (zerop i)
	    (write-string "AND " sql))
	  (write-string (sql cnd) sql)
	  (incf i))))

    (let-when (order (reverse (query-order qry)))
      (write-string " ORDER BY " sql)
      
      (let ((i 0))
	(dolist (col order)
	  (unless (zerop i)
	    (write-string ", " sql))
	  (write-string (sql col) sql)
	  (incf i))))))

(defmethod params ((qry query))
  (let (params)
    (flet ((push-params (in)
	     (dolist (p in)
	       (push p params))))
      (dolist (col (query-select qry))
	(push-params (params col)))
      
      (dolist (src (query-from qry))
	(push-params (params src)))

      (dolist (cnd (query-where qry))
	(push-params (params cnd)))

      (dolist (col (query-order qry))
	(push-params (params col))))))

(defmethod exec ((qry query))
  (multiple-value-bind (result status) (recv)
    (assert (eq status :PGRES_TUPLES_OK))
    result))

(defmacro with-result ((qry) &body body)
  (let (($qry (gensym)) ($result (gensym)) ($row (gensym)))
    `(let ((,$qry ,qry) ,$result (,$row 0))
       (send (sql ,$qry) (params ,$qry))
       (multiple-value-bind (result status) (recv)			
	 (assert (eq status :PGRES_TUPLES_OK))
	 (setf ,$result result))
       (macrolet ((next ()
		    (let ((qry ',$qry) (result ',$result) (row ',$row))
		      `(when (< ,row (PQntuples ,result))
			 (let ((rec (load-rec (new-rec)
					      (query-select ,qry)
					      ,result
					      :row ,row)))
			   (incf ,row)
			   rec)))))
	 ,@body)
       (assert (null (recv)))
       (PQclear ,$result))))

  
