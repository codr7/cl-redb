(in-package redb)

(defvar *db*)

(defclass db ()
  ((defs :initform nil :reader defs)
   (def-lookup :initform (make-hash-table) :reader def-lookup)))

(defun find-def (name &key (db *db*))
  (gethash name (def-lookup db)))

(defmacro with-db ((id) &body body)
  `(let ((*db* (make-instance ',id)))
     ,@body))

(defmacro define-db (db-id &body forms)
  (let (slots def-forms init-forms)
    (labels ((parse-table (f)
	       (let ((table-name (pop f))
		     (keys (pop f)))
		 (labels ((parse-col (f)
			    (let* ((name (pop f))
				   (type (pop f))
				   (slot-name (sym table-name '- name)))
			      (push slot-name slots)
			      (push `(setf (slot-value db ',slot-name)
					   (,(sym 'new- type '-col) (,table-name db) ',name ,@f))
				    init-forms)))
			  
			  (parse-foreign-key (f)
			    (let* ((name (pop f))
				   (foreign-table (pop f))
				   (slot-name (sym table-name '- name)))
			      (push slot-name slots)
			      (push `(setf (slot-value db ',slot-name)
					   (new-foreign-key (,table-name db)
							    ',name
							    (,foreign-table db)
							    ,@f))
				    init-forms)))
			  
			  (parse-table-form (f)
			    (ecase (kw (first f))
			      (:column
			       (parse-col (rest f)))
			      (:foreign-key
			       (parse-foreign-key (rest f))))))
		   (push table-name slots)
		   
		   (push `(let ((tbl (apply #'new-table ',table-name '(,@keys))))
			    (push tbl defs)
			    (setf (gethash ',table-name def-lookup) tbl)
			    (setf (slot-value db ',table-name) tbl))
			 init-forms)
		   
		   (dolist (tf f)
		     (parse-table-form tf)))))
	     (parse-enum (f)
	       (let* ((name (first f))
		      (ct (sym name '-col)))
		 (push `(define-col-type (enum-col) ,ct ,(sql-name name))
		       def-forms)
		 (push `(let ((enum (new-enum ',name ,@(mapcar #'kw (rest f)))))
			  (push enum defs)
			  (setf (gethash ',name def-lookup) enum))
		       init-forms)))
	     (parse-form (f)
	       (ecase (kw (first f))
		 (:enum (parse-enum (rest f)))
		 (:table (parse-table (rest f))))))
      (dolist (f forms)
	(parse-form f))
      `(progn
	 (defclass ,db-id (db)
	   (,@(mapcar (lambda (n) `(,n :reader ,n)) slots)))

	 ,@def-forms
	 (defmethod initialize-instance :after ((db ,db-id) &key)
	   (with-slots (def-lookup defs) db
	     ,@(nreverse init-forms)))))))

(defmethod create ((db db) &key (cx *cx*))
  (dolist (d (reverse (defs db)))
    (unless (exists? d :cx cx)
      (create d :cx cx))))

(defmethod drop ((db db) &key (cx *cx*))
  (dolist (d (defs db))
    (when (exists? d :cx cx)
	(drop d :cx cx))))

(define-db test-db
  (table users (alias)
	 (column alias text)
	 (column name1 text)
	 (column name2 text))
  (enum event-type
	user-created
	user-updated)
  (table events (id)
	 (column id bigint)
	 (column type event-type)
	 (column at timestamp)
	 (foreign-key by users)))

(defun test-db ()
  (with-db (test-db)
    (assert (= (length (cols (users *db*))) 3))
    (assert (= (length (cols (primary-key (users *db*)))) 1))
    
    (with-cx ("test" "test" "test")
      (create *db*)
      (drop *db*))))
