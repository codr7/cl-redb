(in-package redb)

(defvar *db*)

(defclass db ()
  ((defs :initform nil :reader defs)
   (def-lookup :initform (make-hash-table) :reader def-lookup)))

(defun find-def (def &rest names)
  (with-slots (def-lookup) def
    (setf def (gethash (pop names) def-lookup))
    (if names
	(apply #'find-def def names)
	def)))

(defmacro with-db ((id) &body body)
  `(let ((*db* (make-instance ',id)))
     ,@body))

(defmacro db (&rest names)
  `(apply #'find-def *db* '(,@names)))

(defmacro define-db (db-id &body forms)
  (let (def-forms init-forms)
    (labels ((parse-table (f)
	       (let ((table-name (pop f))
		     (keys (pop f)))
		 (labels ((parse-col (f)
			    (let* ((name (pop f))
				   (type (pop f)))
			      (push `(,(sym 'new- type '-col) (db ,table-name) ',name ,@f)
				    init-forms)))
			  
			  (parse-foreign-key (f)
			    (let* ((name (pop f))
				   (foreign-table (pop f)))
			      (push `(new-foreign-key (db ,table-name)
						      ',name
						      (db ,foreign-table)
						      ,@f)
				    init-forms)))
			  
			  (parse-table-form (f)
			    (ecase (kw (first f))
			      (:column
			       (parse-col (rest f)))
			      (:foreign-key
			       (parse-foreign-key (rest f))))))
		   
		   (push `(let ((tbl (apply #'new-table ',table-name '(,@keys))))
			    (push tbl defs)
			    (setf (gethash ',table-name def-lookup) tbl))
			 init-forms)
		   
		   (dolist (tf f)
		     (parse-table-form tf)))))
	     (parse-enum (f)
	       (let ((name (first f)))
		 (push `(define-col-type (enum) ,name ,(sql-name name))
		       def-forms)
		 (push `(let ((enum (new-enum ',name ,@(mapcar #'kw (rest f)))))
			  (push enum defs)
			  (setf (gethash ',name def-lookup) enum))
		       init-forms)))
	     (parse-seq (f)
	       (let ((name (first f)))
		 (push `(let ((seq (new-seq ',name ,@(rest f))))
			  (push seq defs)
			  (setf (gethash ',name def-lookup) seq))
		       init-forms)))
	     (parse-form (f)
	       (ecase (kw (first f))
		 (:enum (parse-enum (rest f)))
		 (:sequence (parse-seq (rest f)))
		 (:table (parse-table (rest f))))))
      (dolist (f forms)
	(parse-form f))
      `(progn
	 (defclass ,db-id (db)
	   ())

	 ,@def-forms
	 (defmethod initialize-instance :after ((*db* ,db-id) &key)
	   (with-slots (def-lookup defs) *db*
	     ,@(nreverse init-forms)
	     (setf defs (nreverse defs))))))))

(defmethod create ((db db))
  (dolist (d (defs db))
    (create d)))

(defmethod drop ((db db))
  (dolist (d (reverse (defs db)))
    (drop d)))

(define-db test-db
  (table users (alias)
	 (column alias text)
	 (column name1 text :null? t)
	 (column name2 text :null? t))
  (sequence event-id)
  (enum event-type
	user-created
	user-updated)
  (table events (id)
	 (column id bigint)
	 (column type event-type)
	 (column meta json :null? t)
	 (column body json :null? t)
	 (column at timestamp)
	 (foreign-key by users)))

(defun test-db ()
  (with-db (test-db)
    (assert (= (length (cols (db users))) 3))
    (assert (= (length (cols (primary-key (db users)))) 1))
    (assert (= (length (cols (db events by))) 1))
    
    (with-cx ("test" "test" "test")
      (drop *db*)
      (create *db*))))
