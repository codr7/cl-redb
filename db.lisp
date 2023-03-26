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
			  (parse-index (f)
			    (let ((name (pop f))
				  (unique? (pop f)))
			      (push `(let ((idx (apply #'new-index (db ,table-name)
						       ',name
						       ,unique?
						       (mapcar (lambda (x)
								 (find-def (db ,table-name) x))
							       '(,@f)))))
				       (push idx defs)
				       (setf (gethash ',name def-lookup) idx))
				    init-forms)))
			  (parse-fkey (f)
			    (let* ((name (pop f))
				   (foreign-table (pop f)))
			      (push `(new-fkey (db ,table-name)
					       ',name
					       (db ,foreign-table)
					       ,@f)
				    init-forms)))
			  
			  (parse-table-form (f)
			    (ecase (kw (pop f))
			      (:col
			       (parse-col f))
			      (:index
			       (parse-index f))
			      (:fkey
			       (parse-fkey f)))))
		   
		   (push `(let ((tbl (apply #'new-table ',table-name '(,@keys))))
			    (push tbl defs)
			    (setf (gethash ',table-name def-lookup) tbl))
			 init-forms)
		   
		   (dolist (tf f)
		     (parse-table-form tf)))))
	     (parse-enum (f)
	       (let ((name (pop f)))
		 (push `(define-col-type (enum) ,name ,(sql-name name))
		       def-forms)
		 (push `(let ((enum (new-enum ',name ,@(mapcar #'kw f))))
			  (push enum defs)
			  (setf (gethash ',name def-lookup) enum))
		       init-forms)))
	     (parse-seq (f)
	       (let ((name (pop f)))
		 (push `(let ((seq (new-seq ',name ,@f)))
			  (push seq defs)
			  (setf (gethash ',name def-lookup) seq))
		       init-forms)))
	     (parse-form (f)
	       (ecase (kw (pop f))
		 (:enum (parse-enum f))
		 (:seq (parse-seq f))
		 (:table (parse-table f)))))
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
	 (col alias text)
	 (col name1 text :null? t)
	 (col name2 text :null? t))
  (seq event-id)
  (enum event-type
	user-created
	user-updated)
  (table events (id)
	 (col id bigint)
	 (col type event-type)
	 (col meta json :null? t)
	 (col body json :null? t)
	 (col at timestamp)
	 (fkey by users)
	 (index timestamp-idx nil at)))
