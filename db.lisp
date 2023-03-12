(in-package redb)

(defvar *db*)

(defclass db ()
  ((defs :initform nil :reader defs)
   (def-lookup :initform (make-hash-table) :reader def-lookup)))

(defun find-def (name &key (db *db*))
  (gethash name (def-lookup db)))

(defmacro define-db (name &body forms)
  (let* (init-forms)
    (labels ((parse-table (f)
	       (let* ((name (pop f))
		      (key-cols (pop f))
		      defs)
		 (labels ((parse-col (f)
			    (let* ((name (pop f))
				   (type (pop f)))
			      (push `(,(syms! 'new- type '-col) ',name ,@f) defs)))
			  (parse-foreign-key (f)
			    (let* ((name (pop f))
				   (table (pop f)))
			      (push `(new-foreign-key ',name (find-def ',table :db self) ,@f) defs)))
			  (parse-table-form (f)
			    (ecase (kw! (first f))
			      (:column
			       (parse-col (rest f)))
			      (:foreign-key
			       (parse-foreign-key (rest f))))))
		   (dolist (tf f)
		     (parse-table-form tf)))
		 
		 (push `(let* ((table (new-table ',name '(,@key-cols) (list ,@(nreverse defs)))))
			  (push table defs)
			  (setf (gethash ',name def-lookup) table))
		       init-forms)))
	     (parse-enum (f)
	       (let* ((name (first f))
		      (ct (syms! name '-col)))
		 (push `(let* ((enum (new-enum ',name ,@(mapcar #'kw! (rest f)))))
			  (push enum defs)
			  (setf (gethash ',name def-lookup) enum)
			  (define-col-type ,ct ,(to-sql name))
			  
			  (defmethod col-to-sql ((self ,ct) val)
			    (str! val))
			  
			  (defmethod col-from-sql ((self ,ct) val)
			    (kw! val)))
		       init-forms)))
	     (parse-form (f)
	       (ecase (kw! (first f))
		 (:enum (parse-enum (rest f)))
		 (:table (parse-table (rest f))))))
      (dolist (f forms)
	(parse-form f))
      `(progn
	 (defclass ,name (db)
	   ())
	 
	 (defmethod initialize-instance :after ((self ,name) &key)
	   (with-slots (def-lookup defs) self
	     ,@(nreverse init-forms)))))))

(defmethod create ((self db))
  (dolist (d (reverse (defs self)))
    (create d)))

(defmethod drop ((self db))
  (dolist (d (defs self))
    (if (exists? d)
	(drop d))))
