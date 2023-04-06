(in-package redb)

(defclass enum (def)
  ((alts :initarg :alts)))

(defmethod print-object ((enum enum) out)
  (format out "(enum ~a)" (str! (name enum))))

(defun new-enum (name &rest alts)
  (make-instance 'enum :name name
		       :alts alts))

(defmethod create ((enum enum))
  (with-slots (alts) enum
    (if (exists? enum)
	(let ((salts (existing-enum-alts enum)))
	  (dolist (a alts)
	    (unless (member a salts)
	      (let ((sql (format nil "ALTER TYPE ~a ADD VALUE '~a'" (sql-name enum) (enum-to-sql a))))
		(send-cmd sql nil))))
	    
	  (dolist (a salts)
	    (unless (member a alts)
	      (let ((sql (format nil "ALTER TYPE ~a DROP VALUE '~a'" (sql-name enum) (enum-to-sql a))))
		(send-cmd sql nil))))
	  
	  nil)
	(let ((sql (with-output-to-string (out)
		     (format out "CREATE TYPE ~a AS ENUM (" (sql-name enum))
		     (let ((i 0))
		       (dolist (a alts)
			 (unless (zerop i)
			   (format out ", "))
			 (format out "'~a'" (symbol-name a))
			 (incf i)))
		     (format out ")"))))
	  (send-cmd sql nil)
	  t))))

(defmethod drop ((enum enum))
  (when (exists? enum)
    (let ((sql (format nil "DROP TYPE ~a" (sql-name enum))))
      (send-cmd sql nil))
    t))

(defmethod exists? ((enum enum))
  (boolean-from-sql (send-val "SELECT EXISTS (
                                 SELECT FROM pg_type
                                 WHERE typname  = $1
                               )"
			      (list (sql-name (name enum))))))

(defun add-enum (enum alt)
  (with-slots (alts) enum
    (push alt alts)))

(defun del-enum (enum alt)
  (with-slots (alts) enum
    (remove alt alts)))

(defun existing-enum-alts (enum)
  (send "SELECT e.enumlabel
         FROM pg_type t 
         JOIN pg_enum e on e.enumtypid = t.oid  
         JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
         WHERE t.typname = $1"
	`(,(sql-name enum)))

  (multiple-value-bind (result status) (recv)
    (assert (eq status :PGRES_TUPLES_OK))    
    (assert (= (PQnfields result) 1))
    (assert (null (recv)))
    (let (as)
      (dotimes (row (PQntuples result))
	(push (enum-from-sql (PQgetvalue result row 0)) as))
      (PQclear result)
      as)))
