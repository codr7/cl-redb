(in-package redb)

(defclass enum (def)
  ((alts :initarg :alts :reader alts)))

(defmethod print-object ((enum enum) out)
  (format out "(enum ~a)" (str! (name enum))))

(defun new-enum (name &rest alts)
  (make-instance 'enum :name name
		       :alts (make-array (length alts) :element-type 'keyword
						       :initial-contents alts)))

(defmethod create ((self enum) &key (cx *cx*))
  (let ((sql (with-output-to-string (out)
		(format out "CREATE TYPE ~a AS ENUM (" (sql-name self))
		(with-slots (alts) self
		  (dotimes (i (length alts))
		    (unless (zerop i)
		      (format out ", "))
		    (format out "'~a'" (sql-name (aref alts i))))
		  (format out ")")))))
    (send sql nil :cx cx))
  (multiple-value-bind (result status) (recv :cx cx)
    (assert (eq status :PGRES_COMMAND_OK))
    (PQclear result)
    (assert (null (recv :cx cx))))
  nil)

(defmethod drop ((self enum) &key (cx *cx*))
  (let ((sql (format nil "DROP TYPE IF EXISTS ~a" (sql-name self))))
    (send sql nil :cx cx))
  (multiple-value-bind (result status) (recv :cx cx)
    (assert (eq status :PGRES_COMMAND_OK))    
    (PQclear result)
    (assert (null (recv :cx cx))))
  nil)

(defmethod exists? ((self enum) &key (cx *cx*))
  (send "SELECT EXISTS (
                 SELECT FROM pg_type
                 WHERE typname  = $1
               )"
	(list (sql-name (name self)))
	:cx cx)
  (multiple-value-bind (result status) (recv :cx cx)
    (assert (eq status :PGRES_TUPLES_OK))    
    (assert (= (PQntuples result) 1))
    (assert (= (PQnfields result) 1))
    (let ((exists? (boolean-from-sql (PQgetvalue result 0 0))))
      (PQclear result)
      (assert (null (recv :cx cx)))
      exists?)))
