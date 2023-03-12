(in-package redb)

(defclass key (def rel)
  ())

(defun new-key (name cols)
  (let* ((key (make-instance 'key :name name)))
    (dolist (c cols)
      (add-col key c))
    key))

(defmethod key-create ((self key) table)
  (let* ((sql (with-output-to-string (out)
		(format out "ALTER TABLE ~a ADD CONSTRAINT ~a ~a ("
			(to-sql table)
			(to-sql self)
			(if (eq self (primary-key table)) "PRIMARY KEY" "UNIQUE"))
		
		(let* ((i 0))
		  (do-cols (c self)
		    (unless (zerop i)
		      (format out ", "))
		    (format out "~a" (to-sql c))
		    (incf i)))
		
		(format out ")"))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defmethod key-drop ((self key) table)
  (let* ((sql (format nil "ALTER TABLE ~a DROP CONSTRAINT IF EXISTS ~a"
		      (to-sql table) (to-sql self))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  (assert (null (get-result)))
  nil)
