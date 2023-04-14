(in-package redb)

(defclass col (table-def)
  ((null? :initarg :null? :initform nil :reader null?)))

(defmethod sql ((col col))
  (with-output-to-string (out)
    (let ((tbl (table col)))
      (when tbl
	(format out "~a." (sql-name tbl)))
      (write-string (sql-name col) out))))

(defmethod print-object ((col col) out)
  (format out "(col ~a)" (sql col)))

(defmethod cols ((col col))
  `(,col))

(defmethod col-clone ((col col) tbl name)
  (make-instance (type-of col) :table tbl :name name))

(defmethod exists? ((col col))
  (boolean-from-sql (send-val "SELECT EXISTS (
                                 SELECT
                                 FROM pg_attribute 
                                 WHERE attrelid = $1::regclass
                                 AND attname = $2
                                 AND NOT attisdropped
                               )"
			      `(,(sql-name (table col)) ,(sql-name col)))))

(defmethod create ((col col))
  (unless (exists? col)
    (with-slots (table) col
      (let ((sql (with-output-to-string (out)
		   (format out "ALTER TABLE ~a ADD COLUMN ~a ~a"
			   (sql-name table)
			   (sql-name col)
			   (data-type col))

		   (unless (null? col)
		     (format out " NOT NULL")))))
	(send-cmd sql nil)
	t))))

(defmethod drop ((col col))
  (when (exists? col)
    (with-slots (table) col
      (let ((sql (with-output-to-string (out)
		   (format out "ALTER TABLE ~a DROP COLUMN ~a"
			   (sql-name table) (sql-name col)))))
	(send-cmd sql nil)
	t))))

(defmacro define-col-type ((&optional parent) name data-type)
  (let ((cname (sym name '-col)))
    `(progn
       (defclass ,cname (,(if parent (sym parent '-col) 'col))
	 ())
       
       (defun ,(sym 'new- cname) (tbl name &key null?)
	 (make-instance ',cname :table tbl :name name :null? null?))
       
       (defmethod data-type ((col ,cname))
	 ,data-type))))

(defmethod col= ((col col) x y)
  (eq x y))

(define-col-type () boolean "BOOLEAN")

(defmethod boolean-to-sql (val)
  (if val "t" "f"))

(defmethod to-sql ((col boolean-col) val)
  (boolean-to-sql val))

(defun boolean-from-sql (val)
  (string= val "t"))

(defmethod from-sql ((col boolean-col) val)
  (boolean-from-sql val))

(defclass enum-col (col)
  ())

(defun enum-to-sql (val)
  (symbol-name val))

(defmethod to-sql ((col enum-col) val)
  (enum-to-sql val))

(defun enum-from-sql (val)
  (kw val))

(defmethod from-sql ((col enum-col) val)
  (enum-from-sql val))

(define-col-type () integer "INTEGER")

(defun integer-to-sql (val)
  (format nil "~a" val))

(defmethod to-sql ((col integer-col) val)
  (integer-to-sql val))

(defun integer-from-sql (val)
  (parse-integer val))

(defmethod from-sql ((col integer-col) val)
  (integer-from-sql val))

(define-col-type (integer) bigint "BIGINT")

(define-col-type () text "TEXT")

(defmethod to-sql ((col text-col) val)
  val)

(defmethod from-sql ((col text-col) val)
  val)

(defmethod col= ((col text-col) x y)
  (string= x y))

(define-col-type (text) json "JSON")

(define-col-type () timestamp "TIMESTAMP")

(defun timestamp-to-sql (val)
  (with-output-to-string (out)
    (print-object val out)))

(defun timestamp-from-sql (val)
  (flet ((p (i)
	   (parse-integer val :start i :junk-allowed t)))
    (let ((year (p 0))
	  (month (p 5))
	  (day (p 8))
	  (hours (p 11))
	  (minutes (p 14))
	  (seconds (p 17))
	  (microseconds (p 20)))
      (new-timestamp year (get-month month) day hours minutes seconds microseconds))))

(defmethod to-sql ((col timestamp-col) val)
  (timestamp-to-sql val))

(defmethod from-sql ((col timestamp-col) val)
  (timestamp-from-sql val))

(defmethod col= ((col timestamp-col) x y)
  (timestamp= x y))

(defun now ()
  (timestamp-from-sql (send-val "SELECT LOCALTIMESTAMP" nil)))
