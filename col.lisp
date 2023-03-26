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
  (format out "(col ~a)" (str! (name col))))

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
	(send-dml sql nil)
	t))))

(defmethod drop ((col col))
  (when (exists? col)
    (with-slots (table) col
      (let ((sql (with-output-to-string (out)
		   (format out "ALTER TABLE ~a DROP COLUMN ~a"
			   (sql-name table) (sql-name col)))))
	(send-dml sql nil)
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

(defmethod to-sql ((col enum-col) val)
  (sql-name val))

(defmethod from-sql ((col enum-col) val)
  (kw (with-output-to-string (out)
	(dotimes (i (length val))
	  (let ((c (char val i)))
	    (write-char (if (char= c #\_) #\- c) out))))))

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

(define-col-type (text) json "JSON")

(define-col-type () timestamp "TIMESTAMP")

(defmethod to-sql ((col timestamp-col) val)
  (format-timestring nil val)) 

(defun timestamp-from-sql (val)
  (flet ((p (i) (parse-integer val :start i :junk-allowed t)))
    (let ((year (p 0))
	  (month (p 5))
	  (day (p 8))
	  (h (p 11))
	  (m (p 14))
	  (s (p 17)))
      (encode-timestamp 0 s m h day month year))))

(defmethod from-sql ((col timestamp-col) val)
  (timestamp-from-sql val))
